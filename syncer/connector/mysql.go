package connector

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"database-sync-go/domains"
	"database-sync-go/syncer/mapper"

	_ "github.com/go-sql-driver/mysql"
)

type MySQLConnector struct {
	db       *sql.DB
	database string
	source   domains.DataSource
}

func NewMySQL(source domains.DataSource) (*MySQLConnector, error) {
	params := url.Values{}
	params.Set("charset", "utf8mb4")
	params.Set("parseTime", "True")
	params.Set("loc", "Local")
	for key, value := range paramsMap(source.Params) {
		params.Set(key, value)
	}
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s", source.Username, source.Password, source.Host, source.Port, source.Database, params.Encode())
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(30 * time.Minute)
	return &MySQLConnector{db: db, database: source.Database, source: source}, nil
}

func (c *MySQLConnector) Test() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return c.db.PingContext(ctx)
}

func (c *MySQLConnector) ListTables(ctx context.Context) ([]TableInfo, error) {
	database := strings.TrimSpace(c.database)
	if database == "" {
		return nil, fmt.Errorf("database required")
	}
	rows, err := c.db.QueryContext(ctx, `
SELECT TABLE_NAME, TABLE_TYPE, COALESCE(TABLE_COMMENT, '')
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = ?
ORDER BY TABLE_NAME ASC`, database)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]TableInfo, 0)
	for rows.Next() {
		var item TableInfo
		if err := rows.Scan(&item.Name, &item.Type, &item.Comment); err != nil {
			return nil, err
		}
		item.Type = strings.ToLower(strings.TrimPrefix(item.Type, "BASE "))
		items = append(items, item)
	}
	return items, rows.Err()
}

func (c *MySQLConnector) DatabaseDetail(ctx context.Context) (*DatabaseDetail, error) {
	detail := &DatabaseDetail{
		Basic: DatabaseBasicInfo{
			Name: c.database,
			Type: domains.DataSourceTypeMySQL,
		},
		Connection: DatabaseConnectionInfo{
			Host:     c.source.Host,
			Port:     c.source.Port,
			Username: c.source.Username,
			Database: c.database,
			Endpoint: fmt.Sprintf("%s:%d", c.source.Host, c.source.Port),
		},
		CheckedAt: time.Now().UnixMilli(),
		Warnings:  []string{},
	}

	if err := c.db.PingContext(ctx); err != nil {
		return nil, err
	}
	appendWarning := func(section string, err error) {
		if err != nil {
			detail.Warnings = append(detail.Warnings, section+": "+err.Error())
		}
	}

	var version string
	if err := c.db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version); err == nil {
		detail.Basic.Version = version
	} else {
		appendWarning("读取数据库版本失败", err)
	}
	var serverTime string
	if err := c.db.QueryRowContext(ctx, "SELECT DATE_FORMAT(NOW(), '%Y-%m-%d %H:%i:%s')").Scan(&serverTime); err == nil {
		detail.Basic.ServerTime = serverTime
	} else {
		appendWarning("读取服务器时间失败", err)
	}
	if c.database != "" {
		err := c.db.QueryRowContext(ctx, `
SELECT DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME
FROM information_schema.SCHEMATA
WHERE SCHEMA_NAME = ?`, c.database).Scan(&detail.Basic.Charset, &detail.Basic.Collation)
		appendWarning("读取字符集信息失败", err)
	}
	var currentUser string
	var connectionID string
	if err := c.db.QueryRowContext(ctx, "SELECT CURRENT_USER(), CONNECTION_ID()").Scan(&currentUser, &connectionID); err == nil {
		detail.Connection.CurrentUser = currentUser
		detail.Connection.ConnectionID = connectionID
	} else {
		appendWarning("读取当前连接信息失败", err)
	}

	status, err := c.mysqlStatus(ctx)
	if err != nil {
		appendWarning("读取运行状态失败", err)
	}
	variables, err := c.mysqlVariables(ctx)
	if err != nil {
		appendWarning("读取连接变量失败", err)
	}
	detail.Basic.UptimeSeconds = statusInt64(status, "Uptime")
	detail.Connection.MaxConnections = statusInt64(variables, "max_connections")
	detail.Connection.ThreadsRunning = statusInt64(status, "Threads_running")
	detail.Connection.ThreadsConnected = statusInt64(status, "Threads_connected")
	detail.Performance.Queries = statusInt64(status, "Questions")
	detail.Performance.SlowQueries = statusInt64(status, "Slow_queries")
	detail.Performance.Connections = statusInt64(status, "Connections")
	detail.Performance.OpenTables = statusInt64(status, "Open_tables")
	if detail.Basic.UptimeSeconds > 0 {
		detail.Performance.QPS = float64(detail.Performance.Queries) / float64(detail.Basic.UptimeSeconds)
	}
	readRequests := statusInt64(status, "Innodb_buffer_pool_read_requests")
	reads := statusInt64(status, "Innodb_buffer_pool_reads")
	if readRequests > 0 {
		detail.Performance.CacheHitPercent = (1 - float64(reads)/float64(readRequests)) * 100
	}

	storage, tableStats, err := c.mysqlTableStats(ctx)
	if err != nil {
		appendWarning("读取表和存储统计失败", err)
	} else {
		detail.Storage = storage
		detail.TableStats = tableStats
	}
	detail.Basic.Metrics = []MetricItem{
		{Label: "运行时长", Value: fmt.Sprint(detail.Basic.UptimeSeconds), Unit: "秒"},
		{Label: "服务器时间", Value: detail.Basic.ServerTime},
	}
	detail.Connection.Metrics = []MetricItem{
		{Label: "当前连接", Value: fmt.Sprint(detail.Connection.ThreadsConnected), Unit: "个"},
		{Label: "运行线程", Value: fmt.Sprint(detail.Connection.ThreadsRunning), Unit: "个"},
		{Label: "最大连接", Value: fmt.Sprint(detail.Connection.MaxConnections), Unit: "个"},
	}
	detail.Storage.Metrics = []MetricItem{
		{Label: "数据大小", Value: fmt.Sprint(detail.Storage.DataBytes), Unit: "B"},
		{Label: "索引大小", Value: fmt.Sprint(detail.Storage.IndexBytes), Unit: "B"},
		{Label: "空闲空间", Value: fmt.Sprint(detail.Storage.FreeBytes), Unit: "B"},
	}
	detail.Performance.Metrics = []MetricItem{
		{Label: "QPS", Value: fmt.Sprintf("%.2f", detail.Performance.QPS)},
		{Label: "慢查询", Value: fmt.Sprint(detail.Performance.SlowQueries), Unit: "次"},
		{Label: "缓存命中率", Value: fmt.Sprintf("%.2f", detail.Performance.CacheHitPercent), Unit: "%"},
	}
	return detail, nil
}

func (c *MySQLConnector) DescribeTable(ctx context.Context, table string) ([]ColumnInfo, error) {
	database, tableName, err := SplitTableName(c.database, table)
	if err != nil {
		return nil, err
	}
	if database == "" {
		return nil, fmt.Errorf("database required")
	}
	rows, err := c.db.QueryContext(ctx, `
SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY, COALESCE(COLUMN_COMMENT, '')
FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
ORDER BY ORDINAL_POSITION ASC`, database, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := make([]ColumnInfo, 0)
	for rows.Next() {
		var nullable string
		var key string
		var item ColumnInfo
		if err := rows.Scan(&item.Name, &item.DatabaseType, &nullable, &key, &item.Comment); err != nil {
			return nil, err
		}
		item.Nullable = strings.EqualFold(nullable, "YES")
		item.PrimaryKey = strings.EqualFold(key, "PRI")
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(items) == 0 {
		return nil, fmt.Errorf("table %q not found", table)
	}
	return items, nil
}

func (c *MySQLConnector) Count(ctx context.Context, opts QueryOptions) (int64, error) {
	table, err := QuoteIdentifier(opts.Table)
	if err != nil {
		return 0, err
	}
	where, args, err := BuildWhere(opts, "?")
	if err != nil {
		return 0, err
	}
	query := "SELECT COUNT(*) FROM " + table + where
	var total int64
	if err := c.db.QueryRowContext(ctx, query, args...).Scan(&total); err != nil {
		return 0, err
	}
	return total, nil
}

func (c *MySQLConnector) QueryBatch(ctx context.Context, opts QueryOptions) ([]mapper.Row, error) {
	table, err := QuoteIdentifier(opts.Table)
	if err != nil {
		return nil, err
	}
	where, args, err := BuildWhere(opts, "?")
	if err != nil {
		return nil, err
	}
	limit := opts.Limit
	if limit <= 0 {
		limit = 1000
	}
	query := "SELECT * FROM " + table + where
	if strings.TrimSpace(opts.CursorField) != "" {
		cursor, err := QuoteIdentifier(opts.CursorField)
		if err != nil {
			return nil, err
		}
		query += " ORDER BY " + cursor + " ASC"
	}
	query += " LIMIT ? OFFSET ?"
	args = append(args, limit, opts.Offset)
	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRows(rows)
}

func (c *MySQLConnector) EnsureTable(ctx context.Context, table string, columns []ColumnInfo) error {
	tableName, err := QuoteIdentifier(table)
	if err != nil {
		return err
	}
	if len(columns) == 0 {
		return fmt.Errorf("columns required")
	}
	definitions := make([]string, 0, len(columns)+1)
	primaryKeys := make([]string, 0)
	for _, col := range columns {
		name, err := QuoteIdentifier(col.Name)
		if err != nil {
			return err
		}
		columnType := strings.TrimSpace(col.DatabaseType)
		if columnType == "" {
			columnType = "text"
		}
		definition := name + " " + columnType
		if !col.Nullable || col.PrimaryKey {
			definition += " NOT NULL"
		}
		definitions = append(definitions, definition)
		if col.PrimaryKey {
			primaryKeys = append(primaryKeys, name)
		}
	}
	if len(primaryKeys) > 0 {
		definitions = append(definitions, "PRIMARY KEY ("+strings.Join(primaryKeys, ",")+")")
	}
	_, err = c.db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS "+tableName+" ("+strings.Join(definitions, ",")+")")
	return err
}

func (c *MySQLConnector) TruncateTable(ctx context.Context, table string) error {
	tableName, err := QuoteIdentifier(table)
	if err != nil {
		return err
	}
	_, err = c.db.ExecContext(ctx, "TRUNCATE TABLE "+tableName)
	return err
}

func (c *MySQLConnector) WriteBatch(ctx context.Context, rows []mapper.Row, opts WriteOptions) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	table, err := QuoteIdentifier(opts.Table)
	if err != nil {
		return 0, err
	}
	columns := Columns(rows)
	sort.Strings(columns)
	quotedColumns, err := QuoteIdentifiers(columns)
	if err != nil {
		return 0, err
	}

	rowPlaceholders := "(" + strings.TrimRight(strings.Repeat("?,", len(columns)), ",") + ")"
	valuePlaceholders := make([]string, 0, len(rows))
	args := make([]any, 0, len(rows)*len(columns))
	for _, row := range rows {
		valuePlaceholders = append(valuePlaceholders, rowPlaceholders)
		for _, col := range columns {
			args = append(args, row[col])
		}
	}

	writeMode := strings.ToLower(strings.TrimSpace(opts.WriteMode))
	var query string
	switch writeMode {
	case domains.WriteModeReplace:
		query = "REPLACE INTO " + table + " (" + strings.Join(quotedColumns, ",") + ") VALUES " + strings.Join(valuePlaceholders, ",")
	case domains.WriteModeUpsert:
		updates := buildMySQLUpsertUpdates(columns, opts.ConflictKeys)
		if len(updates) == 0 {
			return 0, fmt.Errorf("upsert requires at least one non-conflict column")
		}
		query = "INSERT INTO " + table + " (" + strings.Join(quotedColumns, ",") + ") VALUES " + strings.Join(valuePlaceholders, ",") + " ON DUPLICATE KEY UPDATE " + strings.Join(updates, ",")
	default:
		query = "INSERT INTO " + table + " (" + strings.Join(quotedColumns, ",") + ") VALUES " + strings.Join(valuePlaceholders, ",")
	}

	result, err := c.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	affected, _ := result.RowsAffected()
	return affected, nil
}

func (c *MySQLConnector) Close() error {
	if c.db == nil {
		return nil
	}
	return c.db.Close()
}

func buildMySQLUpsertUpdates(columns []string, conflictKeys []string) []string {
	conflicts := map[string]bool{}
	for _, key := range conflictKeys {
		conflicts[strings.ToLower(strings.TrimSpace(key))] = true
	}
	updates := make([]string, 0, len(columns))
	for _, col := range columns {
		if conflicts[strings.ToLower(col)] {
			continue
		}
		quoted, err := QuoteIdentifier(col)
		if err != nil {
			continue
		}
		updates = append(updates, quoted+"=VALUES("+quoted+")")
	}
	return updates
}

func (c *MySQLConnector) mysqlStatus(ctx context.Context) (map[string]string, error) {
	rows, err := c.db.QueryContext(ctx, "SHOW GLOBAL STATUS")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanNameValueRows(rows)
}

func (c *MySQLConnector) mysqlVariables(ctx context.Context) (map[string]string, error) {
	rows, err := c.db.QueryContext(ctx, "SHOW VARIABLES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanNameValueRows(rows)
}

func (c *MySQLConnector) mysqlTableStats(ctx context.Context) (DatabaseStorageInfo, DatabaseTableStats, error) {
	if strings.TrimSpace(c.database) == "" {
		return DatabaseStorageInfo{}, DatabaseTableStats{}, fmt.Errorf("database required")
	}
	rows, err := c.db.QueryContext(ctx, `
SELECT TABLE_NAME, TABLE_TYPE, COALESCE(TABLE_ROWS, 0), COALESCE(DATA_LENGTH, 0),
       COALESCE(INDEX_LENGTH, 0), COALESCE(DATA_FREE, 0),
       COALESCE(DATE_FORMAT(CREATE_TIME, '%Y-%m-%d %H:%i:%s'), ''),
       COALESCE(DATE_FORMAT(UPDATE_TIME, '%Y-%m-%d %H:%i:%s'), ''),
       COALESCE(TABLE_COMMENT, '')
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = ?
ORDER BY (COALESCE(DATA_LENGTH, 0) + COALESCE(INDEX_LENGTH, 0)) DESC, TABLE_NAME ASC
LIMIT 50`, c.database)
	if err != nil {
		return DatabaseStorageInfo{}, DatabaseTableStats{}, err
	}
	defer rows.Close()

	var storage DatabaseStorageInfo
	stats := DatabaseTableStats{Tables: make([]TableStat, 0)}
	for rows.Next() {
		var table TableStat
		var dataFree int64
		if err := rows.Scan(&table.Name, &table.Type, &table.Rows, &table.DataBytes, &table.IndexBytes, &dataFree, &table.CreatedAt, &table.UpdatedAt, &table.Comment); err != nil {
			return DatabaseStorageInfo{}, DatabaseTableStats{}, err
		}
		table.Type = strings.ToLower(strings.TrimPrefix(table.Type, "BASE "))
		if table.Type == "view" {
			stats.TotalViews += 1
		} else {
			stats.TotalTables += 1
		}
		stats.TotalRows += table.Rows
		storage.DataBytes += table.DataBytes
		storage.IndexBytes += table.IndexBytes
		storage.FreeBytes += dataFree
		stats.Tables = append(stats.Tables, table)
	}
	if err := rows.Err(); err != nil {
		return DatabaseStorageInfo{}, DatabaseTableStats{}, err
	}
	storage.TotalBytes = storage.DataBytes + storage.IndexBytes
	return storage, stats, nil
}

func scanNameValueRows(rows *sql.Rows) (map[string]string, error) {
	out := map[string]string{}
	for rows.Next() {
		var name string
		var value string
		if err := rows.Scan(&name, &value); err != nil {
			return nil, err
		}
		out[name] = value
	}
	return out, rows.Err()
}

func statusInt64(values map[string]string, key string) int64 {
	raw := strings.TrimSpace(values[key])
	if raw == "" {
		return 0
	}
	var out int64
	_, _ = fmt.Sscan(raw, &out)
	return out
}

func scanRows(rows *sql.Rows) ([]mapper.Row, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	out := make([]mapper.Row, 0)
	for rows.Next() {
		values := make([]any, len(columns))
		scanTargets := make([]any, len(columns))
		for i := range values {
			scanTargets[i] = &values[i]
		}
		if err := rows.Scan(scanTargets...); err != nil {
			return nil, err
		}
		row := mapper.Row{}
		for i, col := range columns {
			row[col] = normalizeSQLValue(values[i])
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func normalizeSQLValue(value any) any {
	switch v := value.(type) {
	case []byte:
		return string(v)
	default:
		return v
	}
}
