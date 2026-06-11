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
	return &MySQLConnector{db: db, database: source.Database}, nil
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
