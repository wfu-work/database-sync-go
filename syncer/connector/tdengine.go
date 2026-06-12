package connector

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"database-sync-go/domains"
	"database-sync-go/syncer/mapper"
)

type TDengineConnector struct {
	endpoint string
	database string
	client   *http.Client
	auth     string
	source   domains.DataSource
}

type tdengineResponse struct {
	Status     string  `json:"status"`
	Code       int     `json:"code"`
	Desc       string  `json:"desc"`
	ColumnMeta [][]any `json:"column_meta"`
	Data       [][]any `json:"data"`
	Rows       int     `json:"rows"`
}

type tdengineInsertGroup struct {
	quotedChildTable string
	tagLiterals      []string
	rows             []mapper.Row
}

var tdengineTemplateVarPattern = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

func NewTDengine(source domains.DataSource) (*TDengineConnector, error) {
	params := paramsMap(source.Params)
	scheme := params["scheme"]
	if scheme == "" {
		scheme = "http"
	}
	endpoint := strings.TrimRight(fmt.Sprintf("%s://%s:%d", scheme, source.Host, source.Port), "/")
	if custom := strings.TrimSpace(params["endpoint"]); custom != "" {
		endpoint = strings.TrimRight(custom, "/")
	}
	timeout := 30 * time.Second
	if raw := strings.TrimSpace(params["timeout"]); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil {
			timeout = parsed
		}
	}
	auth := base64.StdEncoding.EncodeToString([]byte(source.Username + ":" + source.Password))
	return &TDengineConnector{
		endpoint: endpoint,
		database: source.Database,
		client:   &http.Client{Timeout: timeout},
		auth:     "Basic " + auth,
		source:   source,
	}, nil
}

func (c *TDengineConnector) Test() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := c.exec(ctx, "SELECT SERVER_VERSION()")
	return err
}

func (c *TDengineConnector) ListTables(ctx context.Context) ([]TableInfo, error) {
	stableResp, err := c.exec(ctx, "SHOW STABLES")
	if err != nil {
		return nil, err
	}
	items := tdengineTableInfos(stableResp, "stable")
	sort.Slice(items, func(i, j int) bool {
		if items[i].Type == items[j].Type {
			return items[i].Name < items[j].Name
		}
		return items[i].Type < items[j].Type
	})
	return items, nil
}

func (c *TDengineConnector) DatabaseDetail(ctx context.Context) (*DatabaseDetail, error) {
	detail := &DatabaseDetail{
		Basic: DatabaseBasicInfo{
			Name: c.database,
			Type: domains.DataSourceTypeTDengine,
		},
		Connection: DatabaseConnectionInfo{
			Host:     c.source.Host,
			Port:     c.source.Port,
			Username: c.source.Username,
			Database: c.database,
			Endpoint: c.endpoint,
		},
		CheckedAt: time.Now().UnixMilli(),
		Warnings:  []string{},
	}
	appendWarning := func(section string, err error) {
		if err != nil {
			detail.Warnings = append(detail.Warnings, section+": "+err.Error())
		}
	}

	versionResp, err := c.exec(ctx, "SELECT SERVER_VERSION()")
	if err != nil {
		return nil, err
	}
	if len(versionResp.Data) > 0 && len(versionResp.Data[0]) > 0 {
		detail.Basic.Version = fmt.Sprint(versionResp.Data[0][0])
	}
	timeResp, err := c.exec(ctx, "SELECT NOW()")
	if err == nil && len(timeResp.Data) > 0 && len(timeResp.Data[0]) > 0 {
		detail.Basic.ServerTime = fmt.Sprint(timeResp.Data[0][0])
	} else {
		appendWarning("读取服务器时间失败", err)
	}

	dbInfo, err := c.tdengineDatabaseInfo(ctx)
	if err != nil {
		appendWarning("读取数据库参数失败", err)
	} else {
		applyTDengineDatabaseInfo(detail, dbInfo)
	}

	tableStats, err := c.tdengineTableStats(ctx)
	if err != nil {
		appendWarning("读取表统计失败", err)
	} else {
		detail.TableStats = mergeTDengineTableStats(detail.TableStats, tableStats)
	}

	storage, storageMetrics, err := c.tdengineDiskUsage(ctx)
	if err == nil {
		detail.Storage = mergeTDengineStorage(detail.Storage, storage)
		detail.Storage.Metrics = append(detail.Storage.Metrics, storageMetrics...)
	} else {
		appendWarning("读取磁盘用量失败", err)
	}

	performanceMetrics, err := c.applyTDenginePerformance(ctx, detail)
	if err == nil {
		detail.Performance.Metrics = append(detail.Performance.Metrics, performanceMetrics...)
	} else {
		appendWarning("读取性能信息失败", err)
	}
	clusterNodes, err := c.tdengineClusterNodes(ctx)
	if err == nil && clusterNodes > 0 {
		detail.Connection.MaxConnections = int64(clusterNodes)
		detail.Performance.Metrics = append(detail.Performance.Metrics, MetricItem{
			Label: "集群节点",
			Value: fmt.Sprint(clusterNodes),
			Unit:  "个",
		})
	} else {
		appendWarning("读取集群信息失败", err)
	}

	detail.Performance.OpenTables = int64((detail.TableStats.TotalTables + detail.TableStats.TotalViews))
	detail.Basic.Metrics = append(detail.Basic.Metrics, MetricItem{Label: "服务器时间", Value: detail.Basic.ServerTime})
	detail.Connection.Metrics = []MetricItem{
		{Label: "REST Endpoint", Value: c.endpoint},
		{Label: "数据库", Value: c.database},
	}
	if len(detail.Storage.Metrics) == 0 {
		detail.Storage.Metrics = append(detail.Storage.Metrics, MetricItem{
			Label: "存储统计",
			Value: "当前 TDengine 未返回可聚合的磁盘用量",
		})
	}
	detail.Performance.Metrics = append(detail.Performance.Metrics, MetricItem{
		Label: "表对象",
		Value: fmt.Sprint(detail.TableStats.TotalTables + detail.TableStats.TotalViews),
		Unit:  "个",
	})
	return detail, nil
}

func (c *TDengineConnector) DescribeTable(ctx context.Context, table string) ([]ColumnInfo, error) {
	quoted, err := QuoteIdentifier(table)
	if err != nil {
		return nil, err
	}
	resp, err := c.exec(ctx, "DESCRIBE "+quoted)
	if err != nil {
		return nil, err
	}
	items := make([]ColumnInfo, 0, len(resp.Data))
	for i, row := range resp.Data {
		if len(row) == 0 {
			continue
		}
		item := ColumnInfo{
			Name:       fmt.Sprint(row[0]),
			Nullable:   true,
			PrimaryKey: i == 0,
		}
		if len(row) > 1 {
			item.DatabaseType = fmt.Sprint(row[1])
		}
		if len(row) > 3 {
			item.Comment = fmt.Sprint(row[3])
		}
		item.IsTag = tdengineDescribeRowIsTag(row)
		items = append(items, item)
	}
	if len(items) == 0 {
		return nil, fmt.Errorf("table %q not found", table)
	}
	return items, nil
}

func (c *TDengineConnector) Count(ctx context.Context, opts QueryOptions) (int64, error) {
	table, err := QuoteIdentifier(opts.Table)
	if err != nil {
		return 0, err
	}
	where, err := buildTDengineWhere(opts)
	if err != nil {
		return 0, err
	}
	resp, err := c.exec(ctx, "SELECT COUNT(*) FROM "+table+where)
	if err != nil {
		return 0, err
	}
	if len(resp.Data) == 0 || len(resp.Data[0]) == 0 {
		return 0, nil
	}
	return anyToInt64(resp.Data[0][0])
}

func (c *TDengineConnector) QueryBatch(ctx context.Context, opts QueryOptions) ([]mapper.Row, error) {
	table, err := QuoteIdentifier(opts.Table)
	if err != nil {
		return nil, err
	}
	where, err := buildTDengineWhere(opts)
	if err != nil {
		return nil, err
	}
	limit := opts.Limit
	if limit <= 0 {
		limit = 1000
	}
	selectFields := []string{"*"}
	extraSelectFields := make([]string, 0, len(opts.ExtraSelectFields))
	for _, field := range opts.ExtraSelectFields {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		quotedField, err := QuoteIdentifier(field)
		if err != nil {
			return nil, err
		}
		extraSelectFields = append(extraSelectFields, quotedField)
	}
	selectFields = append(selectFields, extraSelectFields...)
	query := "SELECT " + strings.Join(selectFields, ",") + " FROM " + table + where
	if strings.TrimSpace(opts.CursorField) != "" {
		cursor, err := QuoteIdentifier(opts.CursorField)
		if err != nil {
			return nil, err
		}
		query += " ORDER BY " + cursor + " ASC"
	}
	query += fmt.Sprintf(" LIMIT %d OFFSET %d", limit, opts.Offset)
	resp, err := c.exec(ctx, query)
	if err != nil {
		return nil, err
	}
	columns := tdengineColumns(resp.ColumnMeta)
	out := make([]mapper.Row, 0, len(resp.Data))
	for _, values := range resp.Data {
		row := mapper.Row{}
		for i, col := range columns {
			if i < len(values) {
				row[col] = values[i]
			}
		}
		out = append(out, row)
	}
	return out, nil
}

func (c *TDengineConnector) EnsureTable(ctx context.Context, table string, columns []ColumnInfo) error {
	tableName, err := QuoteIdentifier(table)
	if err != nil {
		return err
	}
	if len(columns) == 0 {
		return fmt.Errorf("columns required")
	}
	definitions := make([]string, 0, len(columns))
	for i, col := range columns {
		name, err := QuoteIdentifier(col.Name)
		if err != nil {
			return err
		}
		columnType := strings.TrimSpace(col.DatabaseType)
		if columnType == "" {
			if i == 0 {
				columnType = "timestamp"
			} else {
				columnType = "nchar(255)"
			}
		}
		definitions = append(definitions, name+" "+columnType)
	}
	_, err = c.exec(ctx, "CREATE TABLE IF NOT EXISTS "+tableName+" ("+strings.Join(definitions, ",")+")")
	return err
}

func (c *TDengineConnector) TruncateTable(ctx context.Context, table string) error {
	tableName, err := QuoteIdentifier(table)
	if err != nil {
		return err
	}
	if _, err := c.exec(ctx, "DELETE FROM "+tableName); err != nil {
		return err
	}
	return nil
}

func (c *TDengineConnector) WriteBatch(ctx context.Context, rows []mapper.Row, opts WriteOptions) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	if tdengineChildTableTemplate(opts) != "" {
		return c.writeBatchUsingStable(ctx, rows, opts)
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
	valueRows := make([]string, 0, len(rows))
	for _, row := range rows {
		values := make([]string, 0, len(columns))
		for _, col := range columns {
			values = append(values, sqlLiteral(row[col]))
		}
		valueRows = append(valueRows, "("+strings.Join(values, ",")+")")
	}
	prefix := "INSERT INTO "
	if strings.EqualFold(opts.WriteMode, domains.WriteModeReplace) {
		prefix = "INSERT INTO "
	}
	query := prefix + table + " (" + strings.Join(quotedColumns, ",") + ") VALUES " + strings.Join(valueRows, ",")
	resp, err := c.exec(ctx, query)
	if err != nil {
		return 0, err
	}
	if resp.Rows > 0 {
		return int64(resp.Rows), nil
	}
	return int64(len(rows)), nil
}

func (c *TDengineConnector) writeBatchUsingStable(ctx context.Context, rows []mapper.Row, opts WriteOptions) (int64, error) {
	stable, err := QuoteIdentifier(opts.Table)
	if err != nil {
		return 0, err
	}
	childTableTemplate := tdengineChildTableTemplate(opts)
	if childTableTemplate == "" {
		return 0, errors.New("tdengine child table template required")
	}
	if len(opts.TDengineTagMappings) == 0 {
		return 0, errors.New("tdengine tag mappings required")
	}

	tagNames := make([]string, 0, len(opts.TDengineTagMappings))
	for _, tag := range opts.TDengineTagMappings {
		name := strings.TrimSpace(tag.Name)
		if name == "" {
			return 0, errors.New("tdengine tag name required")
		}
		tagNames = append(tagNames, name)
	}
	quotedTagNames, err := QuoteIdentifiers(tagNames)
	if err != nil {
		return 0, err
	}

	groups := map[string]*tdengineInsertGroup{}
	groupOrder := make([]string, 0)
	childTagKeys := map[string]string{}
	for _, row := range rows {
		childTable, err := renderTDengineChildTableName(row, childTableTemplate, opts.TDengineChildTableField)
		if err != nil {
			return 0, err
		}
		quotedChildTable, err := QuoteIdentifier(childTable)
		if err != nil {
			return 0, err
		}
		tagValues, err := mapper.MapTagValues(row, opts.TDengineTagMappings)
		if err != nil {
			return 0, err
		}
		tagLiterals := make([]string, 0, len(tagNames))
		for _, tagName := range tagNames {
			value, ok := mapper.Lookup(tagValues, tagName)
			if !ok || value == nil {
				return 0, fmt.Errorf("tdengine tag %q value missing", tagName)
			}
			tagLiterals = append(tagLiterals, sqlLiteral(value))
		}
		tagKey := strings.Join(tagLiterals, "\x00")
		if existingTagKey, ok := childTagKeys[quotedChildTable]; ok && existingTagKey != tagKey {
			return 0, fmt.Errorf("tdengine child table %s has inconsistent tag values", childTable)
		}
		childTagKeys[quotedChildTable] = tagKey
		key := quotedChildTable + "\x00" + tagKey
		group, ok := groups[key]
		if !ok {
			group = &tdengineInsertGroup{
				quotedChildTable: quotedChildTable,
				tagLiterals:      tagLiterals,
				rows:             make([]mapper.Row, 0),
			}
			groups[key] = group
			groupOrder = append(groupOrder, key)
		}
		group.rows = append(group.rows, row)
	}

	total := int64(0)
	for _, key := range groupOrder {
		group := groups[key]
		columns := opts.InsertColumns
		if len(columns) == 0 {
			columns = Columns(group.rows)
		}
		columns = filterColumns(columns, append(tagNames, tdengineTemplateSourceFields(childTableTemplate, opts.TDengineChildTableField)...))
		if len(columns) == 0 {
			return 0, errors.New("tdengine insert columns required")
		}
		sort.Strings(columns)
		quotedColumns, err := QuoteIdentifiers(columns)
		if err != nil {
			return 0, err
		}
		valueRows := make([]string, 0, len(group.rows))
		for _, row := range group.rows {
			values := make([]string, 0, len(columns))
			for _, col := range columns {
				values = append(values, sqlLiteral(row[col]))
			}
			valueRows = append(valueRows, "("+strings.Join(values, ",")+")")
		}
		query := "INSERT INTO " + group.quotedChildTable +
			" USING " + stable +
			" (" + strings.Join(quotedTagNames, ",") + ")" +
			" TAGS (" + strings.Join(group.tagLiterals, ",") + ")" +
			" (" + strings.Join(quotedColumns, ",") + ")" +
			" VALUES " + strings.Join(valueRows, ",")
		resp, err := c.exec(ctx, query)
		if err != nil {
			return total, err
		}
		if resp.Rows > 0 {
			total += int64(resp.Rows)
		} else {
			total += int64(len(group.rows))
		}
	}
	return total, nil
}

func (c *TDengineConnector) Close() error {
	return nil
}

func (c *TDengineConnector) exec(ctx context.Context, sqlText string) (*tdengineResponse, error) {
	url := c.endpoint + "/rest/sql"
	if strings.TrimSpace(c.database) != "" {
		url += "/" + strings.TrimSpace(c.database)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBufferString(sqlText))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", c.auth)
	req.Header.Set("Content-Type", "text/plain")
	res, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return nil, fmt.Errorf("tdengine http %d: %s", res.StatusCode, strings.TrimSpace(string(body)))
	}
	var parsed tdengineResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, err
	}
	if parsed.Status != "" && !strings.EqualFold(parsed.Status, "succ") && !strings.EqualFold(parsed.Status, "success") {
		if parsed.Desc == "" {
			parsed.Desc = strings.TrimSpace(string(body))
		}
		return nil, errors.New(parsed.Desc)
	}
	if parsed.Status == "" && parsed.Code != 0 {
		if parsed.Desc == "" {
			parsed.Desc = strings.TrimSpace(string(body))
		}
		return nil, errors.New(parsed.Desc)
	}
	return &parsed, nil
}

func buildTDengineWhere(opts QueryOptions) (string, error) {
	clauses := make([]string, 0, 4)
	if strings.TrimSpace(opts.WhereClause) != "" {
		clauses = append(clauses, "("+strings.TrimSpace(opts.WhereClause)+")")
	}
	if strings.TrimSpace(opts.TimeField) != "" {
		field, err := QuoteIdentifier(opts.TimeField)
		if err != nil {
			return "", err
		}
		if strings.TrimSpace(opts.TimeStart) != "" {
			clauses = append(clauses, field+" >= "+sqlLiteral(strings.TrimSpace(opts.TimeStart)))
		}
		if strings.TrimSpace(opts.TimeEnd) != "" {
			clauses = append(clauses, field+" < "+sqlLiteral(strings.TrimSpace(opts.TimeEnd)))
		}
	}
	if strings.TrimSpace(opts.CursorField) != "" && strings.TrimSpace(opts.CursorValue) != "" {
		field, err := QuoteIdentifier(opts.CursorField)
		if err != nil {
			return "", err
		}
		clauses = append(clauses, field+" > "+sqlLiteral(opts.CursorValue))
	}
	if len(clauses) == 0 {
		return "", nil
	}
	return " WHERE " + strings.Join(clauses, " AND "), nil
}

func tdengineDescribeRowIsTag(row []any) bool {
	for _, value := range row[2:] {
		text := strings.ToLower(strings.TrimSpace(fmt.Sprint(value)))
		switch text {
		case "tag", "tags":
			return true
		}
	}
	return false
}

func tdengineChildTableTemplate(opts WriteOptions) string {
	template := strings.TrimSpace(opts.TDengineChildTableTemplate)
	if template != "" {
		return template
	}
	return strings.TrimSpace(opts.TDengineChildTableField)
}

func tdengineTemplateFields(template string) []string {
	template = strings.TrimSpace(template)
	if template == "" {
		return nil
	}
	matches := tdengineTemplateVarPattern.FindAllStringSubmatch(template, -1)
	if len(matches) == 0 {
		return nil
	}
	fields := make([]string, 0, len(matches))
	seen := map[string]bool{}
	for _, match := range matches {
		if len(match) < 2 {
			continue
		}
		field := strings.TrimSpace(match[1])
		key := strings.ToLower(field)
		if field == "" || seen[key] {
			continue
		}
		seen[key] = true
		fields = append(fields, field)
	}
	return fields
}

func tdengineTemplateSourceFields(template string, legacyField string) []string {
	fields := tdengineTemplateFields(template)
	if len(fields) > 0 {
		return fields
	}
	template = strings.TrimSpace(template)
	legacyField = strings.TrimSpace(legacyField)
	if legacyField != "" && strings.EqualFold(template, legacyField) {
		return []string{legacyField}
	}
	return nil
}

func renderTDengineChildTableName(row mapper.Row, template string, legacyField string) (string, error) {
	template = strings.TrimSpace(template)
	if template == "" {
		return "", errors.New("tdengine child table template required")
	}
	fields := tdengineTemplateFields(template)
	legacyField = strings.TrimSpace(legacyField)
	if len(fields) == 0 && legacyField != "" && strings.EqualFold(template, legacyField) {
		value, ok := mapper.Lookup(row, template)
		if !ok || strings.TrimSpace(fmt.Sprint(value)) == "" {
			return "", fmt.Errorf("tdengine child table template field %q missing", template)
		}
		return strings.TrimSpace(fmt.Sprint(value)), nil
	}
	if len(fields) == 0 {
		return template, nil
	}
	missing := make([]string, 0)
	name := tdengineTemplateVarPattern.ReplaceAllStringFunc(template, func(token string) string {
		match := tdengineTemplateVarPattern.FindStringSubmatch(token)
		if len(match) < 2 {
			return token
		}
		field := match[1]
		value, ok := mapper.Lookup(row, field)
		if !ok || strings.TrimSpace(fmt.Sprint(value)) == "" {
			missing = append(missing, field)
			return ""
		}
		return strings.TrimSpace(fmt.Sprint(value))
	})
	if len(missing) > 0 {
		return "", fmt.Errorf("tdengine child table template fields missing: %s", strings.Join(uniqueStringList(missing), ", "))
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return "", errors.New("tdengine child table template rendered empty")
	}
	return name, nil
}

func uniqueStringList(values []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		key := strings.ToLower(value)
		if value == "" || seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, value)
	}
	return out
}

func tdengineColumns(meta [][]any) []string {
	columns := make([]string, 0, len(meta))
	for _, item := range meta {
		if len(item) > 0 {
			columns = append(columns, fmt.Sprint(item[0]))
		}
	}
	return columns
}

func filterColumns(columns []string, ignored []string) []string {
	ignoredSet := make(map[string]bool, len(ignored))
	for _, name := range ignored {
		name = strings.ToLower(strings.TrimSpace(name))
		if name != "" {
			ignoredSet[name] = true
		}
	}
	out := make([]string, 0, len(columns))
	for _, column := range columns {
		if ignoredSet[strings.ToLower(strings.TrimSpace(column))] {
			continue
		}
		out = append(out, column)
	}
	return out
}

func tdengineTableInfos(resp *tdengineResponse, tableType string) []TableInfo {
	items := make([]TableInfo, 0, len(resp.Data))
	for _, row := range resp.Data {
		if len(row) == 0 {
			continue
		}
		name := strings.TrimSpace(fmt.Sprint(row[0]))
		if name == "" {
			continue
		}
		items = append(items, TableInfo{
			Name: name,
			Type: tableType,
		})
	}
	return items
}

func (c *TDengineConnector) tdengineDatabaseInfo(ctx context.Context) (map[string]any, error) {
	if strings.TrimSpace(c.database) == "" {
		return nil, fmt.Errorf("database required")
	}
	if item, err := c.tdengineFirstMatchedRow(ctx, "SELECT * FROM information_schema.ins_databases", "name", "database", "db_name"); err == nil {
		return item, nil
	}
	return c.tdengineDatabaseRows(ctx)
}

func applyTDengineDatabaseInfo(detail *DatabaseDetail, values map[string]any) {
	appendMetric := func(label string, keys ...string) {
		if value, ok := firstNonBlankMapValue(values, keys...); ok {
			detail.Basic.Metrics = append(detail.Basic.Metrics, MetricItem{Label: label, Value: fmt.Sprint(value)})
		}
	}
	setFirstText := func(target *string, keys ...string) {
		if value, ok := firstNonBlankMapValue(values, keys...); ok {
			*target = fmt.Sprint(value)
		}
	}
	setFirstInt64 := func(target *int64, keys ...string) {
		if value, ok := firstNonBlankMapValue(values, keys...); ok {
			if n, err := anyToInt64(value); err == nil {
				*target = n
			}
		}
	}

	setFirstText(&detail.Basic.Charset, "precision")
	setFirstText(&detail.Basic.Collation, "cachemodel", "cache_model")
	setFirstInt64(&detail.Basic.UptimeSeconds, "duration")
	setFirstInt64(&detail.Storage.TotalBytes, "total", "total_bytes")
	setFirstInt64(&detail.Storage.DataBytes, "data", "data_bytes")
	setFirstInt64(&detail.Storage.FreeBytes, "free", "free_bytes")
	setFirstInt64(&detail.TableStats.TotalRows, "rows", "total_rows")
	if value, ok := firstNonBlankMapValue(values, "ntables", "tables"); ok {
		if n, err := anyToInt64(value); err == nil && n > int64(detail.TableStats.TotalTables) {
			detail.TableStats.TotalTables = int(n)
		}
	}

	appendMetric("时间精度", "precision")
	appendMetric("保留策略", "keep", "keep_time")
	appendMetric("副本数", "replica")
	appendMetric("VGroups", "vgroups", "vgroup")
	appendMetric("Buffer", "buffer")
	appendMetric("CacheModel", "cachemodel", "cache_model")
	appendMetric("WAL Level", "wal_level", "wal")
	appendMetric("压缩级别", "comp")
	appendMetric("建库时间", "create_time", "created_time")
}

func (c *TDengineConnector) tdengineTableStats(ctx context.Context) (DatabaseTableStats, error) {
	stableStats, err := c.tdengineStableStats(ctx)
	if err == nil {
		return stableStats, nil
	}
	return c.tdengineTableStatsFromShow(ctx)
}

func (c *TDengineConnector) tdengineStableStats(ctx context.Context) (DatabaseTableStats, error) {
	rows, err := c.tdengineQueryRows(ctx, "SELECT * FROM information_schema.ins_stables")
	if err != nil {
		return DatabaseTableStats{}, err
	}
	stats := DatabaseTableStats{Tables: make([]TableStat, 0)}
	for _, row := range rows {
		dbName := mapString(row, "db_name", "database", "dbname")
		if dbName != "" && !strings.EqualFold(dbName, c.database) {
			continue
		}
		table := tdengineTableStatFromMap(row, "stable")
		if table.Name == "" {
			continue
		}
		stats.TotalViews += 1
		stats.TotalRows += table.Rows
		if len(stats.Tables) < 50 {
			stats.Tables = append(stats.Tables, table)
		}
	}
	return stats, nil
}

func (c *TDengineConnector) tdengineTableStatsFromShow(ctx context.Context) (DatabaseTableStats, error) {
	tables, err := c.ListTables(ctx)
	if err != nil {
		return DatabaseTableStats{}, err
	}
	stats := DatabaseTableStats{Tables: make([]TableStat, 0, minInt(len(tables), 50))}
	for _, item := range tables {
		if item.Type == "stable" {
			stats.TotalViews += 1
		} else {
			stats.TotalTables += 1
		}
		if len(stats.Tables) < 50 {
			stats.Tables = append(stats.Tables, TableStat{
				Name:    item.Name,
				Type:    item.Type,
				Comment: item.Comment,
			})
		}
	}
	return stats, nil
}

func tdengineTableStatFromMap(row map[string]any, fallbackType string) TableStat {
	table := TableStat{
		Name:      mapString(row, "table_name", "stable_name", "name", "tbname", "stable"),
		Type:      mapString(row, "table_type", "type"),
		CreatedAt: mapString(row, "create_time", "created_time"),
		UpdatedAt: mapString(row, "update_time", "updated_time"),
		Comment:   mapString(row, "comment"),
	}
	if table.Type == "" {
		table.Type = fallbackType
	}
	table.Type = normalizeTDengineTableType(table.Type, fallbackType)
	table.Rows = mapInt64(row, "rows", "ntrows", "total_rows")
	table.DataBytes = mapInt64(row, "data_bytes", "data_size", "size")
	table.IndexBytes = mapInt64(row, "index_bytes", "index_size")
	return table
}

func (c *TDengineConnector) tdengineDiskUsage(ctx context.Context) (DatabaseStorageInfo, []MetricItem, error) {
	rows, err := c.tdengineQueryRows(ctx, "SELECT * FROM information_schema.ins_disk_usage")
	if err != nil {
		return DatabaseStorageInfo{}, nil, err
	}
	var storage DatabaseStorageInfo
	metrics := make([]MetricItem, 0)
	for _, row := range rows {
		dbName := mapString(row, "db_name", "database", "dbname")
		if dbName != "" && !strings.EqualFold(dbName, c.database) {
			continue
		}
		dataBytes := mapInt64(row, "data_bytes", "data_size", "data")
		indexBytes := mapInt64(row, "index_bytes", "index_size", "index")
		totalBytes := mapInt64(row, "total_bytes", "total_size", "total")
		freeBytes := mapInt64(row, "free_bytes", "free_size", "free")
		storage.DataBytes += dataBytes
		storage.IndexBytes += indexBytes
		storage.FreeBytes += freeBytes
		storage.TotalBytes += totalBytes
		if mount := mapString(row, "mount_point", "path", "dir"); mount != "" && len(metrics) < 4 {
			metrics = append(metrics, MetricItem{
				Label: mount,
				Value: fmt.Sprint(firstNonZeroInt64(totalBytes, dataBytes+indexBytes)),
				Unit:  "B",
			})
		}
	}
	if storage.TotalBytes == 0 {
		storage.TotalBytes = storage.DataBytes + storage.IndexBytes
	}
	return storage, metrics, nil
}

func (c *TDengineConnector) applyTDenginePerformance(ctx context.Context, detail *DatabaseDetail) ([]MetricItem, error) {
	metrics := make([]MetricItem, 0)
	if rows, err := c.tdengineQueryRows(ctx, "SELECT * FROM information_schema.ins_dnodes"); err == nil {
		var uptime int64
		for _, row := range rows {
			if n := mapInt64(row, "uptime", "uptime_second", "uptime_seconds"); n > uptime {
				uptime = n
			}
		}
		if uptime > 0 {
			detail.Basic.UptimeSeconds = uptime
			metrics = append(metrics, MetricItem{Label: "运行时长", Value: fmt.Sprint(uptime), Unit: "秒"})
		}
	}
	if rows, err := c.tdengineQueryRows(ctx, "SELECT * FROM performance_schema.perf_connections"); err == nil {
		detail.Connection.ThreadsConnected = int64(len(rows))
		detail.Connection.ThreadsRunning = int64(len(rows))
		detail.Performance.Connections = int64(len(rows))
		metrics = append(metrics, MetricItem{Label: "活跃连接", Value: fmt.Sprint(len(rows)), Unit: "个"})
	}
	if rows, err := c.tdengineQueryRows(ctx, "SELECT * FROM performance_schema.perf_apps"); err == nil && len(rows) > 0 {
		detail.Connection.ThreadsConnected = int64(len(rows))
		detail.Performance.Connections = int64(len(rows))
		metrics = append(metrics, MetricItem{Label: "客户端应用", Value: fmt.Sprint(len(rows)), Unit: "个"})
	}
	if rows, err := c.tdengineQueryRows(ctx, "SELECT * FROM performance_schema.perf_queries"); err == nil {
		detail.Performance.Queries = int64(len(rows))
		detail.Performance.QPS = float64(len(rows))
		metrics = append(metrics, MetricItem{Label: "当前查询", Value: fmt.Sprint(len(rows)), Unit: "个"})
	}
	return metrics, nil
}

func (c *TDengineConnector) tdengineClusterNodes(ctx context.Context) (int, error) {
	resp, err := c.exec(ctx, "SHOW CLUSTER")
	if err != nil {
		rows, fallbackErr := c.tdengineQueryRows(ctx, "SELECT * FROM information_schema.ins_dnodes")
		if fallbackErr != nil {
			return 0, err
		}
		return len(rows), nil
	}
	return len(resp.Data), nil
}

func (c *TDengineConnector) tdengineQueryRows(ctx context.Context, sqlText string) ([]map[string]any, error) {
	resp, err := c.exec(ctx, sqlText)
	if err != nil {
		return nil, err
	}
	columns := tdengineColumns(resp.ColumnMeta)
	rows := make([]map[string]any, 0, len(resp.Data))
	for _, values := range resp.Data {
		item := map[string]any{}
		for i, col := range columns {
			if i < len(values) {
				item[strings.ToLower(col)] = values[i]
			}
		}
		rows = append(rows, item)
	}
	return rows, nil
}

func (c *TDengineConnector) tdengineFirstMatchedRow(ctx context.Context, sqlText string, nameKeys ...string) (map[string]any, error) {
	rows, err := c.tdengineQueryRows(ctx, sqlText)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		name := strings.TrimSpace(fmt.Sprint(firstMapValue(row, nameKeys...)))
		if strings.EqualFold(name, c.database) {
			return row, nil
		}
	}
	return nil, fmt.Errorf("database %q not found in %s", c.database, sqlText)
}

func mergeTDengineTableStats(current DatabaseTableStats, next DatabaseTableStats) DatabaseTableStats {
	if next.TotalTables > 0 || len(next.Tables) > 0 {
		current.TotalTables = next.TotalTables
		current.Tables = next.Tables
	}
	if next.TotalViews > 0 {
		current.TotalViews = next.TotalViews
	}
	if next.TotalRows > 0 {
		current.TotalRows = next.TotalRows
	}
	return current
}

func mergeTDengineStorage(current DatabaseStorageInfo, next DatabaseStorageInfo) DatabaseStorageInfo {
	if next.TotalBytes > 0 {
		current.TotalBytes = next.TotalBytes
	}
	if next.DataBytes > 0 {
		current.DataBytes = next.DataBytes
	}
	if next.IndexBytes > 0 {
		current.IndexBytes = next.IndexBytes
	}
	if next.FreeBytes > 0 {
		current.FreeBytes = next.FreeBytes
	}
	return current
}

func (c *TDengineConnector) tdengineDatabaseRows(ctx context.Context) (map[string]any, error) {
	resp, err := c.exec(ctx, "SHOW DATABASES")
	if err != nil {
		return nil, err
	}
	columns := tdengineColumns(resp.ColumnMeta)
	for _, row := range resp.Data {
		item := map[string]any{}
		for i, col := range columns {
			if i < len(row) {
				item[strings.ToLower(col)] = row[i]
			}
		}
		name := strings.TrimSpace(fmt.Sprint(firstMapValue(item, "name", "database", "db_name")))
		if strings.EqualFold(name, c.database) {
			return item, nil
		}
	}
	return nil, fmt.Errorf("database %q not found in SHOW DATABASES", c.database)
}

func sortTDengineTableStats(tables []TableStat) {
	sort.SliceStable(tables, func(i, j int) bool {
		left := tables[i].DataBytes + tables[i].IndexBytes
		right := tables[j].DataBytes + tables[j].IndexBytes
		if left == right {
			return tables[i].Name < tables[j].Name
		}
		return left > right
	})
}

func appendTDengineTableStats(dst []TableStat, src []TableStat, limit int) []TableStat {
	for _, table := range src {
		if len(dst) >= limit {
			break
		}
		dst = append(dst, table)
	}
	return dst
}

func normalizeTDengineTableType(value string, fallback string) string {
	normalized := strings.ToLower(strings.TrimSpace(value))
	switch normalized {
	case "super", "super table", "super_table", "supertable", "stable", "stable_table":
		return "stable"
	case "child", "child table", "child_table", "normal", "normal table", "normal_table", "table":
		return "table"
	case "":
		return fallback
	default:
		return normalized
	}
}

func mapString(values map[string]any, keys ...string) string {
	if value, ok := firstNonBlankMapValue(values, keys...); ok {
		return fmt.Sprint(value)
	}
	return ""
}

func mapInt64(values map[string]any, keys ...string) int64 {
	if value, ok := firstNonBlankMapValue(values, keys...); ok {
		if n, err := anyToInt64(value); err == nil {
			return n
		}
	}
	return 0
}

func firstNonBlankMapValue(values map[string]any, keys ...string) (any, bool) {
	for _, key := range keys {
		value, ok := values[strings.ToLower(key)]
		if !ok || value == nil {
			continue
		}
		if strings.TrimSpace(fmt.Sprint(value)) == "" {
			continue
		}
		return value, true
	}
	return nil, false
}

func firstMapValue(values map[string]any, keys ...string) any {
	for _, key := range keys {
		if value, ok := values[strings.ToLower(key)]; ok {
			return value
		}
	}
	return nil
}

func firstNonZeroInt64(values ...int64) int64 {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func sqlLiteral(value any) string {
	if value == nil {
		return "NULL"
	}
	switch v := value.(type) {
	case string:
		return "'" + strings.ReplaceAll(v, "'", "''") + "'"
	case []byte:
		return "'" + strings.ReplaceAll(string(v), "'", "''") + "'"
	case time.Time:
		return "'" + v.Format("2006-01-02 15:04:05.000") + "'"
	case bool:
		if v {
			return "true"
		}
		return "false"
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return fmt.Sprint(v)
	default:
		return "'" + strings.ReplaceAll(fmt.Sprint(v), "'", "''") + "'"
	}
}

func anyToInt64(value any) (int64, error) {
	switch v := value.(type) {
	case int:
		return int64(v), nil
	case int64:
		return v, nil
	case float64:
		if math.Trunc(v) != v {
			return 0, fmt.Errorf("value %v is not integer", value)
		}
		return int64(v), nil
	case json.Number:
		return v.Int64()
	case string:
		return strconv.ParseInt(strings.TrimSpace(v), 10, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", value)
	}
}
