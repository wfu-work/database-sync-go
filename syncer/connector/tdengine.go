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
}

type tdengineResponse struct {
	Status     string  `json:"status"`
	Code       int     `json:"code"`
	Desc       string  `json:"desc"`
	ColumnMeta [][]any `json:"column_meta"`
	Data       [][]any `json:"data"`
	Rows       int     `json:"rows"`
}

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
	}, nil
}

func (c *TDengineConnector) Test() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := c.exec(ctx, "SELECT SERVER_VERSION()")
	return err
}

func (c *TDengineConnector) ListTables(ctx context.Context) ([]TableInfo, error) {
	tableResp, err := c.exec(ctx, "SHOW TABLES")
	if err != nil {
		return nil, err
	}
	items := tdengineTableInfos(tableResp, "table")
	stableResp, err := c.exec(ctx, "SHOW STABLES")
	if err == nil {
		items = append(items, tdengineTableInfos(stableResp, "stable")...)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Type == items[j].Type {
			return items[i].Name < items[j].Name
		}
		return items[i].Type < items[j].Type
	})
	return items, nil
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
	query := "SELECT * FROM " + table + where
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

func (c *TDengineConnector) WriteBatch(ctx context.Context, rows []mapper.Row, opts WriteOptions) (int64, error) {
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
	clauses := make([]string, 0, 2)
	if strings.TrimSpace(opts.WhereClause) != "" {
		clauses = append(clauses, "("+strings.TrimSpace(opts.WhereClause)+")")
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

func tdengineColumns(meta [][]any) []string {
	columns := make([]string, 0, len(meta))
	for _, item := range meta {
		if len(item) > 0 {
			columns = append(columns, fmt.Sprint(item[0]))
		}
	}
	return columns
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
