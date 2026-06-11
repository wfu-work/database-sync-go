package connector

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"database-sync-go/domains"
	"database-sync-go/syncer/mapper"
	"database-sync-go/utils"
)

type QueryOptions struct {
	Table       string
	WhereClause string
	CursorField string
	CursorValue string
	Limit       int
	Offset      int
}

type WriteOptions struct {
	Table        string
	WriteMode    string
	ConflictKeys []string
}

type TableInfo struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	Comment string `json:"comment"`
}

type ColumnInfo struct {
	Name         string `json:"name"`
	DatabaseType string `json:"databaseType"`
	Nullable     bool   `json:"nullable"`
	PrimaryKey   bool   `json:"primaryKey"`
	Comment      string `json:"comment"`
}

type Connector interface {
	Test() error
	ListTables(ctx context.Context) ([]TableInfo, error)
	DescribeTable(ctx context.Context, table string) ([]ColumnInfo, error)
	Count(ctx context.Context, opts QueryOptions) (int64, error)
	QueryBatch(ctx context.Context, opts QueryOptions) ([]mapper.Row, error)
	WriteBatch(ctx context.Context, rows []mapper.Row, opts WriteOptions) (int64, error)
	Close() error
}

func New(source domains.DataSource) (Connector, error) {
	password, err := utils.DecryptSecret(source.Password)
	if err != nil {
		return nil, fmt.Errorf("decrypt datasource password failed: %w", err)
	}
	source.Password = password
	switch strings.ToLower(strings.TrimSpace(source.Type)) {
	case domains.DataSourceTypeMySQL:
		return NewMySQL(source)
	case domains.DataSourceTypeTDengine:
		return NewTDengine(source)
	default:
		return nil, fmt.Errorf("unsupported datasource type %q", source.Type)
	}
}

var identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func QuoteIdentifier(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", errors.New("identifier required")
	}
	parts := strings.Split(name, ".")
	quoted := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if !identifierPattern.MatchString(part) {
			return "", fmt.Errorf("invalid identifier %q", name)
		}
		quoted = append(quoted, "`"+part+"`")
	}
	return strings.Join(quoted, "."), nil
}

func QuoteIdentifiers(names []string) ([]string, error) {
	out := make([]string, 0, len(names))
	for _, name := range names {
		quoted, err := QuoteIdentifier(name)
		if err != nil {
			return nil, err
		}
		out = append(out, quoted)
	}
	return out, nil
}

func BuildWhere(opts QueryOptions, placeholder string) (string, []any, error) {
	clauses := make([]string, 0, 2)
	args := make([]any, 0, 1)
	if strings.TrimSpace(opts.WhereClause) != "" {
		clauses = append(clauses, "("+strings.TrimSpace(opts.WhereClause)+")")
	}
	if strings.TrimSpace(opts.CursorField) != "" && strings.TrimSpace(opts.CursorValue) != "" {
		field, err := QuoteIdentifier(opts.CursorField)
		if err != nil {
			return "", nil, err
		}
		clauses = append(clauses, field+" > "+placeholder)
		args = append(args, opts.CursorValue)
	}
	if len(clauses) == 0 {
		return "", args, nil
	}
	return " WHERE " + strings.Join(clauses, " AND "), args, nil
}

func Columns(rows []mapper.Row) []string {
	if len(rows) == 0 {
		return nil
	}
	cols := make([]string, 0, len(rows[0]))
	for col := range rows[0] {
		cols = append(cols, col)
	}
	return cols
}

func ColumnSet(columns []ColumnInfo) map[string]bool {
	out := make(map[string]bool, len(columns))
	for _, col := range columns {
		out[strings.ToLower(strings.TrimSpace(col.Name))] = true
	}
	return out
}

func SplitTableName(defaultDatabase string, table string) (string, string, error) {
	table = strings.TrimSpace(table)
	if table == "" {
		return "", "", errors.New("table required")
	}
	parts := strings.Split(table, ".")
	if len(parts) == 1 {
		if !identifierPattern.MatchString(parts[0]) {
			return "", "", fmt.Errorf("invalid table %q", table)
		}
		return strings.TrimSpace(defaultDatabase), parts[0], nil
	}
	if len(parts) == 2 {
		if !identifierPattern.MatchString(parts[0]) || !identifierPattern.MatchString(parts[1]) {
			return "", "", fmt.Errorf("invalid table %q", table)
		}
		return parts[0], parts[1], nil
	}
	return "", "", fmt.Errorf("invalid table %q", table)
}
