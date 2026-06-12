package mapper

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type FieldMapping struct {
	Source    string `json:"source,omitempty"`
	Target    string `json:"target"`
	Default   any    `json:"default,omitempty"`
	Transform string `json:"transform,omitempty"`
}

type TagMapping struct {
	Name      string `json:"name"`
	Source    string `json:"source,omitempty"`
	Default   any    `json:"default,omitempty"`
	Transform string `json:"transform,omitempty"`
}

type Row map[string]any

func Validate(fields []FieldMapping) error {
	if len(fields) == 0 {
		return errors.New("field mapping required")
	}
	for i, field := range fields {
		if strings.TrimSpace(field.Target) == "" {
			return fmt.Errorf("field mapping[%d].target required", i)
		}
		if strings.TrimSpace(field.Source) == "" && field.Default == nil {
			return fmt.Errorf("field mapping[%d].source or default required", i)
		}
	}
	return nil
}

func ValidateTags(tags []TagMapping) error {
	for i, tag := range tags {
		if strings.TrimSpace(tag.Name) == "" {
			return fmt.Errorf("tag mapping[%d].name required", i)
		}
		if strings.TrimSpace(tag.Source) == "" && tag.Default == nil {
			return fmt.Errorf("tag mapping[%d].source or default required", i)
		}
	}
	return nil
}

func MapRows(rows []Row, fields []FieldMapping) ([]Row, error) {
	out := make([]Row, 0, len(rows))
	for _, row := range rows {
		mapped := Row{}
		for _, field := range fields {
			value, ok := lookup(row, field.Source)
			if !ok {
				value = field.Default
			}
			transformed, err := Transform(value, field.Transform)
			if err != nil {
				return nil, fmt.Errorf("map %s to %s failed: %w", field.Source, field.Target, err)
			}
			mapped[strings.TrimSpace(field.Target)] = transformed
		}
		out = append(out, mapped)
	}
	return out, nil
}

func MapTagValues(row Row, tags []TagMapping) (Row, error) {
	mapped := Row{}
	for _, tag := range tags {
		value, ok := lookup(row, tag.Source)
		if !ok {
			value = tag.Default
		}
		transformed, err := Transform(value, tag.Transform)
		if err != nil {
			return nil, fmt.Errorf("map %s to tag %s failed: %w", tag.Source, tag.Name, err)
		}
		mapped[strings.TrimSpace(tag.Name)] = transformed
	}
	return mapped, nil
}

func Lookup(row Row, key string) (any, bool) {
	return lookup(row, key)
}

func lookup(row Row, key string) (any, bool) {
	key = strings.TrimSpace(key)
	if key == "" {
		return nil, false
	}
	if value, ok := row[key]; ok {
		return value, true
	}
	for rowKey, value := range row {
		if strings.EqualFold(rowKey, key) {
			return value, true
		}
	}
	return nil, false
}

func Transform(value any, name string) (any, error) {
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" {
		return value, nil
	}
	switch name {
	case "string":
		return toString(value), nil
	case "int":
		return toInt64(value)
	case "float":
		return toFloat64(value)
	case "bool":
		return toBool(value)
	case "time_to_millis":
		return toMillis(value)
	case "millis_to_time":
		return millisToTime(value)
	default:
		return nil, fmt.Errorf("unsupported transform %q", name)
	}
}

func toString(value any) string {
	if value == nil {
		return ""
	}
	switch v := value.(type) {
	case []byte:
		return string(v)
	case time.Time:
		return v.Format(time.RFC3339Nano)
	default:
		return fmt.Sprint(v)
	}
}

func toInt64(value any) (int64, error) {
	switch v := value.(type) {
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case uint:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		return int64(v), nil
	case float32:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case []byte:
		return strconv.ParseInt(strings.TrimSpace(string(v)), 10, 64)
	case string:
		return strconv.ParseInt(strings.TrimSpace(v), 10, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to int", value)
	}
}

func toFloat64(value any) (float64, error) {
	switch v := value.(type) {
	case int:
		return float64(v), nil
	case int8:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case uint:
		return float64(v), nil
	case uint8:
		return float64(v), nil
	case uint16:
		return float64(v), nil
	case uint32:
		return float64(v), nil
	case uint64:
		return float64(v), nil
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	case []byte:
		return strconv.ParseFloat(strings.TrimSpace(string(v)), 64)
	case string:
		return strconv.ParseFloat(strings.TrimSpace(v), 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to float", value)
	}
}

func toBool(value any) (bool, error) {
	switch v := value.(type) {
	case bool:
		return v, nil
	case int:
		return v != 0, nil
	case int64:
		return v != 0, nil
	case float64:
		return v != 0, nil
	case []byte:
		return strconv.ParseBool(strings.TrimSpace(string(v)))
	case string:
		text := strings.TrimSpace(v)
		if text == "1" {
			return true, nil
		}
		if text == "0" {
			return false, nil
		}
		return strconv.ParseBool(text)
	default:
		return false, fmt.Errorf("cannot convert %T to bool", value)
	}
}

func toMillis(value any) (int64, error) {
	switch v := value.(type) {
	case time.Time:
		return v.UnixMilli(), nil
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case []byte:
		return parseTimeToMillis(string(v))
	case string:
		return parseTimeToMillis(v)
	default:
		return 0, fmt.Errorf("cannot convert %T to millis", value)
	}
}

func parseTimeToMillis(value string) (int64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, errors.New("empty time")
	}
	if n, err := strconv.ParseInt(value, 10, 64); err == nil {
		return n, nil
	}
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}
	var lastErr error
	for _, layout := range layouts {
		t, err := time.ParseInLocation(layout, value, time.Local)
		if err == nil {
			return t.UnixMilli(), nil
		}
		lastErr = err
	}
	return 0, lastErr
}

func millisToTime(value any) (time.Time, error) {
	n, err := toInt64(value)
	if err != nil {
		return time.Time{}, err
	}
	return time.UnixMilli(n), nil
}
