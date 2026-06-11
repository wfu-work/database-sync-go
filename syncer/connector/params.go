package connector

import (
	"encoding/json"
	"net/url"
	"strings"
)

func paramsMap(raw string) map[string]string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return map[string]string{}
	}
	var parsed map[string]string
	if err := json.Unmarshal([]byte(raw), &parsed); err == nil {
		return parsed
	}
	values, err := url.ParseQuery(raw)
	if err != nil {
		return map[string]string{}
	}
	parsed = map[string]string{}
	for key, val := range values {
		if len(val) > 0 {
			parsed[key] = val[0]
		}
	}
	return parsed
}
