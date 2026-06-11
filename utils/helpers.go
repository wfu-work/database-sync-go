package utils

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
)

func QueryParams(c *gin.Context) map[string]string {
	params := make(map[string]string)
	for key, val := range c.Request.URL.Query() {
		if len(val) > 0 {
			params[key] = val[0]
		}
	}
	return params
}

func FirstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func Str2Int(value string) int {
	n, _ := strconv.Atoi(strings.TrimSpace(value))
	return n
}

func HashToken(token string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(token)))
	return hex.EncodeToString(sum[:])
}

func BearerToken(c *gin.Context) string {
	auth := strings.TrimSpace(c.GetHeader("Authorization"))
	if strings.HasPrefix(strings.ToLower(auth), "bearer ") {
		return strings.TrimSpace(auth[7:])
	}
	return strings.TrimSpace(c.GetHeader("X-Navmesh-Device-Token"))
}
