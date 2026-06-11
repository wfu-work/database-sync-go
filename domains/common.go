package domains

import "time"

func NowMilli() int64 {
	return time.Now().UnixMilli()
}

type Status int

const (
	StatusDisabled Status = 0
	StatusEnabled  Status = 1
)
