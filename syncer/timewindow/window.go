package timewindow

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

const SQLTimeLayout = "2006-01-02 15:04:05.000"

type Range struct {
	Start time.Time
	End   time.Time
}

type Window struct {
	Start time.Time
	End   time.Time
}

func ParseRange(startValue string, endValue string, now time.Time) (Range, error) {
	startValue = strings.TrimSpace(startValue)
	endValue = strings.TrimSpace(endValue)
	if startValue == "" {
		return Range{}, errors.New("syncStartDate required for tdengine source")
	}
	if now.IsZero() {
		now = time.Now()
	}
	loc := now.Location()
	start, err := parseBoundary(startValue, false, loc)
	if err != nil {
		return Range{}, fmt.Errorf("invalid syncStartDate: %w", err)
	}
	end := now
	if endValue != "" {
		end, err = parseBoundary(endValue, true, loc)
		if err != nil {
			return Range{}, fmt.Errorf("invalid syncEndDate: %w", err)
		}
	}
	if !end.After(start) {
		return Range{}, errors.New("syncEndDate must be after syncStartDate")
	}
	return Range{Start: start, End: end}, nil
}

func ForEachDayBackward(ctx context.Context, value Range, fn func(Window) error) error {
	cursorEnd := value.End
	for cursorEnd.After(value.Start) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		windowStart := startOfDay(cursorEnd)
		if !cursorEnd.After(windowStart) {
			windowStart = windowStart.AddDate(0, 0, -1)
		}
		if windowStart.Before(value.Start) {
			windowStart = value.Start
		}
		window := Window{Start: windowStart, End: cursorEnd}
		if !window.End.After(window.Start) {
			break
		}
		if err := fn(window); err != nil {
			return err
		}
		cursorEnd = windowStart
	}
	return nil
}

func FirstDayBackward(value Range) Window {
	cursorEnd := value.End
	windowStart := startOfDay(cursorEnd)
	if !cursorEnd.After(windowStart) {
		windowStart = windowStart.AddDate(0, 0, -1)
	}
	if windowStart.Before(value.Start) {
		windowStart = value.Start
	}
	return Window{Start: windowStart, End: cursorEnd}
}

func FormatSQL(value time.Time) string {
	return value.Format(SQLTimeLayout)
}

func parseBoundary(value string, endOfDate bool, loc *time.Location) (time.Time, error) {
	if parsed, err := time.ParseInLocation("2006-01-02", value, loc); err == nil {
		if endOfDate {
			return parsed.AddDate(0, 0, 1), nil
		}
		return parsed, nil
	}
	layouts := []string{
		"2006-01-02 15:04:05.000",
		"2006-01-02 15:04:05",
		"2006-01-02 15:04",
		"2006-01-02T15:04:05",
		"2006-01-02T15:04",
		time.RFC3339,
	}
	var lastErr error
	for _, layout := range layouts {
		parsed, err := time.ParseInLocation(layout, value, loc)
		if err == nil {
			return parsed, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = errors.New("unsupported date format")
	}
	return time.Time{}, lastErr
}

func startOfDay(value time.Time) time.Time {
	year, month, day := value.Date()
	return time.Date(year, month, day, 0, 0, 0, 0, value.Location())
}
