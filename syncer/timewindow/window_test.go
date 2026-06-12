package timewindow

import (
	"context"
	"testing"
	"time"
)

func TestParseRangeUsesNowWhenEndEmpty(t *testing.T) {
	now := time.Date(2026, 6, 12, 15, 30, 0, 0, time.Local)
	got, err := ParseRange("2026-06-10", "", now)
	if err != nil {
		t.Fatalf("ParseRange failed: %v", err)
	}
	if !got.End.Equal(now) {
		t.Fatalf("end = %v, want %v", got.End, now)
	}
	if got.Start.Format("2006-01-02 15:04:05") != "2026-06-10 00:00:00" {
		t.Fatalf("unexpected start: %v", got.Start)
	}
}

func TestParseRangeDateEndIsInclusiveDay(t *testing.T) {
	now := time.Date(2026, 6, 12, 15, 30, 0, 0, time.Local)
	got, err := ParseRange("2026-06-10", "2026-06-12", now)
	if err != nil {
		t.Fatalf("ParseRange failed: %v", err)
	}
	if got.End.Format("2006-01-02 15:04:05") != "2026-06-13 00:00:00" {
		t.Fatalf("unexpected end: %v", got.End)
	}
}

func TestForEachDayBackward(t *testing.T) {
	value := Range{
		Start: time.Date(2026, 6, 10, 10, 0, 0, 0, time.Local),
		End:   time.Date(2026, 6, 12, 15, 30, 0, 0, time.Local),
	}
	var windows []Window
	if err := ForEachDayBackward(context.Background(), value, func(window Window) error {
		windows = append(windows, window)
		return nil
	}); err != nil {
		t.Fatalf("ForEachDayBackward failed: %v", err)
	}
	got := make([]string, 0, len(windows))
	for _, window := range windows {
		got = append(got, window.Start.Format("01-02 15:04")+" -> "+window.End.Format("01-02 15:04"))
	}
	want := []string{
		"06-12 00:00 -> 06-12 15:30",
		"06-11 00:00 -> 06-12 00:00",
		"06-10 10:00 -> 06-11 00:00",
	}
	if len(got) != len(want) {
		t.Fatalf("windows = %#v, want %#v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("window %d = %q, want %q", i, got[i], want[i])
		}
	}
}
