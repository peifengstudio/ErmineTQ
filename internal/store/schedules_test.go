package store_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/peifengstudio/erminetq/internal/store"
)

func ptr[T any](v T) *T { return &v }

func TestCreateSchedule_CronExpr(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	nextRun := time.Now().UTC().Truncate(time.Second).Add(time.Hour)
	payload := json.RawMessage(`{"limit":100}`)

	in := store.CreateScheduleInput{
		TaskType:  "crawl_url",
		Queue:     "batch",
		Payload:   payload,
		CronExpr:  ptr("0 9 * * 1-5"),
		Enabled:   true,
		NextRunAt: &nextRun,
	}

	sc, err := s.CreateSchedule(ctx, in)
	if err != nil {
		t.Fatalf("CreateSchedule: %v", err)
	}

	if sc.ID == "" {
		t.Error("ID should be set")
	}
	if sc.TaskType != "crawl_url" {
		t.Errorf("TaskType = %q, want crawl_url", sc.TaskType)
	}
	if sc.Queue != "batch" {
		t.Errorf("Queue = %q, want batch", sc.Queue)
	}
	if string(sc.Payload) != string(payload) {
		t.Errorf("Payload = %s, want %s", sc.Payload, payload)
	}
	if sc.CronExpr == nil || *sc.CronExpr != "0 9 * * 1-5" {
		t.Errorf("CronExpr = %v, want %q", sc.CronExpr, "0 9 * * 1-5")
	}
	if sc.IntervalSecs != nil {
		t.Errorf("IntervalSecs = %v, want nil", sc.IntervalSecs)
	}
	if !sc.Enabled {
		t.Error("Enabled should be true")
	}
	if sc.LastRunAt != nil {
		t.Errorf("LastRunAt = %v, want nil", sc.LastRunAt)
	}
	if sc.NextRunAt == nil || !sc.NextRunAt.Equal(nextRun) {
		t.Errorf("NextRunAt = %v, want %v", sc.NextRunAt, nextRun)
	}
	if sc.CreatedAt.IsZero() {
		t.Error("CreatedAt should not be zero")
	}
}

func TestCreateSchedule_IntervalSecs(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	sc, err := s.CreateSchedule(ctx, store.CreateScheduleInput{
		TaskType:     "heartbeat",
		IntervalSecs: ptr(3600),
		Enabled:      true,
	})
	if err != nil {
		t.Fatalf("CreateSchedule: %v", err)
	}

	if sc.IntervalSecs == nil || *sc.IntervalSecs != 3600 {
		t.Errorf("IntervalSecs = %v, want 3600", sc.IntervalSecs)
	}
	if sc.CronExpr != nil {
		t.Errorf("CronExpr = %v, want nil", sc.CronExpr)
	}
}

func TestCreateSchedule_Defaults(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	sc, err := s.CreateSchedule(ctx, store.CreateScheduleInput{
		TaskType: "noop",
	})
	if err != nil {
		t.Fatalf("CreateSchedule: %v", err)
	}

	if sc.Queue != "default" {
		t.Errorf("Queue = %q, want default", sc.Queue)
	}
	if sc.Enabled {
		t.Error("Enabled should be false by default")
	}
	if sc.Payload != nil {
		t.Errorf("Payload = %v, want nil", sc.Payload)
	}
	if sc.CronExpr != nil {
		t.Errorf("CronExpr = %v, want nil", sc.CronExpr)
	}
	if sc.IntervalSecs != nil {
		t.Errorf("IntervalSecs = %v, want nil", sc.IntervalSecs)
	}
	if sc.NextRunAt != nil {
		t.Errorf("NextRunAt = %v, want nil", sc.NextRunAt)
	}
}

func TestGetSchedule_RoundTrip(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	created, err := s.CreateSchedule(ctx, store.CreateScheduleInput{
		TaskType: "daily_report",
		CronExpr: ptr("0 8 * * *"),
		Enabled:  true,
	})
	if err != nil {
		t.Fatalf("CreateSchedule: %v", err)
	}

	got, err := s.GetSchedule(ctx, created.ID)
	if err != nil {
		t.Fatalf("GetSchedule: %v", err)
	}

	if got.ID != created.ID {
		t.Errorf("ID mismatch: %q vs %q", got.ID, created.ID)
	}
	if got.TaskType != created.TaskType {
		t.Errorf("TaskType mismatch: %q vs %q", got.TaskType, created.TaskType)
	}
	if got.CronExpr == nil || *got.CronExpr != *created.CronExpr {
		t.Errorf("CronExpr mismatch: %v vs %v", got.CronExpr, created.CronExpr)
	}
	if got.Enabled != created.Enabled {
		t.Errorf("Enabled mismatch: %v vs %v", got.Enabled, created.Enabled)
	}
}

func TestGetSchedule_NotFound(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	_, err := s.GetSchedule(ctx, "nonexistent-schedule")
	if err != store.ErrNotFound {
		t.Errorf("GetSchedule = %v, want ErrNotFound", err)
	}
}

func TestListSchedules_Empty(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	schedules, err := s.ListSchedules(ctx)
	if err != nil {
		t.Fatalf("ListSchedules: %v", err)
	}
	if schedules != nil {
		t.Errorf("expected nil slice for empty result, got %v", schedules)
	}
}

func TestListSchedules_Multiple(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	for _, tt := range []string{"job_a", "job_b", "job_c"} {
		if _, err := s.CreateSchedule(ctx, store.CreateScheduleInput{
			TaskType: tt,
			Enabled:  true,
		}); err != nil {
			t.Fatalf("CreateSchedule(%s): %v", tt, err)
		}
	}

	schedules, err := s.ListSchedules(ctx)
	if err != nil {
		t.Fatalf("ListSchedules: %v", err)
	}
	if len(schedules) != 3 {
		t.Errorf("len(schedules) = %d, want 3", len(schedules))
	}
}

func TestUpdateSchedule_EnableDisable(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	sc, err := s.CreateSchedule(ctx, store.CreateScheduleInput{
		TaskType: "job",
		Enabled:  true,
	})
	if err != nil {
		t.Fatalf("CreateSchedule: %v", err)
	}

	updated, err := s.UpdateSchedule(ctx, sc.ID, store.UpdateScheduleInput{
		Enabled: ptr(false),
	})
	if err != nil {
		t.Fatalf("UpdateSchedule: %v", err)
	}
	if updated.Enabled {
		t.Error("Enabled should be false after update")
	}

	reenabled, err := s.UpdateSchedule(ctx, sc.ID, store.UpdateScheduleInput{
		Enabled: ptr(true),
	})
	if err != nil {
		t.Fatalf("UpdateSchedule re-enable: %v", err)
	}
	if !reenabled.Enabled {
		t.Error("Enabled should be true after re-enable")
	}
}

func TestUpdateSchedule_NextRunAt(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	sc, err := s.CreateSchedule(ctx, store.CreateScheduleInput{
		TaskType: "job",
		Enabled:  true,
	})
	if err != nil {
		t.Fatalf("CreateSchedule: %v", err)
	}

	next := time.Now().UTC().Truncate(time.Second).Add(2 * time.Hour)
	updated, err := s.UpdateSchedule(ctx, sc.ID, store.UpdateScheduleInput{
		NextRunAt: &next,
	})
	if err != nil {
		t.Fatalf("UpdateSchedule: %v", err)
	}
	if updated.NextRunAt == nil || !updated.NextRunAt.Equal(next) {
		t.Errorf("NextRunAt = %v, want %v", updated.NextRunAt, next)
	}
}

func TestUpdateSchedule_MultipleFields(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	sc, err := s.CreateSchedule(ctx, store.CreateScheduleInput{
		TaskType: "report",
		CronExpr: ptr("0 8 * * *"),
		Enabled:  true,
	})
	if err != nil {
		t.Fatalf("CreateSchedule: %v", err)
	}

	interval := 7200
	updated, err := s.UpdateSchedule(ctx, sc.ID, store.UpdateScheduleInput{
		Enabled:      ptr(false),
		IntervalSecs: &interval,
	})
	if err != nil {
		t.Fatalf("UpdateSchedule: %v", err)
	}
	if updated.Enabled {
		t.Error("Enabled should be false")
	}
	if updated.IntervalSecs == nil || *updated.IntervalSecs != 7200 {
		t.Errorf("IntervalSecs = %v, want 7200", updated.IntervalSecs)
	}
	// CronExpr must be unchanged
	if updated.CronExpr == nil || *updated.CronExpr != "0 8 * * *" {
		t.Errorf("CronExpr = %v, want unchanged", updated.CronExpr)
	}
}

func TestUpdateSchedule_NoFields(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	sc, err := s.CreateSchedule(ctx, store.CreateScheduleInput{
		TaskType: "job",
		Enabled:  true,
	})
	if err != nil {
		t.Fatalf("CreateSchedule: %v", err)
	}

	// Empty update should return current schedule unchanged
	got, err := s.UpdateSchedule(ctx, sc.ID, store.UpdateScheduleInput{})
	if err != nil {
		t.Fatalf("UpdateSchedule: %v", err)
	}
	if got.ID != sc.ID {
		t.Errorf("ID changed: %q vs %q", got.ID, sc.ID)
	}
}

func TestUpdateSchedule_NotFound(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	_, err := s.UpdateSchedule(ctx, "nonexistent", store.UpdateScheduleInput{
		Enabled: ptr(true),
	})
	if err != store.ErrNotFound {
		t.Errorf("UpdateSchedule = %v, want ErrNotFound", err)
	}
}
