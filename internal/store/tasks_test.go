package store_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/peifengstudio/erminetq/internal/store"
)

func TestCreateTask_FieldRoundTrip(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	timeout := 120
	parentID := "parent-001"
	scheduleID := "sched-001"
	payload := json.RawMessage(`{"url":"https://example.com"}`)
	runAt := time.Now().UTC().Truncate(time.Second).Add(5 * time.Minute)

	in := store.CreateTaskInput{
		Type:        "crawl_url",
		Queue:       "high",
		Payload:     payload,
		Priority:    10,
		MaxRetries:  5,
		TimeoutSecs: &timeout,
		RunAt:       runAt,
		ParentID:    &parentID,
		ScheduleID:  &scheduleID,
	}

	task, err := s.CreateTask(ctx, in)
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	if task.ID == "" {
		t.Error("ID should be set")
	}
	if task.Type != "crawl_url" {
		t.Errorf("Type = %q, want %q", task.Type, "crawl_url")
	}
	if task.Queue != "high" {
		t.Errorf("Queue = %q, want %q", task.Queue, "high")
	}
	if string(task.Payload) != string(payload) {
		t.Errorf("Payload = %s, want %s", task.Payload, payload)
	}
	if task.Status != store.TaskStatusQueued {
		t.Errorf("Status = %q, want %q", task.Status, store.TaskStatusQueued)
	}
	if task.Priority != 10 {
		t.Errorf("Priority = %d, want 10", task.Priority)
	}
	if task.RetryCount != 0 {
		t.Errorf("RetryCount = %d, want 0", task.RetryCount)
	}
	if task.MaxRetries != 5 {
		t.Errorf("MaxRetries = %d, want 5", task.MaxRetries)
	}
	if task.TimeoutSecs == nil || *task.TimeoutSecs != 120 {
		t.Errorf("TimeoutSecs = %v, want 120", task.TimeoutSecs)
	}
	if !task.RunAt.Equal(runAt) {
		t.Errorf("RunAt = %v, want %v", task.RunAt, runAt)
	}
	if task.ParentID == nil || *task.ParentID != parentID {
		t.Errorf("ParentID = %v, want %q", task.ParentID, parentID)
	}
	if task.ScheduleID == nil || *task.ScheduleID != scheduleID {
		t.Errorf("ScheduleID = %v, want %q", task.ScheduleID, scheduleID)
	}
	if task.CreatedAt.IsZero() {
		t.Error("CreatedAt should not be zero")
	}
	if task.UpdatedAt.IsZero() {
		t.Error("UpdatedAt should not be zero")
	}
}

func TestCreateTask_Defaults(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	before := time.Now().UTC().Truncate(time.Second)

	task, err := s.CreateTask(ctx, store.CreateTaskInput{Type: "send_email"})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	if task.Queue != "default" {
		t.Errorf("Queue = %q, want %q", task.Queue, "default")
	}
	if task.MaxRetries != 3 {
		t.Errorf("MaxRetries = %d, want 3", task.MaxRetries)
	}
	if task.TimeoutSecs != nil {
		t.Errorf("TimeoutSecs = %v, want nil", task.TimeoutSecs)
	}
	if task.ParentID != nil {
		t.Errorf("ParentID = %v, want nil", task.ParentID)
	}
	if task.ScheduleID != nil {
		t.Errorf("ScheduleID = %v, want nil", task.ScheduleID)
	}
	if task.RunAt.Before(before) {
		t.Errorf("RunAt %v should not be before %v", task.RunAt, before)
	}
	if task.Payload != nil {
		t.Errorf("Payload = %v, want nil", task.Payload)
	}
}

func TestGetTask_RoundTrip(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	created, err := s.CreateTask(ctx, store.CreateTaskInput{
		Type:    "run_script",
		Queue:   "batch",
		Payload: json.RawMessage(`{"script":"etl.py"}`),
	})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	got, err := s.GetTask(ctx, created.ID)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}

	if got.ID != created.ID {
		t.Errorf("ID mismatch: %q vs %q", got.ID, created.ID)
	}
	if got.Type != created.Type {
		t.Errorf("Type mismatch: %q vs %q", got.Type, created.Type)
	}
	if got.Queue != created.Queue {
		t.Errorf("Queue mismatch: %q vs %q", got.Queue, created.Queue)
	}
	if string(got.Payload) != string(created.Payload) {
		t.Errorf("Payload mismatch: %s vs %s", got.Payload, created.Payload)
	}
	if got.Status != store.TaskStatusQueued {
		t.Errorf("Status = %q, want queued", got.Status)
	}
}

func TestGetTask_NotFound(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	_, err := s.GetTask(ctx, "nonexistent-id")
	if err != store.ErrNotFound {
		t.Errorf("GetTask = %v, want ErrNotFound", err)
	}
}

func TestListTasks_NoFilter(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	for _, typ := range []string{"job_a", "job_b", "job_c"} {
		if _, err := s.CreateTask(ctx, store.CreateTaskInput{Type: typ}); err != nil {
			t.Fatalf("CreateTask(%s): %v", typ, err)
		}
	}

	tasks, err := s.ListTasks(ctx, store.ListTasksFilter{})
	if err != nil {
		t.Fatalf("ListTasks: %v", err)
	}
	if len(tasks) != 3 {
		t.Errorf("len(tasks) = %d, want 3", len(tasks))
	}
}

func TestListTasks_FilterByStatus(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	for _, typ := range []string{"a", "b", "c"} {
		if _, err := s.CreateTask(ctx, store.CreateTaskInput{Type: typ}); err != nil {
			t.Fatalf("CreateTask: %v", err)
		}
	}

	status := store.TaskStatusQueued
	tasks, err := s.ListTasks(ctx, store.ListTasksFilter{Status: &status})
	if err != nil {
		t.Fatalf("ListTasks: %v", err)
	}
	if len(tasks) != 3 {
		t.Errorf("len(tasks) = %d, want 3", len(tasks))
	}

	notExist := store.TaskStatusRunning
	tasks, err = s.ListTasks(ctx, store.ListTasksFilter{Status: &notExist})
	if err != nil {
		t.Fatalf("ListTasks: %v", err)
	}
	if len(tasks) != 0 {
		t.Errorf("len(tasks) = %d, want 0", len(tasks))
	}
}

func TestListTasks_FilterByTypeAndQueue(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	inputs := []store.CreateTaskInput{
		{Type: "crawl", Queue: "high"},
		{Type: "crawl", Queue: "low"},
		{Type: "index", Queue: "high"},
	}
	for _, in := range inputs {
		if _, err := s.CreateTask(ctx, in); err != nil {
			t.Fatalf("CreateTask: %v", err)
		}
	}

	typ := "crawl"
	tasks, err := s.ListTasks(ctx, store.ListTasksFilter{Type: &typ})
	if err != nil {
		t.Fatalf("ListTasks by type: %v", err)
	}
	if len(tasks) != 2 {
		t.Errorf("type=crawl: len=%d, want 2", len(tasks))
	}

	q := "high"
	tasks, err = s.ListTasks(ctx, store.ListTasksFilter{Queue: &q})
	if err != nil {
		t.Fatalf("ListTasks by queue: %v", err)
	}
	if len(tasks) != 2 {
		t.Errorf("queue=high: len=%d, want 2", len(tasks))
	}
}

func TestListTasks_Pagination(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		if _, err := s.CreateTask(ctx, store.CreateTaskInput{Type: "job"}); err != nil {
			t.Fatalf("CreateTask: %v", err)
		}
	}

	page1, err := s.ListTasks(ctx, store.ListTasksFilter{Limit: 3, Offset: 0})
	if err != nil {
		t.Fatalf("ListTasks page1: %v", err)
	}
	if len(page1) != 3 {
		t.Errorf("page1 len=%d, want 3", len(page1))
	}

	page2, err := s.ListTasks(ctx, store.ListTasksFilter{Limit: 3, Offset: 3})
	if err != nil {
		t.Fatalf("ListTasks page2: %v", err)
	}
	if len(page2) != 2 {
		t.Errorf("page2 len=%d, want 2", len(page2))
	}

	// IDs must not overlap
	ids := make(map[string]bool)
	for _, task := range append(page1, page2...) {
		if ids[task.ID] {
			t.Errorf("duplicate ID %q across pages", task.ID)
		}
		ids[task.ID] = true
	}
}

func TestListTasks_PriorityOrdering(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	if _, err := s.CreateTask(ctx, store.CreateTaskInput{Type: "low", Priority: 1}); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if _, err := s.CreateTask(ctx, store.CreateTaskInput{Type: "high", Priority: 10}); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if _, err := s.CreateTask(ctx, store.CreateTaskInput{Type: "mid", Priority: 5}); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	tasks, err := s.ListTasks(ctx, store.ListTasksFilter{})
	if err != nil {
		t.Fatalf("ListTasks: %v", err)
	}
	if len(tasks) != 3 {
		t.Fatalf("len=%d, want 3", len(tasks))
	}
	if tasks[0].Type != "high" || tasks[1].Type != "mid" || tasks[2].Type != "low" {
		t.Errorf("order: %s %s %s, want high mid low",
			tasks[0].Type, tasks[1].Type, tasks[2].Type)
	}
}

func TestListTasks_EmptyResult(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	tasks, err := s.ListTasks(ctx, store.ListTasksFilter{})
	if err != nil {
		t.Fatalf("ListTasks: %v", err)
	}
	if tasks != nil {
		t.Errorf("expected nil slice for empty result, got %v", tasks)
	}
}
