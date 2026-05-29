package scheduler

import (
	"fmt"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/peifengstudio/erminetq/internal/store"
)

// standardParser handles 5-field cron expressions in the form:
//
//	minute hour day-of-month month day-of-week
//
// Example: "0 9 * * 1-5" — weekdays at 09:00.
var standardParser = cron.NewParser(
	cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow,
)

// computeNextRun returns the next scheduled trigger time for sc strictly after
// `after`.
//
// Cron schedules: the cron expression is parsed and the next matching instant
// after `after` is returned.  An unparseable expression returns an error.
//
// Interval schedules: next = sc.LastRunAt + interval_secs.  When sc.LastRunAt
// is nil (the schedule has never run) `after` is used as the base, so the
// first trigger is one interval from now.
//
// A schedule that has neither cron_expr nor interval_secs returns an error.
func computeNextRun(sc *store.Schedule, after time.Time) (time.Time, error) {
	switch {
	case sc.CronExpr != nil:
		sched, err := standardParser.Parse(*sc.CronExpr)
		if err != nil {
			return time.Time{}, fmt.Errorf("computeNextRun: invalid cron expression %q: %w", *sc.CronExpr, err)
		}
		return sched.Next(after), nil

	case sc.IntervalSecs != nil:
		base := after
		if sc.LastRunAt != nil {
			base = *sc.LastRunAt
		}
		return base.Add(time.Duration(*sc.IntervalSecs) * time.Second), nil

	default:
		return time.Time{}, fmt.Errorf("computeNextRun: schedule %q has neither cron_expr nor interval_secs", sc.ID)
	}
}
