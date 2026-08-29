package recorder

import (
	"context"
	"errors"
	"time"

	"github.com/spectado/stream-recorder/internal/schedule"
)

// RunSchedulePoller loads the cached schedule (if any) synchronously, then
// fetches the live schedule immediately and on every poll interval. Failures
// never discard the last known schedule.
func (m *Manager) RunSchedulePoller(ctx context.Context, f *schedule.Fetcher) {
	m.mu.Lock()
	m.info.URL = f.URL
	loaded := m.sched != nil
	m.mu.Unlock()

	if !loaded {
		if cached, err := f.LoadCache(); err == nil {
			m.mu.Lock()
			m.applyScheduleLocked(cached)
			m.mu.Unlock()
			m.log.Info("loaded cached schedule (will refresh from URL)", "items", len(cached.Items),
				"fetchedAt", cached.FetchedAt.UTC().Format(time.RFC3339))
			m.Reconcile(time.Now())
		}
	}

	m.pollOnce(ctx, f, true)

	// Until a schedule is loaded retry quickly (the schedule server may still
	// be starting); afterwards use the configured interval.
	const bootstrapInterval = 5 * time.Second
	interval := func() time.Duration {
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.sched == nil && bootstrapInterval < m.cfg.SchedulePollInterval {
			return bootstrapInterval
		}
		return m.cfg.SchedulePollInterval
	}
	t := time.NewTimer(interval())
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.pollOnce(ctx, f, false)
			t.Reset(interval())
		}
	}
}

func (m *Manager) pollOnce(ctx context.Context, f *schedule.Fetcher, initial bool) {
	fctx, cancel := context.WithTimeout(ctx, m.cfg.ScheduleFetchTimeout+5*time.Second)
	s, err := f.Fetch(fctx)
	cancel()
	if ctx.Err() != nil {
		return
	}
	now := time.Now()

	var cacheErr *schedule.CacheError
	if errors.As(err, &cacheErr) {
		m.log.Warn("schedule fetched but cache could not be written", "error", cacheErr.Err)
		err = nil
	}

	m.mu.Lock()
	m.info.LastAttemptAt = &now
	m.met.ScheduleLastAttempt.Set(float64(now.Unix()))
	switch {
	case err == nil:
		m.info.LastSuccessAt = &now
		m.info.LastError = ""
		m.info.ConsecutiveFailures = 0
		m.met.ScheduleFetchTotal.WithLabelValues("success").Inc()
		m.met.ScheduleLastSuccess.Set(float64(now.Unix()))
		m.met.ScheduleConsecutiveFailures.Set(0)
		if len(s.Items) == 0 && len(m.active) > 0 {
			m.log.Warn("schedule is empty; all active recordings will be stopped as removed", "active", len(m.active))
		}
		m.applyScheduleLocked(s)
		m.mu.Unlock()
		m.log.Info("schedule loaded", "items", len(s.Items), "invalid", len(s.Invalid), "etag", s.ETag)
		m.Reconcile(now)
		return

	case errors.Is(err, schedule.ErrNotModified):
		m.info.LastSuccessAt = &now
		m.info.LastError = ""
		m.info.ConsecutiveFailures = 0
		m.met.ScheduleFetchTotal.WithLabelValues("not_modified").Inc()
		m.met.ScheduleLastSuccess.Set(float64(now.Unix()))
		m.met.ScheduleConsecutiveFailures.Set(0)
		m.mu.Unlock()
		m.log.Debug("schedule not modified")
		return

	default:
		m.info.LastError = redactLine(err.Error())
		m.info.LastErrorAt = &now
		m.info.ConsecutiveFailures++
		failures := m.info.ConsecutiveFailures
		loaded := m.sched != nil
		m.met.ScheduleFetchTotal.WithLabelValues("failure").Inc()
		m.met.ScheduleConsecutiveFailures.Set(float64(failures))
		m.mu.Unlock()
		m.log.Warn("schedule fetch failed, keeping last known schedule", "error", redactLine(err.Error()),
			"consecutiveFailures", failures, "haveSchedule", loaded)
		if loaded {
			return
		}
		if initial {
			m.log.Warn("no schedule available yet; unfinished recordings from a previous run are resumed from their own metadata")
		}
	}
}
