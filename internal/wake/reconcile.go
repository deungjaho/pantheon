// Package wake — reconcile.go implements the continuation reconcile tick
// (ADR-0017). This is the idempotent daemon-side detection of completed/blocked
// runs that require an explicit successor. It sends deduplicated wake
// notifications to the PM message queue.
//
// The reconcile tick is NOT an LLM scheduler. It does not auto-launch workers.
// It only detects pending continuations and notifies the PM. The PM must
// explicitly create the successor run.
package wake

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/tangtszho/pantheon/internal/domain"
)

// ContinuationStore is the subset of store.Store required by the reconcile tick.
// Defined at the consumer side per AGENTS.md convention.
type ContinuationStore interface {
	ListPendingContinuations(ctx context.Context) ([]*domain.Continuation, error)
	MarkContinuationNotified(ctx context.Context, continuationID, eventID string) error
	UpdateWakeSentAt(ctx context.Context, continuationID string, now time.Time) error
	PublishMessageEnvelope(ctx context.Context, msg *domain.Message) (seq, messageSeq int64, messageID string, err error)
}

// OrphanedRunStore is the subset of store.Store required for orphaned-run
// detection. Defined at the consumer side per AGENTS.md convention. The store
// returns domain.OrphanedRun records; the PID liveness check is done in Go
// (os.FindProcess + signal 0), not in SQL.
type OrphanedRunStore interface {
	ListOrphanedRuns(ctx context.Context) ([]domain.OrphanedRun, error)
	PublishMessageEnvelope(ctx context.Context, msg *domain.Message) (seq, messageSeq int64, messageID string, err error)
}

// Reconciler is the idempotent continuation reconcile tick (ADR-0017).
// It detects pending continuations and sends deduplicated wake notifications.
// It also detects orphaned runs (transient state, all agents dead) and surfaces
// them as attention-required without auto-creating a successor.
type Reconciler struct {
	store   ContinuationStore
	orphans OrphanedRunStore // optional; nil disables orphaned-run detection
	logger  *log.Logger
	wakeGap time.Duration // minimum time between re-notifications for the same continuation
}

// NewReconciler creates a reconciler backed by the given store.
// wakeGap is the minimum time between re-notifications (default 1h).
// The orphanStore enables orphaned-run detection; pass nil to disable it
// (e.g. in unit tests that only exercise continuation notifications).
func NewReconciler(store ContinuationStore, orphanStore OrphanedRunStore, logger *log.Logger, wakeGap time.Duration) *Reconciler {
	if logger == nil {
		logger = log.Default()
	}
	if wakeGap <= 0 {
		wakeGap = time.Hour
	}
	return &Reconciler{
		store:   store,
		orphans: orphanStore,
		logger:  logger,
		wakeGap: wakeGap,
	}
}

// ReconcileResult records what the tick did.
type ReconcileResult struct {
	Checked       int                  // total pending/notified continuations examined
	Notified      int                  // new notifications sent (pending → notified)
	ReNotified    int                  // re-notifications sent (notified, wake_sent_at stale)
	Skipped       int                  // skipped (within wake_gap, dedup)
	Errors        int                  // errors encountered
	OrphanedRuns  []domain.OrphanedRun // runs surfaced as attention-required
	Notifications []string             // continuation IDs that were notified or re-notified
}

// Tick performs one reconcile pass. It is idempotent: running it multiple times
// within the wake_gap produces no duplicate notifications. Running it after a
// daemon restart resumes correctly because all state is in SQLite.
func (r *Reconciler) Tick(ctx context.Context) (*ReconcileResult, error) {
	result := &ReconcileResult{}

	continuations, err := r.store.ListPendingContinuations(ctx)
	if err != nil {
		return nil, fmt.Errorf("reconcile: list pending: %w", err)
	}
	result.Checked = len(continuations)

	now := time.Now().UTC()

	for _, c := range continuations {
		shouldNotify := false
		reason := ""
		wasPending := false

		if c.State == domain.ContinuationPending {
			// pending → always notify (first notification)
			shouldNotify = true
			reason = "first"
			wasPending = true
		} else if c.State == domain.ContinuationNotified {
			// notified → re-notify only if wake_sent_at is older than wake_gap
			if c.WakeSentAt == nil {
				shouldNotify = true
				reason = "notified_but_no_wake_sent_at"
			} else if now.Sub(*c.WakeSentAt) >= r.wakeGap {
				shouldNotify = true
				reason = "stale"
			} else {
				result.Skipped++
				continue
			}
		}

		if !shouldNotify {
			continue
		}

		if err := r.notify(ctx, c, reason); err != nil {
			result.Errors++
			r.logger.Printf("reconcile: notify %s: %v", c.ContinuationID, err)
			continue
		}

		result.Notifications = append(result.Notifications, c.ContinuationID)
		// Use wasPending (captured before notify mutated the state) rather
		// than re-checking c.State, which notify may have transitioned.
		if wasPending {
			result.Notified++
		} else {
			result.ReNotified++
		}
	}

	// Detect orphaned runs (transient state, all agents dead). This surfaces
	// them as attention-required WITHOUT auto-creating a successor or
	// auto-marking the run as failed — the PM must act explicitly.
	if r.orphans != nil {
		orphaned, err := r.orphans.ListOrphanedRuns(ctx)
		if err != nil {
			r.logger.Printf("reconcile: list orphaned runs: %v", err)
			result.Errors++
		} else {
			for _, or := range orphaned {
				if err := r.notifyOrphaned(ctx, &or); err != nil {
					result.Errors++
					r.logger.Printf("reconcile: notify orphaned run %s: %v", or.RunID, err)
					continue
				}
				result.OrphanedRuns = append(result.OrphanedRuns, or)
			}
		}
	}

	return result, nil
}

// notifyOrphaned publishes a wake.continuation message for an orphaned run.
// It does NOT auto-create a successor, auto-fulfill a continuation, or
// auto-mark the run as failed. It only notifies the PM that attention is
// required.
func (r *Reconciler) notifyOrphaned(ctx context.Context, or *domain.OrphanedRun) error {
	body := fmt.Sprintf("WAKE: orphaned run %s state=%s has no live agent. PM attention required.", or.RunID, or.State)
	to := or.Owner
	if to == "" {
		to = "portfolio-pm"
	}
	msgID, err := domain.NewID("msg_")
	if err != nil {
		return fmt.Errorf("generate message_id: %w", err)
	}
	msg := &domain.Message{
		MessageID:      msgID,
		RunID:          or.RunID,
		Sender:         domain.MessageEndpoint{Role: domain.RoleMetis, Instance: "reconciler"},
		Recipient:      domain.MessageEndpoint{Role: domain.RolePM, Instance: to},
		Type:           domain.MsgDirective,
		IdempotencyKey: fmt.Sprintf("wake_orphan_%s_%d", or.RunID, time.Now().UnixNano()),
		PayloadRef:     domain.PayloadRef{Kind: domain.PayloadKindInline, Inline: body},
		CreatedAt:      time.Now().UTC(),
	}
	if _, _, _, err := r.orphans.PublishMessageEnvelope(ctx, msg); err != nil {
		return fmt.Errorf("publish orphan wake message: %w", err)
	}
	r.logger.Printf("reconcile: surfaced orphaned run %s (state=%s agent=%s pid=%d)",
		or.RunID, or.State, or.AgentID, or.AgentPID)
	return nil
}

// notify sends a wake notification to the PM message queue and updates the
// continuation state. For pending continuations, it transitions to notified
// (which sets wake_sent_at and increments wake_count). For already-notified
// continuations, it only updates wake_sent_at (re-notification).
func (r *Reconciler) notify(ctx context.Context, c *domain.Continuation, reason string) error {
	// Build the wake notification message body.
	body := fmt.Sprintf("WAKE: continuation %s for run %s requires successor. Objective: %s. Reason: %s.",
		c.ContinuationID, c.RunID, c.SuccessorObjective, reason)

	to := c.Owner
	if to == "" {
		to = "portfolio-pm"
	}

	// Publish the wake notification to the PM message queue as a typed envelope.
	msgID, err := domain.NewID("msg_")
	if err != nil {
		return fmt.Errorf("generate message_id: %w", err)
	}
	msg := &domain.Message{
		MessageID:      msgID,
		RunID:          c.RunID,
		Sender:         domain.MessageEndpoint{Role: domain.RoleMetis, Instance: "reconciler"},
		Recipient:      domain.MessageEndpoint{Role: domain.RolePM, Instance: to},
		Type:           domain.MsgDirective,
		IdempotencyKey: fmt.Sprintf("wake_%s_%d", c.ContinuationID, time.Now().UnixNano()),
		PayloadRef:     domain.PayloadRef{Kind: domain.PayloadKindInline, Inline: body},
		CreatedAt:      time.Now().UTC(),
	}
	if _, _, _, err := r.store.PublishMessageEnvelope(ctx, msg); err != nil {
		return fmt.Errorf("publish wake message: %w", err)
	}

	// Update continuation state.
	if c.State == domain.ContinuationPending {
		// pending → notified (first notification, increments wake_count)
		notifyEventID := fmt.Sprintf("evt_cont_notified_%s_%d", c.ContinuationID, time.Now().UnixNano())
		if err := r.store.MarkContinuationNotified(ctx, c.ContinuationID, notifyEventID); err != nil {
			return fmt.Errorf("mark notified: %w", err)
		}
	} else {
		// already notified → just update wake_sent_at (re-notification)
		if err := r.store.UpdateWakeSentAt(ctx, c.ContinuationID, time.Now().UTC()); err != nil {
			return fmt.Errorf("update wake_sent_at: %w", err)
		}
	}

	r.logger.Printf("reconcile: notified continuation %s (run=%s reason=%s wake_count will increment)",
		c.ContinuationID, c.RunID, reason)
	return nil
}
