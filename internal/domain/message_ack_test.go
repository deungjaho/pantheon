package domain

import (
	"testing"
	"time"
)

func TestAckStateValid(t *testing.T) {
	valid := []AckState{AckStatePending, AckStateAcked, AckStateNacked, AckStateExpired, AckStateDead}
	for _, s := range valid {
		if !s.Valid() {
			t.Errorf("AckState %q should be valid", s)
		}
	}
	if AckState("unknown").Valid() {
		t.Error("unknown ack state should be invalid")
	}
}

func TestBackoffDuration(t *testing.T) {
	cases := []struct {
		retry int
		want  time.Duration
	}{
		{0, 1 * time.Second},
		{1, 4 * time.Second},
		{2, 16 * time.Second},
		{3, 16 * time.Second}, // capped at MaxRetries-1=2
		{-1, 1 * time.Second}, // negative clamped to 0
	}
	for _, c := range cases {
		got := BackoffDuration(c.retry)
		if got != c.want {
			t.Errorf("BackoffDuration(%d) = %v, want %v", c.retry, got, c.want)
		}
	}
}

func TestMessageDeadlineExceeded(t *testing.T) {
	now := time.Now().UTC()

	t.Run("no_ttl", func(t *testing.T) {
		m := Message{CreatedAt: now, TTL: 0}
		if m.DeadlineExceeded(now.Add(1 * time.Hour)) {
			t.Error("TTL=0 should never exceed")
		}
	})

	t.Run("not_expired", func(t *testing.T) {
		m := Message{CreatedAt: now, TTL: 60}
		if m.DeadlineExceeded(now.Add(30 * time.Second)) {
			t.Error("30s < 60s TTL should not exceed")
		}
	})

	t.Run("expired", func(t *testing.T) {
		m := Message{CreatedAt: now, TTL: 10}
		if !m.DeadlineExceeded(now.Add(15 * time.Second)) {
			t.Error("15s > 10s TTL should exceed")
		}
	})
}

func TestMessageIsDead(t *testing.T) {
	m := Message{}
	if !m.IsDead(MaxRetries) {
		t.Errorf("IsDead(%d) should be true", MaxRetries)
	}
	if m.IsDead(MaxRetries - 1) {
		t.Errorf("IsDead(%d) should be false", MaxRetries-1)
	}
}
