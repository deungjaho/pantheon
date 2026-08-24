package store

import (
	"context"
	"testing"
	"time"

	"github.com/tangtszho/pantheon/internal/domain"
)

func TestCreateFinding(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	f := &domain.Finding{
		FindingID:      mustNewID(t, "fnd_"),
		Type:           domain.FindingRiskFinding,
		Severity:       domain.FindingSeverityWarning,
		Title:          "Repeated run failures detected",
		Description:    "3 runs have failed.",
		RunIDs:         []string{"run_a", "run_b"},
		Evidence:       []string{"evt_1", "evt_2"},
		ProposedAction: "Review failed run root causes.",
		Status:         domain.FindingPending,
		CreatedAt:      time.Now().UTC(),
	}
	if err := s.CreateFinding(ctx, f); err != nil {
		t.Fatalf("CreateFinding: %v", err)
	}

	got, err := s.GetFinding(ctx, f.FindingID)
	if err != nil {
		t.Fatalf("GetFinding: %v", err)
	}
	if got == nil {
		t.Fatal("GetFinding returned nil")
	}
	if got.FindingID != f.FindingID {
		t.Fatalf("finding_id = %q, want %q", got.FindingID, f.FindingID)
	}
	if got.Type != f.Type {
		t.Fatalf("type = %q, want %q", got.Type, f.Type)
	}
	if got.Severity != f.Severity {
		t.Fatalf("severity = %q, want %q", got.Severity, f.Severity)
	}
	if got.Title != f.Title {
		t.Fatalf("title = %q, want %q", got.Title, f.Title)
	}
	if got.Description != f.Description {
		t.Fatalf("description = %q, want %q", got.Description, f.Description)
	}
	if got.ProposedAction != f.ProposedAction {
		t.Fatalf("proposed_action = %q, want %q", got.ProposedAction, f.ProposedAction)
	}
	if got.Status != domain.FindingPending {
		t.Fatalf("status = %q, want pending", got.Status)
	}
	if len(got.RunIDs) != 2 || got.RunIDs[0] != "run_a" || got.RunIDs[1] != "run_b" {
		t.Fatalf("run_ids = %v, want [run_a run_b]", got.RunIDs)
	}
	if len(got.Evidence) != 2 {
		t.Fatalf("evidence = %v, want 2 entries", got.Evidence)
	}
	if got.CreatedAt.IsZero() {
		t.Fatal("created_at is zero")
	}
	if got.ReviewedAt != nil {
		t.Fatalf("reviewed_at = %v, want nil", got.ReviewedAt)
	}
	if got.ReviewedBy != "" {
		t.Fatalf("reviewed_by = %q, want empty", got.ReviewedBy)
	}
}

func TestCreateFinding_DuplicateRejected(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	f := &domain.Finding{
		FindingID:   mustNewID(t, "fnd_"),
		Type:        domain.FindingRecommendation,
		Severity:    domain.FindingSeverityInfo,
		Title:       "dup",
		Description: "d",
		Status:      domain.FindingPending,
		CreatedAt:   time.Now().UTC(),
	}
	if err := s.CreateFinding(ctx, f); err != nil {
		t.Fatalf("CreateFinding 1: %v", err)
	}
	err := s.CreateFinding(ctx, f)
	if err == nil {
		t.Fatal("duplicate CreateFinding should fail")
	}
	de := domain.AsError(err)
	if de.Code != domain.CodeConflict {
		t.Fatalf("code = %q, want CONFLICT", de.Code)
	}
}

func TestCreateFinding_DefaultsPending(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	f := &domain.Finding{
		FindingID:   mustNewID(t, "fnd_"),
		Type:        domain.FindingRecommendation,
		Severity:    domain.FindingSeverityInfo,
		Title:       "no status",
		Description: "d",
		CreatedAt:   time.Now().UTC(),
	}
	if err := s.CreateFinding(ctx, f); err != nil {
		t.Fatalf("CreateFinding: %v", err)
	}
	got, err := s.GetFinding(ctx, f.FindingID)
	if err != nil {
		t.Fatalf("GetFinding: %v", err)
	}
	if got.Status != domain.FindingPending {
		t.Fatalf("status = %q, want pending (default)", got.Status)
	}
}

func TestGetFinding_NotFound(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	got, err := s.GetFinding(ctx, "fnd_nonexistent")
	if err != nil {
		t.Fatalf("GetFinding: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil, got %+v", got)
	}
}

func TestListFindingsByStatus(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	// Create three findings: two pending, one accepted.
	findings := []*domain.Finding{
		{FindingID: mustNewID(t, "fnd_"), Type: domain.FindingRiskFinding, Severity: domain.FindingSeverityWarning, Title: "p1", Description: "d", Status: domain.FindingPending, CreatedAt: time.Now().UTC()},
		{FindingID: mustNewID(t, "fnd_"), Type: domain.FindingRecommendation, Severity: domain.FindingSeverityInfo, Title: "p2", Description: "d", Status: domain.FindingPending, CreatedAt: time.Now().UTC()},
		{FindingID: mustNewID(t, "fnd_"), Type: domain.FindingPolicyProposal, Severity: domain.FindingSeverityCritical, Title: "a1", Description: "d", Status: domain.FindingAccepted, CreatedAt: time.Now().UTC()},
	}
	for _, f := range findings {
		if err := s.CreateFinding(ctx, f); err != nil {
			t.Fatalf("CreateFinding: %v", err)
		}
	}

	// Filter by pending.
	pending, err := s.ListFindings(ctx, string(domain.FindingPending), 100)
	if err != nil {
		t.Fatalf("ListFindings pending: %v", err)
	}
	if len(pending) != 2 {
		t.Fatalf("pending count = %d, want 2", len(pending))
	}
	for _, f := range pending {
		if f.Status != domain.FindingPending {
			t.Fatalf("status = %q, want pending", f.Status)
		}
	}

	// Filter by accepted.
	accepted, err := s.ListFindings(ctx, string(domain.FindingAccepted), 100)
	if err != nil {
		t.Fatalf("ListFindings accepted: %v", err)
	}
	if len(accepted) != 1 {
		t.Fatalf("accepted count = %d, want 1", len(accepted))
	}
	if accepted[0].Title != "a1" {
		t.Fatalf("title = %q, want a1", accepted[0].Title)
	}

	// No filter returns all.
	all, err := s.ListFindings(ctx, "", 100)
	if err != nil {
		t.Fatalf("ListFindings all: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("all count = %d, want 3", len(all))
	}
}

func TestListFindings_Limit(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		f := &domain.Finding{
			FindingID:   mustNewID(t, "fnd_"),
			Type:        domain.FindingRecommendation,
			Severity:    domain.FindingSeverityInfo,
			Title:       "limit test",
			Description: "d",
			Status:      domain.FindingPending,
			CreatedAt:   time.Now().UTC(),
		}
		if err := s.CreateFinding(ctx, f); err != nil {
			t.Fatalf("CreateFinding: %v", err)
		}
	}
	got, err := s.ListFindings(ctx, "", 2)
	if err != nil {
		t.Fatalf("ListFindings: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("count = %d, want 2 (limit)", len(got))
	}
}

func TestReviewFinding(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	f := &domain.Finding{
		FindingID:   mustNewID(t, "fnd_"),
		Type:        domain.FindingRiskFinding,
		Severity:    domain.FindingSeverityWarning,
		Title:       "review me",
		Description: "d",
		Status:      domain.FindingPending,
		CreatedAt:   time.Now().UTC(),
	}
	if err := s.CreateFinding(ctx, f); err != nil {
		t.Fatalf("CreateFinding: %v", err)
	}

	// Accept the finding.
	if err := s.ReviewFinding(ctx, f.FindingID, string(domain.FindingAccepted), "pm-alice"); err != nil {
		t.Fatalf("ReviewFinding accept: %v", err)
	}
	got, err := s.GetFinding(ctx, f.FindingID)
	if err != nil {
		t.Fatalf("GetFinding: %v", err)
	}
	if got.Status != domain.FindingAccepted {
		t.Fatalf("status = %q, want accepted", got.Status)
	}
	if got.ReviewedBy != "pm-alice" {
		t.Fatalf("reviewed_by = %q, want pm-alice", got.ReviewedBy)
	}
	if got.ReviewedAt == nil || got.ReviewedAt.IsZero() {
		t.Fatal("reviewed_at should be set after accept")
	}

	// Reject another finding.
	f2 := &domain.Finding{
		FindingID:   mustNewID(t, "fnd_"),
		Type:        domain.FindingRecommendation,
		Severity:    domain.FindingSeverityInfo,
		Title:       "reject me",
		Description: "d",
		Status:      domain.FindingPending,
		CreatedAt:   time.Now().UTC(),
	}
	if err := s.CreateFinding(ctx, f2); err != nil {
		t.Fatalf("CreateFinding f2: %v", err)
	}
	if err := s.ReviewFinding(ctx, f2.FindingID, string(domain.FindingRejected), "pm-bob"); err != nil {
		t.Fatalf("ReviewFinding reject: %v", err)
	}
	got2, err := s.GetFinding(ctx, f2.FindingID)
	if err != nil {
		t.Fatalf("GetFinding f2: %v", err)
	}
	if got2.Status != domain.FindingRejected {
		t.Fatalf("status = %q, want rejected", got2.Status)
	}
	if got2.ReviewedBy != "pm-bob" {
		t.Fatalf("reviewed_by = %q, want pm-bob", got2.ReviewedBy)
	}
}

func TestReviewFinding_InvalidStatus(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	f := &domain.Finding{
		FindingID:   mustNewID(t, "fnd_"),
		Type:        domain.FindingRiskFinding,
		Severity:    domain.FindingSeverityWarning,
		Title:       "bad status",
		Description: "d",
		Status:      domain.FindingPending,
		CreatedAt:   time.Now().UTC(),
	}
	if err := s.CreateFinding(ctx, f); err != nil {
		t.Fatalf("CreateFinding: %v", err)
	}
	err := s.ReviewFinding(ctx, f.FindingID, "pending", "x")
	if err == nil {
		t.Fatal("ReviewFinding with pending should fail")
	}
	de := domain.AsError(err)
	if de.Code != domain.CodeInvalidInput {
		t.Fatalf("code = %q, want INVALID_INPUT", de.Code)
	}
}

func TestReviewFinding_NotFound(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	err := s.ReviewFinding(ctx, "fnd_nonexistent", string(domain.FindingAccepted), "x")
	if err == nil {
		t.Fatal("ReviewFinding on nonexistent should fail")
	}
	de := domain.AsError(err)
	if de.Code != domain.CodeNotFound {
		t.Fatalf("code = %q, want NOT_FOUND", de.Code)
	}
}

func TestMigrateFindings_SchemaVersion(t *testing.T) {
	s := newTestStore(t)
	var v string
	if err := s.db.QueryRow(`SELECT value FROM meta WHERE key = 'schema_version'`).Scan(&v); err != nil {
		t.Fatalf("read schema_version: %v", err)
	}
	if v != "12" {
		t.Fatalf("schema_version = %q, want 12", v)
	}
	// The findings table must exist (empty).
	got, err := s.ListFindings(context.Background(), "", 100)
	if err != nil {
		t.Fatalf("ListFindings on fresh migration: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected 0 findings, got %d", len(got))
	}
}
