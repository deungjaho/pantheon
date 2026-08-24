package domain

import "time"

// FindingType is the category of a Global Auditor finding (Phase 4). Each
// type maps to a different downstream action:
//   - recommendation: a suggested improvement (process, config, scope).
//   - memory_candidate: a durable lesson worth persisting to Mnemos (when
//     available); stored locally until Mnemos integration lands.
//   - policy_proposal: a proposed change to a versioned policy/doc.
//   - risk_finding: a detected risk pattern that needs human attention.
//
// The auditor does NOT auto-modify anything — all findings start in the
// pending state and require human acceptance before they become versioned
// policies/docs (ROADMAP Phase 4: "人工接受后变成 versioned policy/doc",
// "不允许自修改").
type FindingType string

const (
	FindingRecommendation  FindingType = "recommendation"
	FindingMemoryCandidate FindingType = "memory_candidate"
	FindingPolicyProposal  FindingType = "policy_proposal"
	FindingRiskFinding     FindingType = "risk_finding"
)

// FindingSeverity grades a finding's urgency.
type FindingSeverity string

const (
	FindingSeverityInfo     FindingSeverity = "info"
	FindingSeverityWarning  FindingSeverity = "warning"
	FindingSeverityCritical FindingSeverity = "critical"
)

// FindingStatus is the review state of a finding. Findings start pending;
// a human accepts or rejects them. The auditor never auto-accepts.
type FindingStatus string

const (
	FindingPending  FindingStatus = "pending"
	FindingAccepted FindingStatus = "accepted"
	FindingRejected FindingStatus = "rejected"
)

// ValidFindingStatus reports whether s is a recognized FindingStatus.
func ValidFindingStatus(s FindingStatus) bool {
	switch s {
	case FindingPending, FindingAccepted, FindingRejected:
		return true
	}
	return false
}

// Finding is a structured auditor output (Phase 4). It records a detected
// pattern from run history, the evidence that triggered it, and a proposed
// action for a human to accept or reject. Findings are stored locally in
// SQLite; Mnemos integration (memory candidates) is deferred.
type Finding struct {
	FindingID      string          `json:"finding_id"`
	Type           FindingType     `json:"type"`
	Severity       FindingSeverity `json:"severity"`
	Title          string          `json:"title"`
	Description    string          `json:"description"`
	RunIDs         []string        `json:"run_ids,omitempty"`         // runs that triggered this finding
	Evidence       []string        `json:"evidence,omitempty"`        // event IDs or data references
	ProposedAction string          `json:"proposed_action,omitempty"` // what should be done
	Status         FindingStatus   `json:"status"`                    // pending, accepted, rejected
	CreatedAt      time.Time       `json:"created_at"`
	ReviewedAt     *time.Time      `json:"reviewed_at,omitempty"`
	ReviewedBy     string          `json:"reviewed_by,omitempty"`
}
