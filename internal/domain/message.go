package domain

import (
	"encoding/json"
	"fmt"
	"time"
)

// MessageType is the enumerated type for the v1.1 message envelope.
type MessageType string

const (
	MsgDirective MessageType = "directive" // metis/pm → worker
	MsgReport    MessageType = "report"    // worker → pm/metis
	MsgState     MessageType = "state"     // worker/verifier → pm
	MsgBlock     MessageType = "block"     // worker → pm
	MsgComplete  MessageType = "complete"  // worker → pm (claim, not fact)
	MsgVerify    MessageType = "verify"    // pm → verifier
	MsgAck       MessageType = "ack"       // recipient → sender
	MsgNack      MessageType = "nack"      // recipient → sender
)

// Valid reports whether mt is a known message type.
func (mt MessageType) Valid() bool {
	switch mt {
	case MsgDirective, MsgReport, MsgState, MsgBlock, MsgComplete, MsgVerify, MsgAck, MsgNack:
		return true
	}
	return false
}

// AgentRole is already declared in types.go with RoleController, RoleWorker,
// and RoleVerifier. The v1.1 message envelope adds two communication roles
// that are not agent registration roles but message routing roles.
const (
	RoleMetis AgentRole = "metis" // Metis (portfolio master) as message endpoint
	RolePM    AgentRole = "pm"    // Project Master as message endpoint
)

// ValidMessageRole reports whether r is a valid role for a message endpoint.
// This extends the agent registration roles (controller/worker/verifier) with
// the communication roles (metis/pm).
func ValidMessageRole(r AgentRole) bool {
	switch r {
	case RoleController, RoleWorker, RoleVerifier, RoleMetis, RolePM:
		return true
	}
	return false
}

// Sensitivity classifies payload handling for projection and storage.
type Sensitivity string

const (
	SensNormal     Sensitivity = "normal"
	SensRestricted Sensitivity = "restricted"
)

// Valid reports whether s is a known sensitivity level.
func (s Sensitivity) Valid() bool {
	switch s {
	case SensNormal, SensRestricted:
		return true
	}
	return false
}

// MessageEndpoint identifies one end of a message (sender or recipient).
type MessageEndpoint struct {
	AgentID  string    `json:"agent_id,omitempty"`
	Role     AgentRole `json:"role"`
	Instance string    `json:"instance,omitempty"`
}

// PayloadRef describes how the message payload is stored. Inline payloads
// must be ≤ MaxInlinePayload bytes. Larger payloads use an artifact reference
// with a SHA-256 checksum.
type PayloadRef struct {
	Kind       string `json:"kind"`                  // "inline" or "artifact"
	Inline     string `json:"inline,omitempty"`      // present when Kind=="inline"
	ArtifactID string `json:"artifact_id,omitempty"` // present when Kind=="artifact"
	SHA256     string `json:"sha256,omitempty"`      // present when Kind=="artifact"
}

const (
	PayloadKindInline   = "inline"
	PayloadKindArtifact = "artifact"
)

// MaxInlinePayload is the maximum size of an inline payload in bytes.
// Payloads exceeding this must use an artifact reference.
const MaxInlinePayload = 4 * 1024 // 4 KB

// AckState tracks the delivery state of a message envelope (C-002).
type AckState string

const (
	AckStatePending AckState = "pending" // message published, not yet acked/nacked
	AckStateAcked   AckState = "acked"   // recipient confirmed processing
	AckStateNacked  AckState = "nacked"  // recipient rejected, eligible for retry
	AckStateExpired AckState = "expired" // TTL passed without ack
	AckStateDead    AckState = "dead"    // retry count exhausted
)

// Valid reports whether s is a known ack state.
func (s AckState) Valid() bool {
	switch s {
	case AckStatePending, AckStateAcked, AckStateNacked, AckStateExpired, AckStateDead:
		return true
	}
	return false
}

// C-002 retry/backoff constants.
const (
	MaxRetries         = 3 // max retry attempts before dead
	BackoffBaseSeconds = 1 // base for exponential backoff
)

// BackoffDuration returns the backoff duration for the given retry attempt
// (0-indexed). The sequence is 1s, 4s, 16s (exponential with base 4).
// Attempts beyond MaxRetries-1 return the last interval.
func BackoffDuration(retryCount int) time.Duration {
	if retryCount < 0 {
		retryCount = 0
	}
	if retryCount >= MaxRetries {
		retryCount = MaxRetries - 1
	}
	// 1s, 4s, 16s: 4^retryCount seconds
	multiplier := 1
	for i := 0; i < retryCount; i++ {
		multiplier *= 4
	}
	return time.Duration(multiplier) * time.Second
}

// DeadlineExceeded reports whether the message TTL has expired.
// Returns false if TTL is 0 (no expiry).
func (m *Message) DeadlineExceeded(now time.Time) bool {
	if m.TTL <= 0 {
		return false
	}
	return now.Sub(m.CreatedAt) > time.Duration(m.TTL)*time.Second
}

// IsDead reports whether the message has exhausted its retries.
func (m *Message) IsDead(retryCount int) bool {
	return retryCount >= MaxRetries
}

// Message is the v1.1 typed message envelope. It is stored as the JSON
// payload of an event with EventType="message" in the SQLite event journal.
//
// Fields align with COMMUNICATION_CONTRACT.md §2.1. The struct is
// backward-compatible: ParseMessage accepts both the new envelope and the
// legacy {topic, body, from, to} shape.
type Message struct {
	MessageID      string          `json:"message_id"`
	RunID          string          `json:"run_id"`
	TaskID         string          `json:"task_id,omitempty"`
	Sender         MessageEndpoint `json:"sender"`
	Recipient      MessageEndpoint `json:"recipient"`
	Type           MessageType     `json:"type"`
	Seq            int64           `json:"seq"` // per-Run, assigned by Store
	CreatedAt      time.Time       `json:"created_at"`
	TTL            int             `json:"ttl,omitempty"` // seconds; 0 = no expiry
	IdempotencyKey string          `json:"idempotency_key"`
	PayloadRef     PayloadRef      `json:"payload_ref"`
	Sensitivity    Sensitivity     `json:"sensitivity,omitempty"`
}

// Validate checks that the envelope has all required fields and that
// inline payloads are within bounds. It does NOT check idempotency or
// seq assignment — those are the Store's responsibility.
func (m *Message) Validate() error {
	if m.MessageID == "" {
		return fmt.Errorf("message_id is required")
	}
	if m.RunID == "" {
		return fmt.Errorf("run_id is required")
	}
	if !m.Type.Valid() {
		return fmt.Errorf("invalid message type %q", m.Type)
	}
	if !ValidMessageRole(m.Sender.Role) {
		return fmt.Errorf("invalid sender role %q", m.Sender.Role)
	}
	if !ValidMessageRole(m.Recipient.Role) {
		return fmt.Errorf("invalid recipient role %q", m.Recipient.Role)
	}
	if m.IdempotencyKey == "" {
		return fmt.Errorf("idempotency_key is required")
	}
	if m.PayloadRef.Kind == "" {
		return fmt.Errorf("payload_ref.kind is required")
	}
	switch m.PayloadRef.Kind {
	case PayloadKindInline:
		if len(m.PayloadRef.Inline) == 0 {
			return fmt.Errorf("inline payload is empty")
		}
		if len(m.PayloadRef.Inline) > MaxInlinePayload {
			return fmt.Errorf("inline payload %d bytes exceeds max %d", len(m.PayloadRef.Inline), MaxInlinePayload)
		}
	case PayloadKindArtifact:
		if m.PayloadRef.ArtifactID == "" {
			return fmt.Errorf("artifact payload requires artifact_id")
		}
		if m.PayloadRef.SHA256 == "" {
			return fmt.Errorf("artifact payload requires sha256")
		}
	default:
		return fmt.Errorf("invalid payload_ref.kind %q", m.PayloadRef.Kind)
	}
	if m.Sensitivity != "" && !m.Sensitivity.Valid() {
		return fmt.Errorf("invalid sensitivity %q", m.Sensitivity)
	}
	if m.CreatedAt.IsZero() {
		return fmt.Errorf("created_at is required")
	}
	return nil
}

// legacyPayload is the P0 shape stored in event payloads before v1.1.
type legacyPayload struct {
	Topic string `json:"topic"`
	Body  string `json:"body"`
	From  string `json:"from,omitempty"`
	To    string `json:"to,omitempty"`
}

// ParseMessage decodes a raw event payload (json.RawMessage) into a Message.
// It accepts both the v1.1 envelope and the legacy {topic, body, from, to}
// shape. For legacy payloads, the returned Message has:
//   - Type derived from the topic prefix (directive.* → directive, etc.)
//   - Sender/Recipient roles set from From/To strings (best-effort)
//   - PayloadRef.Kind = "inline", PayloadRef.Inline = Body
//   - MessageID, IdempotencyKey, Seq left zero (not present in legacy)
//
// This allows v1.1 readers to consume pre-v1.1 events without migration.
func ParseMessage(raw json.RawMessage) (Message, error) {
	// Try v1.1 envelope first.
	var msg Message
	if err := json.Unmarshal(raw, &msg); err == nil && msg.MessageID != "" {
		return msg, nil
	}

	// Fall back to legacy shape.
	var lp legacyPayload
	if err := json.Unmarshal(raw, &lp); err != nil {
		return Message{}, fmt.Errorf("parse message payload: not v1.1 envelope nor legacy: %w", err)
	}
	if lp.Topic == "" {
		return Message{}, fmt.Errorf("parse message payload: empty topic in legacy shape")
	}

	msg = Message{
		RunID:       "", // legacy messages were not Run-scoped
		Type:        inferTypeFromTopic(lp.Topic),
		PayloadRef:  PayloadRef{Kind: PayloadKindInline, Inline: lp.Body},
		Sensitivity: SensNormal,
	}
	msg.Sender = inferEndpoint(lp.From)
	msg.Recipient = inferEndpoint(lp.To)
	return msg, nil
}

// inferTypeFromTopic maps a legacy topic prefix to a MessageType.
// "directive.*" → directive, "report.*" → report, "verify.*" → verify,
// "state.*" → state, "block.*" → block, "complete.*" → complete,
// "ack.*" → ack, "nack.*" → nack. Unknown prefixes default to "report".
func inferTypeFromTopic(topic string) MessageType {
	for i := 0; i < len(topic); i++ {
		if topic[i] == '.' {
			prefix := topic[:i]
			switch MessageType(prefix) {
			case MsgDirective, MsgReport, MsgState, MsgBlock, MsgComplete, MsgVerify, MsgAck, MsgNack:
				return MessageType(prefix)
			}
			break
		}
	}
	// No dot or unknown prefix — default to report (the most common legacy type).
	return MsgReport
}

// inferEndpoint maps a legacy from/to string to a MessageEndpoint.
// If the string is empty, returns a zero-value endpoint.
// If the string matches a known role name, sets that role.
// Otherwise treats the string as an agent_id with unknown role.
func inferEndpoint(s string) MessageEndpoint {
	if s == "" {
		return MessageEndpoint{}
	}
	switch AgentRole(s) {
	case RoleMetis, RolePM, RoleController, RoleWorker, RoleVerifier:
		return MessageEndpoint{Role: AgentRole(s)}
	default:
		return MessageEndpoint{AgentID: s}
	}
}
