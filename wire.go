package realtime

// Wire protocol frame types for the Foony Realtime WebSocket service. These are the
// in-memory frame shapes; on the wire every frame travels in the binary opcode format
// (binary.go).
//
// Mirrors services/realtime-saas/internal/wire/wire.go exactly. Any change here MUST be
// mirrored on the server side and vice versa. The server file is the canonical source.
// Two tables have drifted before, so watch them: the frame opcode list and the error
// code table. Both must stay one-for-one (the realtime-js SDK mirrors the same tables).

import (
	"encoding/json"
	"fmt"
	"regexp"
)

// Error codes the server uses on error frames, mirrored one-for-one from the server's
// wire package (the canonical source). Compare against [ServerError.Code].
const (
	// CodeBadFrame means a malformed or unparseable frame.
	CodeBadFrame = 40001
	// CodeBadAuth means authentication failed (bad token or key).
	CodeBadAuth = 40101
	// CodeAuthExpired means previously valid auth has expired. Re-authenticate.
	CodeAuthExpired = 40102
	// CodeForbidden means authenticated but not permitted for this channel or action.
	CodeForbidden = 40300
	// CodeCapability means the token's capability does not grant the requested action.
	CodeCapability = 40301
	// CodeChannelDenied means the token's capability does not grant access to this
	// specific channel.
	CodeChannelDenied = 40302
	// CodeNotFound means a referenced resource (e.g. a channel) does not exist.
	CodeNotFound = 40400
	// CodeRateLimited means too many requests. The publish or connection rate limit was
	// exceeded.
	CodeRateLimited = 42900
	// CodeServer means an unexpected server-side error.
	CodeServer = 50000
	// CodeBootstrap means the edge could not bootstrap its streams. Retry later.
	CodeBootstrap = 50001
)

// ServerError is a protocol or authorization error the service returned for a request.
// Check the machine-readable [ServerError.Code] against the Code* constants with
// errors.As:
//
//	var serverErr *realtime.ServerError
//	if errors.As(err, &serverErr) && serverErr.Code == realtime.CodeCapability {
//		// the token does not grant this action
//	}
type ServerError struct {
	// Code is the machine-readable error code; see the Code* constants.
	Code int
	// Message is a human-readable error description for logging and debugging.
	Message string
}

// Error formats the error as "server error <code>: <message>", matching the realtime-js
// SDK's error strings.
func (e *ServerError) Error() string {
	return fmt.Sprintf("server error %d: %s", e.Code, e.Message)
}

// PresenceAction is a presence transition: a member entered, left, or updated its data.
type PresenceAction string

// The recognized presence transition values.
const (
	// PresenceEnter means a member announced itself present on the channel.
	PresenceEnter PresenceAction = "enter"
	// PresenceLeave means a member's presence entry was removed.
	PresenceLeave PresenceAction = "leave"
	// PresenceUpdate means a member replaced the data on its presence entry.
	PresenceUpdate PresenceAction = "update"
)

// maxChannelNameLength is the server's cap on channel name length.
const maxChannelNameLength = 255

// channelNamePattern is the server's channel grammar: A-Z a-z 0-9 : - _ only.
var channelNamePattern = regexp.MustCompile(`^[A-Za-z0-9:_-]+$`)

// validChannelName reports whether name satisfies the server's channel grammar: 1 to 255
// characters from A-Z a-z 0-9 : - _, not starting with ':'. Mirrors the server's
// ValidateChannelName (the canonical source).
func validChannelName(name string) bool {
	if len(name) == 0 || len(name) > maxChannelNameLength || name[0] == ':' {
		return false
	}
	return channelNamePattern.MatchString(name)
}

// ---- internal frame shapes ----
//
// Client-originated frames carry a numeric id; the server echoes it back on the matching
// ack / err frame so the SDK can correlate requests to responses.

// authFrame is the first frame after the WebSocket handshake. Carries either a JWT token
// or an API key, plus the previous connection id on a reconnect so presence membership
// survives a brief drop with no leave/enter churn.
type authFrame struct {
	token              string
	key                string
	clientID           string
	resumeConnectionID string
}

// subscribeFrame asks the server to start delivering messages + presence for a channel.
// lastSerial > 0 is the resume cursor: replay messages with serial > it before going
// live.
type subscribeFrame struct {
	channel    string
	id         uint64
	lastSerial uint64
}

// unsubscribeFrame asks the server to stop delivering messages + presence for a channel.
type unsubscribeFrame struct {
	channel string
	id      uint64
}

// wireMember is one member of a batch on the wire: its own name/data/encoding under the
// batch's shared id.
type wireMember struct {
	name     string
	data     json.RawMessage
	encoding string
}

// publishFrame publishes to a channel. Single by default (name + data); when members is
// set this is a batch publish (many messages under one messageID, stored and deduped by
// the server as one durable message) and name/data are ignored.
type publishFrame struct {
	channel string
	name    string
	data    json.RawMessage
	// members, when non-empty, makes this a batch publish.
	members []wireMember
	// ephemeral marks a fire-and-forget publish: delivered live but excluded from
	// history and connection-resume.
	ephemeral bool
	// messageID is the client-assigned id, stable across resends, that the server uses
	// as its dedup key so a publish resent after a reconnect collapses to one message.
	messageID string
	encoding  string
	id        uint64
}

// presenceFrame mutates the publisher's presence membership in a channel.
type presenceFrame struct {
	channel  string
	action   PresenceAction
	data     json.RawMessage
	encoding string
	id       uint64
}

// presenceSubscribeFrame asks for presence events on a channel (an initial snapshot of
// current members, then live transitions). Independent of a message subscribe.
type presenceSubscribeFrame struct {
	channel string
	id      uint64
}

// presenceUnsubscribeFrame stops presence events for a channel. Does not remove this
// connection's own membership.
type presenceUnsubscribeFrame struct {
	channel string
	id      uint64
}

// historyFrame requests recent messages for a channel. before > 0 is an exclusive serial
// cursor: return only messages with serial strictly below it (backward paging).
type historyFrame struct {
	channel string
	limit   uint64
	before  uint64
	id      uint64
}

// fetchFrame is a surgical forward gap-fill: return the messages with serial >
// fromSerial (oldest-first) without touching the live subscription. Sent when the SDK
// detects a serial gap mid-stream.
type fetchFrame struct {
	channel    string
	fromSerial uint64
	id         uint64
}

// pingFrame is an application-level liveness probe. The server replies with pong.
type pingFrame struct{}

// connectedFrame is sent once after a successful auth handshake.
type connectedFrame struct {
	connectionID string
	keepAliveMs  uint64
	clientID     string
}

// ackFrame acknowledges a client request that does not need a structured reply. resumed
// is the resume outcome for a subscribe that carried lastSerial (nil for non-resume
// requests); seq is the serial the server assigned to an acked publish (0 for
// ephemeral/unsequenced publishes).
type ackFrame struct {
	id      uint64
	resumed *bool
	seq     uint64
}

// msgFrame is a server-originated channel message: a single message, a batch (members
// set), or a server-coalesced bundle (bundle set, each member a full message that may
// itself be a batch).
type msgFrame struct {
	channel   string
	name      string
	data      json.RawMessage
	timestamp uint64
	messageID string
	clientID  string
	encoding  string
	// seq is the contiguous per-channel serial (0 for ephemeral/unsequenced messages).
	// The SDK uses it to detect gaps, as the resume cursor, and as history's before
	// cursor. For a bundle the outer seq is 0 and each member carries its own.
	seq       uint64
	ephemeral bool
	members   []wireMember
	bundle    []bundledMessage
}

// bundledMessage is one member of a server-coalesced bundle: a delivered message minus
// the redundant channel (taken from the carrying frame), with members set when the
// member was itself a client batch.
type bundledMessage struct {
	name      string
	data      json.RawMessage
	timestamp uint64
	messageID string
	clientID  string
	encoding  string
	seq       uint64
	ephemeral bool
	members   []wireMember
}

// presenceEventFrame is a server-originated presence transition.
type presenceEventFrame struct {
	channel      string
	action       PresenceAction
	clientID     string
	connectionID string
	data         json.RawMessage
	encoding     string
	timestamp    uint64
}

// errorFrame is a protocol or authorization error, request-scoped when id is non-zero.
type errorFrame struct {
	id      uint64
	code    int
	message string
}

// pongFrame is the response to ping.
type pongFrame struct{}

// historyResponseFrame answers a historyFrame: matching messages oldest-first, with more
// true when older messages remain beyond this page.
type historyResponseFrame struct {
	id       uint64
	channel  string
	messages []msgFrame
	more     bool
}

// fetchResponseFrame answers a fetchFrame: the missed messages oldest-first. resumed is
// false when the cursor had aged out of retention — a discontinuity the SDK surfaces and
// re-baselines from instead of re-applying.
type fetchResponseFrame struct {
	id       uint64
	channel  string
	messages []msgFrame
	resumed  bool
}
