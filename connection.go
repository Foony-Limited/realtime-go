package realtime

// Low-level WebSocket connection manager. Handles framing, request/response
// correlation, and dispatch to per-channel hooks. Intentionally protocol-aware but
// channel-agnostic: Channel and Client layer the public API on top.

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
)

// DefaultEndpoint is the Foony Realtime endpoint used when Options.Endpoint is empty.
const DefaultEndpoint = "realtime.foony.io"

// Default reconnect backoff bounds.
const (
	defaultInitialReconnectDelay = time.Second
	defaultMaxReconnectDelay     = 30 * time.Second
)

// handshakeTimeout bounds the dial plus auth handshake of one connect attempt.
const handshakeTimeout = 30 * time.Second

// writeTimeout bounds one socket write. A link too congested to take a frame in this
// window is as good as dead, and the keep-alive deadline will tear it down anyway.
const writeTimeout = 30 * time.Second

// Close codes used when the SDK tears a socket down itself. The WebSocket protocol
// reserves most codes, so app-specific 4xxx codes signal a failed handshake and a
// keep-alive timeout.
const (
	closeCodeHandshakeFailed  websocket.StatusCode = 4001
	closeCodeKeepAliveTimeout websocket.StatusCode = 4002
)

// Bounds on how long the SDK waits after a ping for proof of life (any inbound frame)
// before declaring the link dead. The deadline follows the server's advertised ping
// cadence, clamped to these.
const (
	minPongDeadline = 250 * time.Millisecond
	maxPongDeadline = 10 * time.Second
)

// errConnectionClosed rejects requests and publishes that can never be answered because
// the connection was closed.
var errConnectionClosed = errors.New("realtime: connection closed")

// ConnectionState is a connection lifecycle state. Observe transitions with
// [Connection.On].
type ConnectionState string

// Connection lifecycle states.
const (
	// ConnectionInitialized means the connection was created locally and no connect has
	// been attempted yet.
	ConnectionInitialized ConnectionState = "initialized"
	// ConnectionConnecting means the WebSocket is opening and the auth handshake is in
	// flight. Publishes made now are queued unless Options.DisableQueueing is set.
	ConnectionConnecting ConnectionState = "connecting"
	// ConnectionConnected means connected and authenticated. Messages flow, and
	// [Connection.ID] and [Connection.ClientID] are populated.
	ConnectionConnected ConnectionState = "connected"
	// ConnectionDisconnected means the connection dropped unexpectedly. The state
	// change's Reason says why. Unless Options.DisableAutoReconnect is set, the SDK
	// retries with exponential backoff, starting at Options.InitialReconnectDelay
	// (1 second) and doubling up to Options.MaxReconnectDelay (30 seconds). You can
	// keep publishing: unless Options.DisableQueueing is set, publishes queue locally
	// and are sent on reconnect, and channels re-attach and replay the messages they
	// missed (within retention).
	ConnectionDisconnected ConnectionState = "disconnected"
	// ConnectionClosing means Close was called and the socket is shutting down.
	ConnectionClosing ConnectionState = "closing"
	// ConnectionClosed means the connection was closed by Close. Publishes that were
	// awaiting an ack have been rejected.
	ConnectionClosed ConnectionState = "closed"
	// ConnectionFailed means a failure the SDK will not retry on its own, for example a
	// bad or expired credential with no AuthCallback to re-mint one. The state change's
	// Reason carries the error. An explicit Connect starts a fresh attempt.
	ConnectionFailed ConnectionState = "failed"
)

// ConnectionStateChange is the payload delivered to connection state listeners.
type ConnectionStateChange struct {
	// Current is the state the connection is now in.
	Current ConnectionState
	// Reason is the error that caused the transition, when the event was error-driven.
	Reason error
}

// Options configures a [Client]. Exactly one of Key, Token, or AuthCallback must be
// set; [New] returns an error otherwise.
type Options struct {
	// Endpoint is the Realtime edge host or an absolute ws(s) URL. Defaults to
	// "realtime.foony.io", which resolves to wss://realtime.foony.io.
	Endpoint string
	// Key is a Realtime API key in "appSlug.publicKeyId:privateKey" form. The key is a
	// long-lived secret, so use it only in server-side code and trusted quick starts.
	// Never ship it in client-side code distributed to users: those should use
	// short-lived JWTs from AuthCallback.
	Key string
	// ClientID is the client id to attach when authenticating with Key. With token
	// auth the client id comes from the JWT's subject instead, and this option is not
	// sent.
	ClientID string
	// Token is a static JWT to send in the auth handshake. Mutually exclusive with
	// AuthCallback. Useful for local dev and short scripts. A static token is never
	// renewed: once it expires, the connection ends in the terminal
	// [ConnectionFailed] state, so use AuthCallback for anything long-running.
	Token string
	// AuthCallback returns a fresh JWT. Called once on connect and again on every
	// reconnect. This is the recommended auth method for anything long-running,
	// because the SDK can renew the token whenever it needs one. See the auth docs:
	// https://foony.io/docs/auth
	AuthCallback func(ctx context.Context) (string, error)
	// DisableAutoReconnect stops the SDK from reconnecting after unexpected
	// disconnects. If false (the default), the SDK reconnects with exponential
	// backoff. If true, a dropped connection stays down until you call Connect again.
	// An auth error that cannot be recovered (a bad or expired static Token, or a bad
	// Key, with no AuthCallback to re-mint a credential) still ends in the terminal
	// [ConnectionFailed] state rather than retrying.
	DisableAutoReconnect bool
	// InitialReconnectDelay is the initial backoff for reconnects. The delay doubles
	// each attempt up to MaxReconnectDelay. Defaults to 1 second when zero.
	InitialReconnectDelay time.Duration
	// MaxReconnectDelay caps the reconnect backoff. Defaults to 30 seconds when zero.
	MaxReconnectDelay time.Duration
	// DisableQueueing rejects publishes made while the connection is establishing or
	// temporarily down. If false (the default), those publishes are queued locally and
	// flushed on (re)connect. If true, publishing while not connected returns an error
	// immediately.
	DisableQueueing bool
	// Batch configures the always-on auto-batching applied to every channel,
	// overridable per channel with [WithBatchOptions]. Defaults are documented on
	// [BatchOptions].
	Batch *BatchOptions
}

// ackOutcome resolves one in-flight ack/err request.
type ackOutcome struct {
	ack *ackFrame
	err error
}

// historyOutcome resolves one in-flight history request.
type historyOutcome struct {
	response *historyResponseFrame
	err      error
}

// fetchOutcome resolves one in-flight fetch (gap-fill) request.
type fetchOutcome struct {
	response *fetchResponseFrame
	err      error
}

// outstandingPublish is a publish tracked until the server acks it. The stable
// messageID makes a resend after a reconnect dedupe server-side.
type outstandingPublish struct {
	frame *publishFrame
	done  chan error
	// requestID is the id of the current send attempt, or 0 when the publish is
	// buffered (not yet sent, or awaiting resend after a disconnect).
	requestID uint64
}

// channelHooks are the per-channel dispatch callbacks owned by Channel instances.
type channelHooks struct {
	message  func(*msgFrame)
	presence func(*presenceEventFrame)
	// lastSerial returns the channel's resume cursor (contiguous serial), or 0 when it
	// has none.
	lastSerial func() uint64
	// resumed reports the resume outcome once a reconnect re-subscribe has acked.
	resumed func(bool)
	// reenterPresence re-announces this channel's presence membership after a
	// reconnect (re-enter what was entered).
	reenterPresence func()
}

// connectAttempt is one in-flight connect shared by concurrent Connect callers.
type connectAttempt struct {
	done chan struct{}
	err  error
}

// Connection is the transport layer. One [Client] owns one Connection and all of its
// channels share it. Listen on lifecycle changes with [Connection.On], which delivers
// every [ConnectionState] transition.
type Connection struct {
	opts     Options
	dispatch *dispatcher
	events   *emitter[ConnectionState, ConnectionStateChange]

	mu           sync.Mutex
	state        ConnectionState
	socket       *websocket.Conn
	connectionID string
	clientID     string
	nextID       uint64
	pending      map[uint64]chan ackOutcome
	pendingHist  map[uint64]chan historyOutcome
	pendingFetch map[uint64]chan fetchOutcome
	hooks        map[string]*channelHooks
	connecting   *connectAttempt
	// reconnectTimer drives the next backoff attempt, nil when none is scheduled.
	reconnectTimer   *time.Timer
	reconnectAttempt int
	// hasConnectedBefore tells a reconnect from the first connect, so subscription
	// restore only runs on actual reconnects.
	hasConnectedBefore bool
	// fatalErr is set when a handshake fails with an auth error we cannot recover from
	// (a bad or expired credential with no AuthCallback to re-mint). The socket
	// teardown reads it to move to the terminal failed state instead of retrying a
	// credential that will be rejected identically forever.
	fatalErr error
	// keepAliveStop stops the current socket's keep-alive goroutine.
	keepAliveStop chan struct{}
	// pongDeadline is armed after each ping and disarmed by any inbound frame. When it
	// fires, the link is dead.
	pongDeadline    *time.Timer
	pongDeadlineDur time.Duration
	// desiredSubs are the channels the SDK has asked to be subscribed to. Re-sent on
	// reconnect.
	desiredSubs map[string]struct{}
	// subEpochs is a per-channel counter bumped on every rememberSubscription, so a
	// stale detach cannot forget a newer attach.
	subEpochs map[string]int
	// desiredPresence are the channels the SDK has asked for presence events on.
	// Re-sent on reconnect.
	desiredPresence map[string]struct{}
	// outstanding are publishes awaiting ack, keyed by client messageID. (Re)sent on
	// (re)connect.
	outstanding map[string]*outstandingPublish
	// publishRequestIDs maps a send attempt's request id back to its publish
	// messageID, to route ack/err frames.
	publishRequestIDs map[uint64]string

	// writeMu serializes socket writes (the websocket library allows one writer).
	writeMu sync.Mutex
}

func newConnection(opts Options) (*Connection, error) {
	authMethods := 0
	if opts.Token != "" {
		authMethods++
	}
	if opts.AuthCallback != nil {
		authMethods++
	}
	if opts.Key != "" {
		authMethods++
	}
	if authMethods != 1 {
		return nil, errors.New("realtime: pass exactly one of Options.Token, Options.AuthCallback, or Options.Key")
	}
	return &Connection{
		opts:              opts,
		dispatch:          &dispatcher{},
		events:            newEmitter[ConnectionState, ConnectionStateChange](),
		state:             ConnectionInitialized,
		pending:           make(map[uint64]chan ackOutcome),
		pendingHist:       make(map[uint64]chan historyOutcome),
		pendingFetch:      make(map[uint64]chan fetchOutcome),
		hooks:             make(map[string]*channelHooks),
		desiredSubs:       make(map[string]struct{}),
		subEpochs:         make(map[string]int),
		desiredPresence:   make(map[string]struct{}),
		outstanding:       make(map[string]*outstandingPublish),
		publishRequestIDs: make(map[uint64]string),
		pongDeadlineDur:   maxPongDeadline,
	}, nil
}

// State returns the current [ConnectionState]. Listen on changes with [Connection.On].
func (c *Connection) State() ConnectionState {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}

// ID returns the server-issued connection id, or "" before the auth handshake
// completes.
func (c *Connection) ID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connectionID
}

// ClientID returns the client id this connection is authenticated as, or "" before the
// auth handshake completes. Never "" once connected: the server resolves it from the
// JWT's subject (Token and AuthCallback auth), from Options.ClientID (key auth), or
// assigns one when neither names a client.
func (c *Connection) ClientID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.clientID
}

// On registers a listener for every connection state change and returns its unsubscribe
// function.
func (c *Connection) On(listener func(ConnectionStateChange)) func() {
	return c.events.on(listener)
}

// OnState registers a listener for changes into one state and returns its unsubscribe
// function.
func (c *Connection) OnState(state ConnectionState, listener func(ConnectionStateChange)) func() {
	return c.events.onEvent(state, listener, false)
}

// Once registers a listener invoked one time for the next change into state, and
// returns its unsubscribe function.
func (c *Connection) Once(state ConnectionState, listener func(ConnectionStateChange)) func() {
	return c.events.onEvent(state, listener, true)
}

// Off removes every connection state listener.
func (c *Connection) Off() {
	c.events.offAll()
}

// Connect opens the WebSocket and completes the auth handshake. Connect is idempotent,
// and concurrent calls wait on the same in-flight attempt (ctx cancels the caller's
// wait, not the shared attempt). It returns nil once the connection is connected, and
// the handshake error when auth fails, for example a bad key or an expired static
// Token with no AuthCallback to re-mint one.
func (c *Connection) Connect(ctx context.Context) error {
	c.mu.Lock()
	if c.state == ConnectionConnected {
		c.mu.Unlock()
		return nil
	}
	attempt := c.connecting
	if attempt == nil {
		attempt = &connectAttempt{done: make(chan struct{})}
		c.connecting = attempt
		go c.runConnect(attempt)
	}
	c.mu.Unlock()
	select {
	case <-attempt.done:
		return attempt.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close closes the WebSocket and releases resources. The connection is closed when it
// returns. Requests and publishes still awaiting an ack are rejected with a
// "connection closed" error.
func (c *Connection) Close() {
	c.mu.Lock()
	if c.reconnectTimer != nil {
		c.reconnectTimer.Stop()
		c.reconnectTimer = nil
	}
	c.stopKeepAliveLocked()
	c.setStateLocked(ConnectionClosing, nil)
	ws := c.socket
	c.mu.Unlock()
	if ws != nil {
		// Also abort a socket that is mid-handshake, or a Close racing an in-flight
		// Connect would leave the handshake to complete and resurrect the connection.
		_ = ws.Close(websocket.StatusNormalClosure, "client close")
	}
	c.mu.Lock()
	c.setStateLocked(ConnectionClosed, nil)
	c.failPendingLocked(errConnectionClosed)
	c.failOutstandingLocked(errConnectionClosed)
	c.mu.Unlock()
}

// ---- request/response ----

// request sends a frame that expects an ack. build receives the assigned request id and
// returns the frame. It returns the matching ack, or the server's error.
func (c *Connection) request(ctx context.Context, build func(id uint64) any) (*ackFrame, error) {
	if err := c.Connect(ctx); err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.nextID++
	id := c.nextID
	ch := make(chan ackOutcome, 1)
	c.pending[id] = ch
	ws := c.socket
	c.mu.Unlock()
	if err := c.send(ws, build(id)); err != nil {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return nil, err
	}
	select {
	case outcome := <-ch:
		return outcome.ack, outcome.err
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return nil, ctx.Err()
	}
}

// requestHistory sends a hist frame and returns the matching histRes, or the server's
// error. Unlike request, history is correlated to a dedicated response frame rather
// than a bare ack.
func (c *Connection) requestHistory(ctx context.Context, frame *historyFrame) (*historyResponseFrame, error) {
	if err := c.Connect(ctx); err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.nextID++
	frame.id = c.nextID
	ch := make(chan historyOutcome, 1)
	c.pendingHist[frame.id] = ch
	ws := c.socket
	c.mu.Unlock()
	if err := c.send(ws, frame); err != nil {
		c.mu.Lock()
		delete(c.pendingHist, frame.id)
		c.mu.Unlock()
		return nil, err
	}
	select {
	case outcome := <-ch:
		return outcome.response, outcome.err
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.pendingHist, frame.id)
		c.mu.Unlock()
		return nil, ctx.Err()
	}
}

// requestFetch is a surgical gap-fill: ask the server for the messages after fromSerial
// on a channel without disturbing its live subscription. Used by Channel to heal a
// detected serial gap.
func (c *Connection) requestFetch(ctx context.Context, frame *fetchFrame) (*fetchResponseFrame, error) {
	if err := c.Connect(ctx); err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.nextID++
	frame.id = c.nextID
	ch := make(chan fetchOutcome, 1)
	c.pendingFetch[frame.id] = ch
	ws := c.socket
	c.mu.Unlock()
	if err := c.send(ws, frame); err != nil {
		c.mu.Lock()
		delete(c.pendingFetch, frame.id)
		c.mu.Unlock()
		return nil, err
	}
	select {
	case outcome := <-ch:
		return outcome.response, outcome.err
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.pendingFetch, frame.id)
		c.mu.Unlock()
		return nil, ctx.Err()
	}
}

// publish sends a publish frame, returning once the server acks it. When connected it
// sends immediately. When the connection is establishing or temporarily down and
// queueing is enabled (the default), the publish is buffered and sent on the next
// successful (re)connect. A publish that was already in flight when the connection
// dropped is resent on reconnect (its stable messageID dedupes it server-side). It
// fails fast when the state is closing, closed, or failed, and with DisableQueueing, in
// any state but connected.
func (c *Connection) publish(ctx context.Context, frame *publishFrame) error {
	frame.messageID = newClientMessageID()
	c.mu.Lock()
	queueable := c.state == ConnectionInitialized || c.state == ConnectionConnecting || c.state == ConnectionDisconnected
	if c.state != ConnectionConnected && (c.opts.DisableQueueing || !queueable) {
		state := c.state
		c.mu.Unlock()
		suffix := ""
		if c.opts.DisableQueueing {
			suffix = " (queueing disabled)"
		}
		return fmt.Errorf("realtime: cannot publish while %s%s", state, suffix)
	}
	outstanding := &outstandingPublish{frame: frame, done: make(chan error, 1)}
	c.outstanding[frame.messageID] = outstanding
	connected := c.state == ConnectionConnected
	c.mu.Unlock()
	if connected {
		c.sendPublish(outstanding)
	} else {
		// Buffered: kick a connect so it drains even if no reconnect is pending yet
		// (e.g. the very first publish); reconnect backoff drives subsequent retries.
		go func() { _ = c.Connect(context.Background()) }()
	}
	select {
	case err := <-outstanding.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// sendPublish sends an outstanding publish on the current socket under a fresh request
// id.
func (c *Connection) sendPublish(outstanding *outstandingPublish) {
	c.mu.Lock()
	c.nextID++
	id := c.nextID
	outstanding.requestID = id
	outstanding.frame.id = id
	c.publishRequestIDs[id] = outstanding.frame.messageID
	ws := c.socket
	c.mu.Unlock()
	if err := c.send(ws, outstanding.frame); err != nil {
		// Socket not actually open; leave it outstanding to (re)send on the next
		// connect.
		c.mu.Lock()
		delete(c.publishRequestIDs, id)
		if current, ok := c.outstanding[outstanding.frame.messageID]; ok && current == outstanding {
			outstanding.requestID = 0
		}
		c.mu.Unlock()
	}
}

// flushOutstandingPublishes (re)sends every outstanding publish not currently in
// flight. Called on (re)connect.
func (c *Connection) flushOutstandingPublishes() {
	c.mu.Lock()
	var buffered []*outstandingPublish
	for _, outstanding := range c.outstanding {
		if outstanding.requestID == 0 {
			buffered = append(buffered, outstanding)
		}
	}
	c.mu.Unlock()
	for _, outstanding := range buffered {
		c.sendPublish(outstanding)
	}
}

// settlePublishLocked settles the outstanding publish for requestID. Returns false if
// it wasn't a publish.
func (c *Connection) settlePublishLocked(requestID uint64, err error) bool {
	messageID, ok := c.publishRequestIDs[requestID]
	if !ok {
		return false
	}
	delete(c.publishRequestIDs, requestID)
	if outstanding, ok := c.outstanding[messageID]; ok {
		delete(c.outstanding, messageID)
		outstanding.done <- err
	}
	return true
}

// failOutstandingLocked rejects every outstanding publish — used when no resend path
// remains.
func (c *Connection) failOutstandingLocked(err error) {
	c.publishRequestIDs = make(map[uint64]string)
	for messageID, outstanding := range c.outstanding {
		delete(c.outstanding, messageID)
		outstanding.done <- err
	}
}

// failPendingLocked rejects every in-flight ack, history, and fetch request. They can
// never be answered.
func (c *Connection) failPendingLocked(err error) {
	for id, ch := range c.pending {
		delete(c.pending, id)
		ch <- ackOutcome{err: err}
	}
	for id, ch := range c.pendingHist {
		delete(c.pendingHist, id)
		ch <- historyOutcome{err: err}
	}
	for id, ch := range c.pendingFetch {
		delete(c.pendingFetch, id)
		ch <- fetchOutcome{err: err}
	}
}

// ---- channel registry hooks ----

// registerChannel registers the Channel-owned dispatch callbacks used for inbound
// frames.
func (c *Connection) registerChannel(channel string, hooks *channelHooks) {
	c.mu.Lock()
	c.hooks[channel] = hooks
	c.mu.Unlock()
}

// unregisterChannel forgets a channel's dispatch callbacks when the channel is
// released.
func (c *Connection) unregisterChannel(channel string) {
	c.mu.Lock()
	delete(c.hooks, channel)
	c.mu.Unlock()
}

// rememberSubscription adds channel to the set of subscriptions to restore on
// reconnect, and bumps its epoch so an older in-flight detach cannot erase this newer
// intent.
func (c *Connection) rememberSubscription(channel string) {
	c.mu.Lock()
	c.desiredSubs[channel] = struct{}{}
	c.subEpochs[channel]++
	c.mu.Unlock()
}

// subscriptionEpoch returns the current epoch for channel. Bumped by every
// rememberSubscription.
func (c *Connection) subscriptionEpoch(channel string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.subEpochs[channel]
}

// forgetSubscription stops restoring a subscription on future reconnects. When epoch is
// non-negative, the forget only applies if no newer attach has re-remembered the
// channel since that epoch was read (a detach ack racing a fresh attach must not erase
// the new subscription intent).
func (c *Connection) forgetSubscription(channel string, epoch int) {
	c.mu.Lock()
	if epoch < 0 || epoch == c.subEpochs[channel] {
		delete(c.desiredSubs, channel)
	}
	c.mu.Unlock()
}

// rememberPresence adds channel to the set of presence subscriptions to restore on
// reconnect.
func (c *Connection) rememberPresence(channel string) {
	c.mu.Lock()
	c.desiredPresence[channel] = struct{}{}
	c.mu.Unlock()
}

// forgetPresence stops restoring a presence subscription on future reconnects.
func (c *Connection) forgetPresence(channel string) {
	c.mu.Lock()
	delete(c.desiredPresence, channel)
	c.mu.Unlock()
}

// ---- connect internals ----

func (c *Connection) runConnect(attempt *connectAttempt) {
	err := c.doConnect()
	attempt.err = err
	c.mu.Lock()
	c.connecting = nil
	c.mu.Unlock()
	close(attempt.done)
}

func (c *Connection) doConnect() error {
	c.mu.Lock()
	c.setStateLocked(ConnectionConnecting, nil)
	c.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), handshakeTimeout)
	defer cancel()

	// Build the auth frame BEFORE opening the socket: AuthCallback may take a while (a
	// token fetch), and the server would drop a socket that sits silent past the
	// handshake window.
	auth, err := c.createAuthFrame(ctx)
	var ws *websocket.Conn
	if err == nil {
		ws, _, err = websocket.Dial(ctx, endpointToWsURL(c.opts.Endpoint), nil)
	}
	if err != nil {
		// No socket read loop exists yet, so nothing else will drive the state
		// machine. Mark disconnected and schedule the retry, or a transient
		// token-endpoint failure would wedge the state at connecting with nothing in
		// flight.
		c.mu.Lock()
		if c.state == ConnectionConnecting {
			c.setStateLocked(ConnectionDisconnected, err)
			if !c.opts.DisableAutoReconnect {
				c.scheduleReconnectLocked()
			}
		}
		c.mu.Unlock()
		return err
	}

	c.mu.Lock()
	if c.state == ConnectionClosing || c.state == ConnectionClosed {
		// Close ran while the auth frame was being built. Don't resurrect.
		c.mu.Unlock()
		_ = ws.Close(websocket.StatusNormalClosure, "client close")
		return errConnectionClosed
	}
	c.socket = ws
	c.mu.Unlock()

	if err := c.finishHandshake(ctx, ws, auth); err != nil {
		return err
	}
	return nil
}

// finishHandshake sends the auth frame and processes the server's first message: a
// connected frame on success, an error frame on an auth rejection.
func (c *Connection) finishHandshake(ctx context.Context, ws *websocket.Conn, auth *authFrame) error {
	// A binary auth frame makes the whole connection binary (the edge decides by the
	// WebSocket opcode of this frame): the edge then coalesces frames and delivers
	// binary, both implied by speaking binary.
	if err := c.send(ws, auth); err != nil {
		c.teardownSocket(ws, closeCodeHandshakeFailed, "auth send failed", err)
		return err
	}
	frames, err := c.readFrames(ctx, ws)
	if err != nil {
		err = fmt.Errorf("realtime: websocket closed during handshake: %w", err)
		c.teardownSocket(ws, closeCodeHandshakeFailed, "handshake read failed", err)
		return err
	}
	if len(frames) == 0 {
		err := errors.New("realtime: failed to parse auth response")
		c.teardownSocket(ws, closeCodeHandshakeFailed, "bad auth response", err)
		return err
	}
	switch first := frames[0].(type) {
	case *connectedFrame:
		return c.completeConnect(ws, first, frames[1:])
	case *errorFrame:
		err := fmt.Errorf("realtime: auth failed: %d %s", first.code, first.message)
		authError := first.code == CodeBadAuth || first.code == CodeAuthExpired
		// An auth rejection only retries if AuthCallback can produce a fresh
		// credential next attempt; a static Token/Key would be re-sent and rejected
		// identically, so treat that as terminal.
		if authError && c.opts.AuthCallback == nil {
			c.mu.Lock()
			c.fatalErr = err
			c.mu.Unlock()
		}
		c.teardownSocket(ws, closeCodeHandshakeFailed, "auth error", err)
		return err
	default:
		err := errors.New("realtime: unexpected first frame during handshake")
		c.teardownSocket(ws, closeCodeHandshakeFailed, "unexpected frame", err)
		return err
	}
}

// completeConnect installs the authenticated socket: state, keep-alive, the steady read
// loop, subscription restore, and the publish queue flush.
func (c *Connection) completeConnect(ws *websocket.Conn, connected *connectedFrame, rest []any) error {
	c.mu.Lock()
	if c.state == ConnectionClosing || c.state == ConnectionClosed {
		// Close ran while the handshake was in flight. Don't resurrect the connection
		// into a zombie the app believes is closed.
		c.mu.Unlock()
		_ = ws.Close(websocket.StatusNormalClosure, "client close")
		return errConnectionClosed
	}
	c.connectionID = connected.connectionID
	c.clientID = connected.clientID
	c.reconnectAttempt = 0
	isReconnect := c.hasConnectedBefore
	c.hasConnectedBefore = true
	c.setStateLocked(ConnectionConnected, nil)
	c.startKeepAliveLocked(ws, time.Duration(connected.keepAliveMs)*time.Millisecond)
	c.mu.Unlock()

	// The edge may coalesce more frames into the same WebSocket message as connected.
	// Dispatch them BEFORE the read loop starts, so the next socket message cannot
	// interleave ahead of them.
	for _, frame := range rest {
		c.handleFrame(frame)
	}
	go c.readLoop(ws)
	c.restoreSubscriptionsOnReconnect(isReconnect)
	c.flushOutstandingPublishes()
	return nil
}

// teardownSocket closes a socket that failed its handshake and runs the close
// bookkeeping (the read loop never started, so nothing else will).
func (c *Connection) teardownSocket(ws *websocket.Conn, code websocket.StatusCode, reason string, err error) {
	_ = ws.Close(code, reason)
	c.handleSocketClose(ws, err)
}

// createAuthFrame builds the handshake frame for the configured credential. On a
// reconnect it carries the previous connection id so the server reuses it, keeping this
// connection's presence membership stable across a brief drop (no leave/enter churn).
func (c *Connection) createAuthFrame(ctx context.Context) (*authFrame, error) {
	c.mu.Lock()
	resume := c.connectionID
	c.mu.Unlock()
	if c.opts.Key != "" {
		return &authFrame{key: c.opts.Key, clientID: c.opts.ClientID, resumeConnectionID: resume}, nil
	}
	if c.opts.Token != "" {
		return &authFrame{token: c.opts.Token, resumeConnectionID: resume}, nil
	}
	token, err := c.opts.AuthCallback(ctx)
	if err != nil {
		return nil, fmt.Errorf("realtime: auth callback: %w", err)
	}
	return &authFrame{token: token, resumeConnectionID: resume}, nil
}

// readFrames reads one WebSocket message and decodes its records.
func (c *Connection) readFrames(ctx context.Context, ws *websocket.Conn) ([]any, error) {
	for {
		messageType, data, err := ws.Read(ctx)
		if err != nil {
			return nil, err
		}
		if messageType != websocket.MessageBinary {
			continue
		}
		return decodeServerFrames(data), nil
	}
}

// readLoop is the steady-state receive loop for one socket. Every server frame arrives
// binary: one WebSocket message carries one or more opcode records.
func (c *Connection) readLoop(ws *websocket.Conn) {
	for {
		messageType, data, err := ws.Read(context.Background())
		if err != nil {
			c.handleSocketClose(ws, err)
			return
		}
		// Anything inbound proves the link is alive.
		c.clearPongDeadline()
		if messageType != websocket.MessageBinary {
			continue
		}
		for _, frame := range decodeServerFrames(data) {
			c.handleFrame(frame)
		}
	}
}

// handleFrame dispatches one decoded server frame to its waiting caller or channel.
func (c *Connection) handleFrame(frame any) {
	switch f := frame.(type) {
	case *ackFrame:
		c.mu.Lock()
		if ch, ok := c.pending[f.id]; ok {
			delete(c.pending, f.id)
			c.mu.Unlock()
			ch <- ackOutcome{ack: f}
			return
		}
		c.settlePublishLocked(f.id, nil)
		c.mu.Unlock()
	case *errorFrame:
		c.handleErrorFrame(f)
	case *msgFrame:
		c.mu.Lock()
		hooks := c.hooks[f.channel]
		c.mu.Unlock()
		if hooks != nil {
			hooks.message(f)
		}
	case *presenceEventFrame:
		c.mu.Lock()
		hooks := c.hooks[f.channel]
		c.mu.Unlock()
		if hooks != nil {
			hooks.presence(f)
		}
	case *historyResponseFrame:
		c.mu.Lock()
		if ch, ok := c.pendingHist[f.id]; ok {
			delete(c.pendingHist, f.id)
			c.mu.Unlock()
			ch <- historyOutcome{response: f}
			return
		}
		c.mu.Unlock()
	case *fetchResponseFrame:
		c.mu.Lock()
		if ch, ok := c.pendingFetch[f.id]; ok {
			delete(c.pendingFetch, f.id)
			c.mu.Unlock()
			ch <- fetchOutcome{response: f}
			return
		}
		c.mu.Unlock()
	case *pongFrame, *connectedFrame:
		// connected can only arrive once (the handshake consumed it); pong needs no
		// handling beyond the proof-of-life the read loop already recorded.
	}
}

func (c *Connection) handleErrorFrame(f *errorFrame) {
	serverErr := &ServerError{Code: f.code, Message: f.message}
	if f.id != 0 {
		c.mu.Lock()
		if ch, ok := c.pending[f.id]; ok {
			delete(c.pending, f.id)
			c.mu.Unlock()
			ch <- ackOutcome{err: serverErr}
			return
		}
		if c.settlePublishLocked(f.id, serverErr) {
			c.mu.Unlock()
			return
		}
		if ch, ok := c.pendingHist[f.id]; ok {
			delete(c.pendingHist, f.id)
			c.mu.Unlock()
			ch <- historyOutcome{err: serverErr}
			return
		}
		if ch, ok := c.pendingFetch[f.id]; ok {
			delete(c.pendingFetch, f.id)
			c.mu.Unlock()
			ch <- fetchOutcome{err: serverErr}
			return
		}
		c.mu.Unlock()
	}
	// Unscoped errors (id 0 or an unknown id) are surfaced through the current
	// connection event so consumers can observe transport errors.
	c.mu.Lock()
	c.emitStateLocked(c.state, serverErr)
	c.mu.Unlock()
}

// handleSocketClose runs the teardown for one socket ending, from the read loop's read
// error, a failed handshake, or a synthesized keep-alive timeout. A stale socket's
// close (an earlier, already-replaced or already-handled attempt) is ignored.
func (c *Connection) handleSocketClose(ws *websocket.Conn, cause error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.socket != ws {
		return
	}
	c.socket = nil
	c.stopKeepAliveLocked()
	// The dead socket's requests can never be answered: drop the publish id mappings
	// and reject in-flight acks, history, and fetches so attach, detach, history,
	// presence, and gap-fill callers do not hang forever.
	c.publishRequestIDs = make(map[uint64]string)
	if c.state == ConnectionClosing || c.state == ConnectionClosed {
		c.setStateLocked(ConnectionClosed, nil)
		c.failPendingLocked(errConnectionClosed)
		c.failOutstandingLocked(errConnectionClosed)
		return
	}
	if c.fatalErr != nil {
		// Unrecoverable auth failure: stop here rather than retry a credential the
		// server will keep rejecting. A later explicit Connect can still retry.
		fatal := c.fatalErr
		c.fatalErr = nil
		c.setStateLocked(ConnectionFailed, fatal)
		c.failPendingLocked(fatal)
		c.failOutstandingLocked(fatal)
		return
	}
	// Surface why we dropped (e.g. our own 4001 auth-error close) so listeners can
	// tell a transient network blip from a credential problem the reconnect loop will
	// never fix on its own.
	reason := fmt.Errorf("realtime: websocket closed: %w", cause)
	c.failPendingLocked(reason)
	c.setStateLocked(ConnectionDisconnected, reason)
	willRetry := !c.opts.DisableAutoReconnect && !c.opts.DisableQueueing
	if willRetry {
		// Keep outstanding publishes (in-flight + buffered) to resend on reconnect.
		for _, outstanding := range c.outstanding {
			outstanding.requestID = 0
		}
	} else {
		// No resend path remains, so don't leave publishes hanging.
		c.failOutstandingLocked(errConnectionClosed)
	}
	if !c.opts.DisableAutoReconnect {
		c.scheduleReconnectLocked()
	}
}

func (c *Connection) scheduleReconnectLocked() {
	if c.reconnectTimer != nil {
		return
	}
	initial := c.opts.InitialReconnectDelay
	if initial <= 0 {
		initial = defaultInitialReconnectDelay
	}
	max := c.opts.MaxReconnectDelay
	if max <= 0 {
		max = defaultMaxReconnectDelay
	}
	delay := initial << c.reconnectAttempt
	if delay > max || delay <= 0 {
		delay = max
	}
	c.reconnectAttempt++
	c.reconnectTimer = time.AfterFunc(delay, func() {
		c.mu.Lock()
		c.reconnectTimer = nil
		c.mu.Unlock()
		if err := c.Connect(context.Background()); err != nil {
			// doConnect itself drove the state machine; schedule another attempt
			// unless we've been explicitly closed or hit a terminal auth failure.
			c.mu.Lock()
			if c.state != ConnectionClosed && c.state != ConnectionClosing && c.state != ConnectionFailed {
				c.scheduleReconnectLocked()
			}
			c.mu.Unlock()
		}
	})
}

// restoreSubscriptionsOnReconnect re-issues the remembered subscriptions and presence
// watchers, and re-announces presence membership. Only on an actual reconnect: on the
// first connect the app's own Attach and presence calls have their frames in flight
// already (their requests wait on Connect), so restoring here would send duplicate
// subs, and the duplicate's ack would surface as a spurious update on the channel.
func (c *Connection) restoreSubscriptionsOnReconnect(isReconnect bool) {
	if !isReconnect {
		return
	}
	c.mu.Lock()
	subs := make([]string, 0, len(c.desiredSubs))
	for channel := range c.desiredSubs {
		subs = append(subs, channel)
	}
	presence := make([]string, 0, len(c.desiredPresence))
	for channel := range c.desiredPresence {
		presence = append(presence, channel)
	}
	allHooks := make([]*channelHooks, 0, len(c.hooks))
	for _, hooks := range c.hooks {
		allHooks = append(allHooks, hooks)
	}
	c.mu.Unlock()
	// Re-issue a sub for every remembered channel, carrying its resume cursor so the
	// server replays whatever was published during the disconnect, then report the
	// resume outcome (replayed vs discontinuity) back to the channel.
	for _, channel := range subs {
		c.mu.Lock()
		hooks := c.hooks[channel]
		c.mu.Unlock()
		var lastSerial uint64
		if hooks != nil {
			lastSerial = hooks.lastSerial()
		}
		go func(channel string, lastSerial uint64, hooks *channelHooks) {
			ack, err := c.request(context.Background(), func(id uint64) any {
				return &subscribeFrame{channel: channel, id: id, lastSerial: lastSerial}
			})
			if err != nil {
				// A failed restore surfaces via channel state on the next reconnect;
				// the channel stays attaching until then.
				return
			}
			if hooks != nil {
				hooks.resumed(ack.resumed != nil && *ack.resumed)
			}
		}(channel, lastSerial, hooks)
	}
	// Re-open presence watchers for channels the app is watching presence on.
	for _, channel := range presence {
		go func(channel string) {
			_, _ = c.request(context.Background(), func(id uint64) any {
				return &presenceSubscribeFrame{channel: channel, id: id}
			})
		}(channel)
	}
	// Re-announce presence membership: each channel re-enters whatever it had entered.
	for _, hooks := range allHooks {
		hooks.reenterPresence()
	}
}

// ---- keep-alive ----

// startKeepAliveLocked starts sending a keep-alive ping every interval so an idle
// connection (no subscriptions or traffic) is not culled by an intermediary such as a
// proxy's WebSocket idle timeout, which surfaces to the app as an opaque drop. A no-op
// when the interval is non-positive.
func (c *Connection) startKeepAliveLocked(ws *websocket.Conn, interval time.Duration) {
	c.stopKeepAliveLocked()
	if interval <= 0 {
		return
	}
	c.pongDeadlineDur = interval
	if c.pongDeadlineDur < minPongDeadline {
		c.pongDeadlineDur = minPongDeadline
	}
	if c.pongDeadlineDur > maxPongDeadline {
		c.pongDeadlineDur = maxPongDeadline
	}
	stop := make(chan struct{})
	c.keepAliveStop = stop
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				c.mu.Lock()
				alive := c.state == ConnectionConnected && c.socket == ws
				c.mu.Unlock()
				if !alive {
					return
				}
				if err := c.send(ws, &pingFrame{}); err == nil {
					c.armPongDeadline(ws)
				}
			}
		}
	}()
}

// stopKeepAliveLocked stops the keep-alive ping goroutine and the dead-link detector.
func (c *Connection) stopKeepAliveLocked() {
	if c.keepAliveStop != nil {
		close(c.keepAliveStop)
		c.keepAliveStop = nil
	}
	if c.pongDeadline != nil {
		c.pongDeadline.Stop()
		c.pongDeadline = nil
	}
}

// armPongDeadline arms the dead-link detector after sending a ping. Any inbound frame
// counts as proof of life and disarms it (a busy connection may deliver messages ahead
// of the pong). When nothing arrives before the deadline, the link is dead: without
// this, a half-dead TCP connection sits connected with publishes pending until the
// kernel gives up minutes later.
func (c *Connection) armPongDeadline(ws *websocket.Conn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pongDeadline != nil {
		return
	}
	c.pongDeadline = time.AfterFunc(c.pongDeadlineDur, func() {
		c.mu.Lock()
		c.pongDeadline = nil
		dead := c.state == ConnectionConnected && c.socket == ws
		c.mu.Unlock()
		if !dead {
			return
		}
		// Drive the teardown ourselves instead of waiting for the dead socket's read
		// error. handleSocketClose ignores the socket's own close later.
		_ = ws.Close(closeCodeKeepAliveTimeout, "keep-alive timeout")
		c.handleSocketClose(ws, errors.New("keep-alive timeout"))
	})
}

// clearPongDeadline disarms the dead-link detector. Any inbound frame proves the link
// is alive.
func (c *Connection) clearPongDeadline() {
	c.mu.Lock()
	if c.pongDeadline != nil {
		c.pongDeadline.Stop()
		c.pongDeadline = nil
	}
	c.mu.Unlock()
}

// ---- send / state ----

// send writes one client frame as a length-prefixed binary record on ws.
func (c *Connection) send(ws *websocket.Conn, frame any) error {
	if ws == nil {
		return fmt.Errorf("realtime: socket not open (state=%s)", c.State())
	}
	record := frameBinaryRecord(encodeClientFrame(frame))
	ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
	defer cancel()
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return ws.Write(ctx, websocket.MessageBinary, record)
}

// setStateLocked transitions the state and queues the change for listeners. A no-op
// when the state is unchanged.
func (c *Connection) setStateLocked(state ConnectionState, reason error) {
	if c.state == state {
		return
	}
	c.state = state
	c.emitStateLocked(state, reason)
}

// emitStateLocked queues a state event for listeners without requiring a transition
// (unscoped server errors re-emit the current state with a reason).
func (c *Connection) emitStateLocked(state ConnectionState, reason error) {
	change := ConnectionStateChange{Current: state, Reason: reason}
	c.dispatch.enqueue(func() {
		c.events.emit(state, change)
	})
}

// newClientMessageID builds a client-assigned message id for a publish —
// "<unixMillis>-<random>", roughly time-sortable like the server's ids. Generated once
// per publish and reused across resends, so the server's dedup window can collapse a
// retried publish.
func newClientMessageID() string {
	return fmt.Sprintf("%d-%08x", time.Now().UnixMilli(), rand.Uint32())
}

// endpointToWsURL resolves an endpoint (bare host or absolute URL) to the WebSocket
// URL. http(s) schemes are mapped to ws(s), so an httptest server URL works directly in
// tests.
func endpointToWsURL(endpoint string) string {
	if endpoint == "" {
		endpoint = DefaultEndpoint
	}
	switch {
	case strings.HasPrefix(endpoint, "ws://"), strings.HasPrefix(endpoint, "wss://"):
		return endpoint
	case strings.HasPrefix(endpoint, "http://"):
		return "ws://" + strings.TrimPrefix(endpoint, "http://")
	case strings.HasPrefix(endpoint, "https://"):
		return "wss://" + strings.TrimPrefix(endpoint, "https://")
	}
	return "wss://" + endpoint
}
