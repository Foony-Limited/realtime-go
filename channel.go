package realtime

// Channel public API. Wraps the Connection layer with per-channel state.
//
// The channel deliberately exposes two separate listener surfaces so callers never
// confuse lifecycle with data: On / Once / Off observe the channel's lifecycle state (a
// closed set of events), while Subscribe / Unsubscribe carry application messages
// (open-ended event names).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

// Default auto-batch tuning, see BatchOptions.
const (
	defaultBatchInterval    = 10 * time.Millisecond
	defaultBatchMaxMessages = 200
)

// dedupCacheMax caps the per-channel delivered-message dedup cache (number of message
// ids). This limit determines how many recent messages are remembered for exactly-once
// delivery. A larger limit means more memory, but more reliable delivery.
const dedupCacheMax = 8192

// Message is one message delivered to subscribers or returned from history.
type Message struct {
	// Channel is the channel the message was published to.
	Channel string
	// Name is the application-level event name the message was published under.
	Name string
	// Data is the message payload as published (decrypted on a channel with a cipher).
	// Unmarshal it into your own type with json.Unmarshal.
	Data json.RawMessage
	// Timestamp is the server publish time in milliseconds since the Unix epoch.
	Timestamp int64
	// ID is the unique message id, for dedup and idempotent publishing. Batch members
	// share their batch's id with a ":<index>" suffix.
	ID string
	// ClientID is the client id of the publisher, when known.
	ClientID string
	// Encoding says how Data is encoded (e.g. "cipher+aes-256-gcm/base64" when a
	// message could not be decrypted). Empty for plain JSON.
	Encoding string
	// Serial is the contiguous per-channel serial (0 for ephemeral/unsequenced
	// messages). The SDK uses it to detect gaps, as the resume cursor, and it is
	// history's Before cursor to page backward from this message.
	Serial uint64
	// Ephemeral marks a fire-and-forget message: not stored in history and not
	// replayed on resume.
	Ephemeral bool
}

// BatchMessage is one message in a batch publish: its own event name and payload.
type BatchMessage struct {
	// Name is the application-level event name.
	Name string
	// Data is the JSON-serializable payload.
	Data any
}

// BatchOptions configures automatic publish batching. Single Publish calls are always
// auto-batched, buffered and flushed as one batch frame (one stored, dedupable
// message), which massively raises per-channel throughput for little to no latency
// cost. Batching is always on. PublishBatch and [Client.BatchPublish] are never batched
// further (they assume the caller is managing batching).
type BatchOptions struct {
	// Interval is the minimum gap between batch sends, applied as a throttle. A
	// publish is sent right away unless a batch went out within the last Interval, in
	// which case it waits until the window is up. Publishes spaced further apart than
	// Interval are never batched together and add no latency. Only fast bursts get
	// grouped into one batch. Defaults to 10ms when zero.
	Interval time.Duration
	// MaxMessages flushes early once this many messages are buffered. Defaults to 200
	// when zero.
	MaxMessages int
}

// publishSettings collects the publish options.
type publishSettings struct {
	ephemeral bool
}

// PublishOption customizes one publish call.
type PublishOption func(*publishSettings)

// WithEphemeral marks the publish fire-and-forget: delivered live to current
// subscribers but never stored, so it is excluded from history and reconnect replay.
// For transient events on a channel that otherwise persists.
func WithEphemeral() PublishOption {
	return func(settings *publishSettings) {
		settings.ephemeral = true
	}
}

// ChannelState is a channel lifecycle state. A healthy channel follows the states in
// order: initialized -> attaching -> attached -> detaching -> detached -> attaching,
// and so on. A channel in the failed state is not retried and is not re-attached.
type ChannelState string

// Channel lifecycle states.
const (
	// ChannelInitialized means the channel was created locally and no attach has been
	// attempted yet.
	ChannelInitialized ChannelState = "initialized"
	// ChannelAttaching means an attach was requested and is awaiting server
	// confirmation.
	ChannelAttaching ChannelState = "attaching"
	// ChannelAttached means messages and presence for this channel are flowing.
	ChannelAttached ChannelState = "attached"
	// ChannelDetaching means a detach was requested and is awaiting server
	// confirmation.
	ChannelDetaching ChannelState = "detaching"
	// ChannelDetached means no messages or presence are delivered until re-attached.
	ChannelDetached ChannelState = "detached"
	// ChannelSuspended means the channel was temporarily lost, for example because the
	// connection dropped. The SDK re-attaches on reconnect. You can keep publishing:
	// unless Options.DisableQueueing is set, publishes are queued locally and sent
	// once reconnected.
	ChannelSuspended ChannelState = "suspended"
	// ChannelFailed means the attach failed with an error a retry will not fix, for
	// example a missing capability, and the SDK will not retry it. Call Attach to try
	// again manually, for example after obtaining a token with more capabilities.
	ChannelFailed ChannelState = "failed"
)

// ChannelEvent is an event name channel state listeners can filter on: every
// [ChannelState] plus [ChannelEventUpdate].
type ChannelEvent string

// ChannelEventUpdate is a change that is not a state transition. The channel stayed in
// the same state but something was updated, for example the server reported a resume
// outcome while the channel was already attached. Check Resumed on the payload: false
// means messages may have been missed beyond retention, so reload state or read
// history.
const ChannelEventUpdate ChannelEvent = "update"

// ChannelStateChange is the payload delivered to channel state listeners.
type ChannelStateChange struct {
	// Current is the state the channel is now in.
	Current ChannelState
	// Previous is the state the channel was in immediately before this event.
	Previous ChannelState
	// Reason is the error that caused the transition, when the event was error-driven.
	Reason error
	// Resumed is true when the channel resumed without missing messages (e.g. after a
	// reconnect).
	Resumed bool
}

// transitionInfo carries the optional payload of one state transition. A nil
// transitionInfo on a same-state transition is dropped, while a non-nil one still reaches
// listeners as a ChannelEventUpdate.
type transitionInfo struct {
	reason  error
	resumed bool
}

// bufferedPublish is one buffered publish awaiting the next auto-batch flush.
type bufferedPublish struct {
	member wireMember
	done   chan error
}

// attachAttempt is one in-flight attach shared by concurrent Attach callers.
type attachAttempt struct {
	done chan struct{}
	err  error
}

// Channel is a named channel. Subscribe to receive its messages, publish to send them,
// and use [Channel.Presence] to see who is there. Get instances via
// client.Channels.Get(name). The same name always returns the same instance on a given
// client.
//
// The channel has two separate listener surfaces: On / Once / Off listen on the
// channel's lifecycle [ChannelState], while Subscribe / Unsubscribe receive application
// messages.
//
//	channel := client.Channels.Get("chat:room-1")
//	channel.Subscribe(func(message *realtime.Message) {
//		fmt.Println(message.Name, string(message.Data))
//	}, "greeting")
//	err := channel.Publish(ctx, "greeting", map[string]string{"text": "hi"})
type Channel struct {
	// Name is the channel name this instance is bound to (e.g. "chat:room-1").
	Name string
	// Presence announces membership on this channel and listens on who comes and goes.
	Presence *Presence

	conn     *Connection
	states   *emitter[ChannelEvent, ChannelStateChange]
	messages *emitter[string, *Message]
	cipher   *Cipher
	batch    BatchOptions

	mu        sync.Mutex
	state     ChannelState
	attaching *attachAttempt
	batchBuf  []bufferedPublish
	// batchTimer is the pending flush, nil when none is scheduled.
	batchTimer *time.Timer
	// lastFlush is when the last batch was sent, throttling sends to one per Interval.
	lastFlush time.Time
	// contiguousSerial is the highest serial up to which this channel has received
	// every message with no gap. Sent on (re)subscribe so the server replays anything
	// missed during a disconnect. 0 means no baseline yet: the next sequenced message
	// is adopted as the baseline (a fresh subscriber starts from "now", not from
	// serial 1). A channel that has only seen unsequenced messages (such as ephemeral
	// ones) keeps 0 and resubscribes fresh.
	contiguousSerial uint64
	// backfilling is true while a gap-fill fetch is in flight. Gapped messages
	// arriving during that window do not start more fetches: one fetch from the cursor
	// replays everything after it, so it heals the whole burst.
	backfilling bool
	// seen is a bounded, insertion-ordered set of recently delivered
	// (clientID, messageID) keys, for exactly-once delivery. The server coalesces
	// publishes across clients into one record and does not dedup the individual
	// messages within it, so a publisher retry can deliver a message twice. We drop
	// the repeat here. Keyed on the server-stamped clientID, so one client cannot
	// suppress another's message by reusing its id.
	seen      map[string]struct{}
	seenOrder []string
	// connOff removes this channel's connection state listener. Called on release.
	connOff func()
}

func newChannel(conn *Connection, name string, cipher *Cipher, batch *BatchOptions) *Channel {
	resolved := BatchOptions{Interval: defaultBatchInterval, MaxMessages: defaultBatchMaxMessages}
	if batch != nil {
		if batch.Interval > 0 {
			resolved.Interval = batch.Interval
		}
		if batch.MaxMessages > 0 {
			resolved.MaxMessages = batch.MaxMessages
		}
	}
	channel := &Channel{
		Name:     name,
		conn:     conn,
		states:   newEmitter[ChannelEvent, ChannelStateChange](),
		messages: newEmitter[string, *Message](),
		cipher:   cipher,
		batch:    resolved,
		state:    ChannelInitialized,
		seen:     make(map[string]struct{}),
	}
	channel.Presence = newPresence(conn, name, channel, cipher)
	conn.registerChannel(name, &channelHooks{
		message:         channel.deliverMessage,
		presence:        channel.Presence.emitPresence,
		lastSerial:      channel.cursor,
		resumed:         channel.onResumed,
		reenterPresence: channel.Presence.reenterOnReconnect,
	})
	channel.connOff = conn.On(channel.onConnectionState)
	return channel
}

// dispose detaches this channel's state machine from the connection so released
// instances are not retained forever by its listener set.
func (ch *Channel) dispose() {
	ch.connOff()
}

// State returns the current [ChannelState]. Listen on changes with [Channel.On].
func (ch *Channel) State() ChannelState {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return ch.state
}

// On registers a listener for every channel state event and returns its unsubscribe
// function.
func (ch *Channel) On(listener func(ChannelStateChange)) func() {
	return ch.states.on(listener)
}

// OnEvent registers a listener for one [ChannelEvent] (a state name or
// [ChannelEventUpdate]) and returns its unsubscribe function.
func (ch *Channel) OnEvent(event ChannelEvent, listener func(ChannelStateChange)) func() {
	return ch.states.onEvent(event, listener, false)
}

// Once registers a listener invoked one time for the next matching event, and returns
// its unsubscribe function.
func (ch *Channel) Once(event ChannelEvent, listener func(ChannelStateChange)) func() {
	return ch.states.onEvent(event, listener, true)
}

// Off removes every channel state listener.
func (ch *Channel) Off() {
	ch.states.offAll()
}

// Attach ensures the server is subscribed to this channel. Subscribe and the presence
// methods call this implicitly, so calling it yourself is optional. It is useful for
// surfacing attach errors before the first message arrives. It returns nil once the
// server confirms the channel subscription, the server's error when the token lacks the
// subscribe capability (the channel moves to [ChannelFailed] and is not retried), and
// the transport error when the request fails in transit (the channel moves to
// [ChannelSuspended] and re-attaches on reconnect).
func (ch *Channel) Attach(ctx context.Context) error {
	ch.mu.Lock()
	if ch.state == ChannelAttached {
		ch.mu.Unlock()
		return nil
	}
	attempt := ch.attaching
	if attempt == nil {
		attempt = &attachAttempt{done: make(chan struct{})}
		ch.attaching = attempt
		ch.transitionLocked(ChannelAttaching, nil)
		ch.mu.Unlock()
		// Remember the intent before the request resolves: if the connection drops
		// mid-attach, this channel must still be re-subscribed once the connection is
		// restored, not left orphaned. A terminal capability denial forgets it again
		// below so it isn't retried.
		ch.conn.rememberSubscription(ch.Name)
		go ch.runAttach(attempt)
	} else {
		ch.mu.Unlock()
	}
	select {
	case <-attempt.done:
		return attempt.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (ch *Channel) runAttach(attempt *attachAttempt) {
	lastSerial := ch.cursor()
	ack, err := ch.conn.request(context.Background(), func(id uint64) any {
		return &subscribeFrame{channel: ch.Name, id: id, lastSerial: lastSerial}
	})
	if err != nil && isCapabilityError(err) {
		// Permission won't change on retry, so stop trying and surface it.
		ch.conn.forgetSubscription(ch.Name, -1)
	}
	ch.mu.Lock()
	ch.attaching = nil
	switch {
	case err == nil:
		// The reconnect restore path may have re-subscribed and reported the
		// authoritative resume outcome while this request was in flight. Don't clobber
		// it with a same-state re-confirmation (a spurious update).
		if ch.state != ChannelAttached {
			resumed := ack.resumed != nil && *ack.resumed
			ch.transitionLocked(ChannelAttached, &transitionInfo{resumed: resumed})
		}
	case isCapabilityError(err):
		ch.transitionLocked(ChannelFailed, &transitionInfo{reason: err})
	default:
		// Transient (e.g. the connection dropped mid-attach): stay remembered and
		// suspended so the reconnect re-subscribe recovers the channel.
		ch.transitionLocked(ChannelSuspended, &transitionInfo{reason: err})
	}
	ch.mu.Unlock()
	attempt.err = err
	close(attempt.done)
}

// Detach detaches from the server: stop receiving messages and presence events.
// Buffered auto-batched publishes are flushed first. Local listeners are preserved,
// call Off or Unsubscribe to clear them. It returns nil once the server confirms the
// detach, and the request's error when it fails, though the channel is marked detached
// either way.
func (ch *Channel) Detach(ctx context.Context) error {
	// Don't strand buffered auto-batched publishes on detach.
	ch.Flush()
	ch.mu.Lock()
	if ch.state == ChannelInitialized || ch.state == ChannelDetached || ch.state == ChannelDetaching {
		ch.mu.Unlock()
		return nil
	}
	ch.transitionLocked(ChannelDetaching, nil)
	ch.mu.Unlock()
	// Detaching the channel ends presence too: the server's unsub closes the presence
	// watcher, so stop re-opening it and re-entering on future reconnects.
	ch.Presence.onDetached()
	// A fresh attach (on this instance or on a replacement after Release) can race
	// this detach. Capture the subscription epoch now: if the attach re-remembered the
	// channel while the unsub was in flight, the epoch moved on and this detach must
	// not erase the newer intent or clobber the state.
	epoch := ch.conn.subscriptionEpoch(ch.Name)
	_, err := ch.conn.request(ctx, func(id uint64) any {
		return &unsubscribeFrame{channel: ch.Name, id: id}
	})
	ch.conn.forgetSubscription(ch.Name, epoch)
	ch.mu.Lock()
	if ch.state == ChannelDetaching {
		ch.transitionLocked(ChannelDetached, nil)
	}
	ch.mu.Unlock()
	return err
}

// Subscribe registers a listener for messages on this channel: every message when no
// names are given, otherwise only messages whose name matches one of names. It
// implicitly attaches if needed and returns an unsubscribe function. Subscribe itself
// does not block, so attach failures do not surface here: call [Channel.Attach] first
// if you want to observe them.
func (ch *Channel) Subscribe(listener func(*Message), names ...string) func() {
	var unsubscribe func()
	if len(names) == 0 {
		unsubscribe = ch.messages.on(listener)
	} else {
		offs := make([]func(), len(names))
		for i, name := range names {
			offs[i] = ch.messages.onEvent(name, listener, false)
		}
		unsubscribe = func() {
			for _, off := range offs {
				off()
			}
		}
	}
	// Fire-and-forget attach. The listener stays registered even if attach fails so a
	// retry-on-reconnect surfaces the right state.
	go func() { _ = ch.Attach(context.Background()) }()
	return unsubscribe
}

// Unsubscribe removes every message listener on this channel. To remove one listener,
// call the function Subscribe returned.
func (ch *Channel) Unsubscribe() {
	ch.messages.offAll()
}

// Publish publishes one message to the channel. On a channel with a cipher, data is
// end-to-end encrypted before it is sent. data is marshaled with json.Marshal (pass a
// json.RawMessage to send pre-encoded JSON). It returns nil once the server acks the
// publish. Unless Options.DisableQueueing is set, publishes made while the connection
// is down are queued locally and sent on reconnect. It returns the server's error when
// the service refuses the publish (for example a token without the publish capability),
// and an immediate error when the connection is closing, closed, or failed. With
// DisableQueueing, any connection state but connected returns an error immediately.
//
// Pass [WithEphemeral] to send a fire-and-forget message: delivered live to current
// subscribers but never stored, so it is excluded from history and reconnect replay.
func (ch *Channel) Publish(ctx context.Context, name string, data any, options ...PublishOption) error {
	var settings publishSettings
	for _, option := range options {
		option(&settings)
	}
	member, err := ch.toMember(name, data)
	if err != nil {
		return err
	}
	// Auto-batch single publishes, but not ephemeral ones (a batch shares one
	// ephemeral disposition), so send those immediately.
	if !settings.ephemeral {
		return ch.enqueue(ctx, member)
	}
	return ch.conn.publish(ctx, &publishFrame{
		channel:   ch.Name,
		name:      member.name,
		data:      member.data,
		encoding:  member.encoding,
		ephemeral: true,
	})
}

// PublishBatch publishes a batch of messages in a single frame under one message id.
// This is an atomic batch. The server stores and dedups it as one durable message,
// while subscribers receive the members individually. This counts as 1 message for the
// purposes of usage limits / quotas (message size limits still apply). Pass
// [WithEphemeral] to make the whole batch fire-and-forget.
func (ch *Channel) PublishBatch(ctx context.Context, messages []BatchMessage, options ...PublishOption) error {
	var settings publishSettings
	for _, option := range options {
		option(&settings)
	}
	members := make([]wireMember, 0, len(messages))
	for _, message := range messages {
		member, err := ch.toMember(message.Name, message.Data)
		if err != nil {
			return err
		}
		members = append(members, member)
	}
	return ch.conn.publish(ctx, &publishFrame{
		channel:   ch.Name,
		data:      json.RawMessage(`null`),
		members:   members,
		ephemeral: settings.ephemeral,
	})
}

// toMember builds a wire batch member, encrypting data per-member when a cipher is set.
func (ch *Channel) toMember(name string, data any) (wireMember, error) {
	if ch.cipher == nil {
		raw, err := json.Marshal(data)
		if err != nil {
			return wireMember{}, fmt.Errorf("realtime: marshal publish data: %w", err)
		}
		return wireMember{name: name, data: raw}, nil
	}
	encrypted, err := ch.cipher.Encrypt(data)
	if err != nil {
		return wireMember{}, err
	}
	raw, err := json.Marshal(encrypted.Data)
	if err != nil {
		return wireMember{}, fmt.Errorf("realtime: marshal encrypted data: %w", err)
	}
	return wireMember{name: name, data: raw, encoding: encrypted.Encoding}, nil
}

// Flush sends any buffered (auto-batched) publishes now, as a single batch frame. This
// runs automatically once the throttle window elapses, when the buffer is full, and on
// Detach. Call it to force an immediate send. A no-op when nothing is buffered.
func (ch *Channel) Flush() {
	ch.mu.Lock()
	ch.flushLocked()
	ch.mu.Unlock()
}

func (ch *Channel) flushLocked() {
	if ch.batchTimer != nil {
		ch.batchTimer.Stop()
		ch.batchTimer = nil
	}
	if len(ch.batchBuf) == 0 {
		return
	}
	pending := ch.batchBuf
	ch.batchBuf = nil
	ch.lastFlush = time.Now()
	go ch.sendBatch(pending)
}

func (ch *Channel) sendBatch(pending []bufferedPublish) {
	members := make([]wireMember, len(pending))
	for i, entry := range pending {
		members[i] = entry.member
	}
	err := ch.conn.publish(context.Background(), &publishFrame{
		channel: ch.Name,
		data:    json.RawMessage(`null`),
		members: members,
	})
	for _, entry := range pending {
		entry.done <- err
	}
}

// enqueue buffers a member for the next flush, scheduling or forcing a flush as needed,
// and waits for the flush's ack.
func (ch *Channel) enqueue(ctx context.Context, member wireMember) error {
	done := make(chan error, 1)
	ch.mu.Lock()
	ch.batchBuf = append(ch.batchBuf, bufferedPublish{member: member, done: done})
	if len(ch.batchBuf) >= ch.batch.MaxMessages {
		ch.flushLocked()
	} else if ch.batchTimer == nil {
		// Throttle, don't fixed-delay: send right away unless a batch went out within
		// Interval, in which case wait out the rest of the window. Publishes spaced
		// further apart than Interval thus never batch.
		wait := ch.batch.Interval - time.Since(ch.lastFlush)
		if wait < 0 {
			wait = 0
		}
		ch.batchTimer = time.AfterFunc(wait, ch.Flush)
	}
	ch.mu.Unlock()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// HistoryParams are the query params for [Channel.History].
type HistoryParams struct {
	// Limit caps how many messages are returned. The server applies its own cap and a
	// default when zero.
	Limit int
	// Before, a message serial (see [Message.Serial]), pages backward: only messages
	// with a serial strictly below it are returned. Zero means start from the newest.
	Before uint64
}

// HistoryResult is one page of channel history.
type HistoryResult struct {
	// Messages are the matching messages, ordered oldest-first.
	Messages []*Message
	// More is true when older messages remain beyond this page. Pass the oldest
	// message's Serial as [HistoryParams.Before] to fetch them.
	More bool
}

// History fetches recent messages for this channel, oldest first. History is a one-shot
// read and does not interleave with the live subscription. How far back it reaches
// depends on each message's retention, see https://foony.io/docs/history. On a channel
// with a cipher, messages are decrypted before they are returned. It returns the
// server's error when history cannot be read, for example a missing history capability.
func (ch *Channel) History(ctx context.Context, params HistoryParams) (*HistoryResult, error) {
	response, err := ch.conn.requestHistory(ctx, &historyFrame{
		channel: ch.Name,
		limit:   uint64(params.Limit),
		before:  params.Before,
	})
	if err != nil {
		return nil, err
	}
	// Expand any batch frames into their member messages before decrypting.
	var messages []*Message
	for i := range response.messages {
		messages = append(messages, expandFrame(&response.messages[i])...)
	}
	if ch.cipher != nil {
		for i, message := range messages {
			// An undecryptable message (a rotated key, another key's publish) is
			// returned undecoded with Encoding intact rather than failing the page.
			if decrypted, err := decryptMessage(ch.cipher, message); err == nil {
				messages[i] = decrypted
			}
		}
	}
	return &HistoryResult{Messages: messages, More: response.more}, nil
}

// ---- inbound delivery ----

// deliverMessage delivers an inbound frame to subscribers. A server bundle is unwrapped
// into its member frames and re-delivered (a member may itself be a client batch, so
// this recurses one level). A batch frame is expanded into its member messages in
// order. Each is then dispatched like a single message.
func (ch *Channel) deliverMessage(frame *msgFrame) {
	if len(frame.bundle) > 0 {
		for _, member := range frame.bundle {
			widened := bundledToMsg(frame.channel, member)
			ch.deliverMessage(&widened)
		}
		return
	}
	if len(frame.members) > 0 {
		for index := range frame.members {
			ch.deliverSingle(memberMessage(frame, index))
		}
		// The whole batch is one server record with one serial. Ephemeral messages are
		// never resumable, so they must not advance the cursor, because the server would
		// not find them.
		if !frame.ephemeral {
			ch.recordSerial(frame.seq)
		}
		return
	}
	ch.deliverSingle(frameToMessage(frame))
	if !frame.ephemeral {
		ch.recordSerial(frame.seq)
	}
}

// deliverSingle dispatches one message, dropping duplicates and decrypting first when a
// cipher is set. A message whose encoding isn't a cipher encoding passes through
// unchanged, and a failed decrypt (wrong key / tampered) is dropped rather than delivered
// as garbage.
func (ch *Channel) deliverSingle(message *Message) {
	if ch.isDuplicate(message) {
		return
	}
	if ch.cipher != nil && IsCipherEncoding(message.Encoding) {
		decrypted, err := decryptMessage(ch.cipher, message)
		if err != nil {
			log.Printf("[realtime] failed to decrypt message on channel %s: %v", ch.Name, err)
			return
		}
		message = decrypted
	}
	ch.conn.dispatch.enqueue(func() {
		ch.messages.emit(message.Name, message)
	})
}

// isDuplicate reports whether this (clientID, messageID) was already delivered, and
// records unseen keys, evicting the oldest past the cap. Drops duplicates a publisher
// retry can introduce once the server coalesces.
func (ch *Channel) isDuplicate(message *Message) bool {
	key := message.ClientID + "\x00" + message.ID
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if _, ok := ch.seen[key]; ok {
		return true
	}
	ch.seen[key] = struct{}{}
	ch.seenOrder = append(ch.seenOrder, key)
	if len(ch.seenOrder) > dedupCacheMax {
		delete(ch.seen, ch.seenOrder[0])
		ch.seenOrder = ch.seenOrder[1:]
	}
	return false
}

// cursor returns the resume cursor (contiguous serial), or 0 when the channel has none.
func (ch *Channel) cursor() uint64 {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return ch.contiguousSerial
}

// recordSerial tracks the contiguous per-channel serial and detects gaps. Stored order
// equals serial order and the live tail delivers in that order, so the only way a
// serial arrives out of sequence is real loss (a dropped message to a briefly-slow
// consumer). When that happens we trigger a gap-fill fetch, whose ordered replay closes
// the hole.
//
//   - no baseline yet (cursor 0): adopt this serial as the baseline (a fresh subscriber
//     starts now).
//   - next in sequence: advance the cursor.
//   - already covered (<= cursor): a replay or duplicate, ignored (dedup handles the
//     payload).
//   - ahead of sequence (> cursor + 1): a gap. Backfill from the cursor and leave it
//     un-advanced so the replay can close the hole before the cursor moves past it.
func (ch *Channel) recordSerial(seq uint64) {
	if seq == 0 {
		return
	}
	ch.mu.Lock()
	if ch.contiguousSerial == 0 || seq == ch.contiguousSerial+1 {
		ch.contiguousSerial = seq
		ch.mu.Unlock()
		return
	}
	if seq <= ch.contiguousSerial {
		ch.mu.Unlock()
		return
	}
	// A gap. Heal it with a surgical fetch from the cursor, NOT a re-subscribe: the
	// server returns just the messages after the cursor, leaving the live
	// subscription, presence watcher, and retained replay untouched (a re-subscribe
	// would tear all three down for one dropped message). Debounced to one in-flight
	// fetch.
	if ch.backfilling || ch.state != ChannelAttached || ch.contiguousSerial == 0 {
		ch.mu.Unlock()
		return
	}
	ch.backfilling = true
	fromSerial := ch.contiguousSerial
	ch.mu.Unlock()
	go ch.backfill(fromSerial)
}

// backfill runs one gap-fill fetch. The returned messages are applied in order, so the
// cursor walks forward past the gap, and dedup drops any overlap with messages already
// delivered live. If the cursor has aged out of retention the server reports a
// discontinuity (resumed=false), which we surface and re-baseline from rather than
// re-applying.
func (ch *Channel) backfill(fromSerial uint64) {
	response, err := ch.conn.requestFetch(context.Background(), &fetchFrame{channel: ch.Name, fromSerial: fromSerial})
	if err == nil {
		if response.resumed {
			for i := range response.messages {
				ch.deliverMessage(&response.messages[i])
			}
		} else {
			// The gap aged out of retention: the returned messages start above the
			// hole, so applying them would leave the cursor stuck. Re-baseline and
			// surface the discontinuity instead.
			ch.onResumed(false)
		}
	}
	// A failed fetch leaves the gap. The next gapped message retries it.
	ch.mu.Lock()
	ch.backfilling = false
	ch.mu.Unlock()
}

// onResumed finalizes a reconnect re-subscribe: the connection re-issued the sub with
// our resume cursor and the server reported whether the gap was replayed. resumed=false
// is a discontinuity (messages may have been missed beyond retention), which listeners
// can act on by reloading state or reading history.
func (ch *Channel) onResumed(resumed bool) {
	ch.mu.Lock()
	// A discontinuity means the cursor aged out of retention, so the gap can't be
	// filled. Drop the baseline and adopt the next serial we see, otherwise every
	// later message would look gapped and we'd backfill-loop. The attached
	// {Resumed: false} update tells listeners to recover (reload state / read history)
	// themselves.
	if !resumed {
		ch.contiguousSerial = 0
	}
	if ch.state == ChannelAttaching || ch.state == ChannelSuspended || ch.state == ChannelAttached {
		ch.transitionLocked(ChannelAttached, &transitionInfo{resumed: resumed})
	}
	ch.mu.Unlock()
}

// onConnectionState drives the channel state machine from connection lifecycle changes.
func (ch *Channel) onConnectionState(change ConnectionStateChange) {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	switch change.Current {
	case ConnectionDisconnected:
		if ch.state == ChannelAttached {
			reason := change.Reason
			if reason == nil {
				reason = fmt.Errorf("realtime: connection disconnected")
			}
			ch.transitionLocked(ChannelSuspended, &transitionInfo{reason: reason})
		}
	case ConnectionConnected:
		// The connection re-subscribes remembered channels on reconnect and reports
		// the true resume outcome via onResumed, so move to attaching until that ack
		// arrives rather than optimistically claiming a resume that may be a
		// discontinuity.
		if ch.state == ChannelSuspended {
			ch.transitionLocked(ChannelAttaching, nil)
		}
	case ConnectionFailed:
		if ch.isLiveLocked() {
			var info *transitionInfo
			if change.Reason != nil {
				info = &transitionInfo{reason: change.Reason}
			}
			ch.transitionLocked(ChannelFailed, info)
		}
	case ConnectionClosed:
		if ch.isLiveLocked() {
			ch.transitionLocked(ChannelDetached, nil)
		}
	}
}

// isLiveLocked is true while the channel is in an attach-related state worth
// transitioning out of.
func (ch *Channel) isLiveLocked() bool {
	return ch.state != ChannelInitialized && ch.state != ChannelDetached && ch.state != ChannelFailed
}

// transitionLocked moves the state machine and queues the change for listeners. A
// same-state call with info still reaches listeners as a ChannelEventUpdate: a
// re-confirmation that carries information (e.g. a resume outcome while already
// attached) matters, for example a Resumed=false discontinuity.
func (ch *Channel) transitionLocked(next ChannelState, info *transitionInfo) {
	if ch.state == next {
		if info == nil {
			return
		}
		change := ChannelStateChange{Current: next, Previous: next, Reason: info.reason, Resumed: info.resumed}
		ch.conn.dispatch.enqueue(func() {
			ch.states.emit(ChannelEventUpdate, change)
		})
		return
	}
	previous := ch.state
	ch.state = next
	change := ChannelStateChange{Current: next, Previous: previous}
	if info != nil {
		change.Reason = info.reason
		change.Resumed = info.resumed
	}
	ch.conn.dispatch.enqueue(func() {
		ch.states.emit(ChannelEvent(next), change)
	})
}

// ---- frame -> message conversion ----

// frameToMessage converts a single wire frame to its delivered message.
func frameToMessage(frame *msgFrame) *Message {
	return &Message{
		Channel:   frame.channel,
		Name:      frame.name,
		Data:      frame.data,
		Timestamp: int64(frame.timestamp),
		ID:        frame.messageID,
		ClientID:  frame.clientID,
		Encoding:  frame.encoding,
		Serial:    frame.seq,
		Ephemeral: frame.ephemeral,
	}
}

// memberMessage builds a per-member message from a batch frame. The member id is
// "<batchId>:<index>".
func memberMessage(base *msgFrame, index int) *Message {
	member := &base.members[index]
	return &Message{
		Channel:   base.channel,
		Name:      member.name,
		Data:      member.data,
		Timestamp: int64(base.timestamp),
		ID:        fmt.Sprintf("%s:%d", base.messageID, index),
		ClientID:  base.clientID,
		Encoding:  member.encoding,
		Ephemeral: base.ephemeral,
	}
}

// expandFrame expands one history/fetch frame into delivered messages: batch members
// individually, a non-batch frame as itself.
func expandFrame(frame *msgFrame) []*Message {
	if len(frame.members) > 0 {
		messages := make([]*Message, len(frame.members))
		for index := range frame.members {
			messages[index] = memberMessage(frame, index)
		}
		return messages
	}
	return []*Message{frameToMessage(frame)}
}

// decryptMessage returns a copy of message with its encrypted payload decrypted and its
// cipher encoding stripped (the delivered data is now plaintext). Messages without a
// cipher encoding are returned unchanged.
func decryptMessage(cipher *Cipher, message *Message) (*Message, error) {
	if !IsCipherEncoding(message.Encoding) {
		return message, nil
	}
	plaintext, err := cipher.Decrypt(message.Encoding, message.Data)
	if err != nil {
		return nil, err
	}
	decrypted := *message
	decrypted.Data = plaintext
	decrypted.Encoding = ""
	return &decrypted, nil
}

// isCapabilityError is true for a server error that won't change on retry: the
// forbidden / capability / channel-denied family (403xx). A failed attach with such an
// error is terminal. Any other failure (e.g. a dropped connection) is transient and
// recovers on reconnect.
func isCapabilityError(err error) bool {
	var serverErr *ServerError
	if !errors.As(err, &serverErr) {
		return false
	}
	return serverErr.Code >= 40300 && serverErr.Code < 40400
}
