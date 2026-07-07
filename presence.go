package realtime

// Presence for one channel: announce this connection with Enter / Update / Leave, and
// listen on who comes and goes with On / Subscribe.

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
)

// PresenceEvent is one presence transition delivered to presence listeners.
type PresenceEvent struct {
	// Channel is the channel the presence transition occurred on.
	Channel string
	// Action says which transition occurred (enter, leave, or update).
	Action PresenceAction
	// ClientID is the client id of the member whose presence changed.
	ClientID string
	// ConnectionID is the connection id of the member whose presence changed.
	ConnectionID string
	// Data is the presence payload supplied on enter/update, if any (decrypted on a
	// channel with a cipher).
	Data json.RawMessage
	// Encoding says how Data is encoded when it could not be decrypted. Empty for
	// plain JSON.
	Encoding string
	// Timestamp is the transition time in milliseconds since the Unix epoch.
	Timestamp int64
}

// enteredState is what this connection has entered into the presence set. A pointer
// rather than the raw value so "entered with no data" is distinct from "not entered".
type enteredState struct {
	data any
}

// Presence is the presence surface for one channel. Announce this connection with
// [Presence.Enter] / [Presence.Update] / [Presence.Leave], and listen on who comes and
// goes with [Presence.On]. Reached via [Channel.Presence]. See
// https://foony.io/docs/presence for the full model.
//
//	channel.Presence.On(realtime.PresenceEnter, func(member *realtime.PresenceEvent) {
//		fmt.Println(member.ClientID, "joined")
//	})
//	err := channel.Presence.Enter(ctx, map[string]string{"status": "online"})
type Presence struct {
	conn        *Connection
	channelName string
	channel     *Channel
	cipher      *Cipher
	events      *emitter[PresenceAction, *PresenceEvent]

	mu sync.Mutex
	// watching is true once we have asked the server for presence events on this
	// channel. Set when the first presence listener is added and cleared when the last
	// leaves, so a channel used only for messages never opens a presence watcher.
	watching bool
	// entered is what this connection has entered into the presence set, or nil if
	// not present. Kept so the SDK can re-enter automatically after a reconnect.
	entered *enteredState
}

func newPresence(conn *Connection, channelName string, channel *Channel, cipher *Cipher) *Presence {
	presence := &Presence{
		conn:        conn,
		channelName: channelName,
		channel:     channel,
		cipher:      cipher,
		events:      newEmitter[PresenceAction, *PresenceEvent](),
	}
	// The watcher follows the listener count: every removal path (an unsubscribe
	// function, Off, a one-shot firing) funnels through this hook, so the presence
	// subscription is dropped as soon as the last listener leaves.
	presence.events.onRemoved = presence.maybeStopWatching
	return presence
}

// Subscribe registers a listener for every presence event and returns its unsubscribe
// function. Adding the first listener asks the server for presence on this channel: an
// initial member snapshot, then live transitions. This is independent of a message
// Subscribe, so a channel used only for messages never opens a presence watcher, and
// the watcher is dropped again when the last presence listener is removed.
func (p *Presence) Subscribe(listener func(*PresenceEvent)) func() {
	off := p.events.on(listener)
	p.ensureWatching()
	return off
}

// On registers a listener for presence events with a matching action and returns its
// unsubscribe function. Like [Presence.Subscribe], the first listener opens the
// server-side presence watcher.
func (p *Presence) On(action PresenceAction, listener func(*PresenceEvent)) func() {
	off := p.events.onEvent(action, listener, false)
	p.ensureWatching()
	return off
}

// Once registers a listener invoked one time for the next presence event with a
// matching action, and returns its unsubscribe function.
func (p *Presence) Once(action PresenceAction, listener func(*PresenceEvent)) func() {
	off := p.events.onEvent(action, listener, true)
	p.ensureWatching()
	return off
}

// Off removes every presence listener, dropping the server-side watcher.
func (p *Presence) Off() {
	p.events.offAll()
}

// Enter announces this connection as present on the channel, with optional data (a
// display name, a status) shown to other members. Pass nil for no data. It implicitly
// attaches the channel. The membership is remembered, and the SDK re-enters it
// automatically after a reconnect. It returns nil once the server acks the entry, and
// the server's error when the token lacks the presence capability.
func (p *Presence) Enter(ctx context.Context, data any) error {
	p.mu.Lock()
	p.entered = &enteredState{data: data}
	p.mu.Unlock()
	return p.send(ctx, PresenceEnter, data)
}

// Update replaces the data on this connection's presence entry. Other members receive
// an update event. It returns nil once the server acks the update, and the server's
// error when the token lacks the presence capability.
func (p *Presence) Update(ctx context.Context, data any) error {
	p.mu.Lock()
	p.entered = &enteredState{data: data}
	p.mu.Unlock()
	return p.send(ctx, PresenceUpdate, data)
}

// Leave removes this connection's presence entry and stops the automatic re-entry on
// reconnect. It returns nil once the server acks the leave, and the request's error
// when it fails.
func (p *Presence) Leave(ctx context.Context) error {
	p.mu.Lock()
	p.entered = nil
	p.mu.Unlock()
	return p.send(ctx, PresenceLeave, nil)
}

// ensureWatching asks the server to start sending presence events on this channel,
// once. This is idempotent and remembered so it is re-sent on reconnect. A capability
// denial gives up (the permission will not change on retry).
func (p *Presence) ensureWatching() {
	p.mu.Lock()
	if p.watching {
		p.mu.Unlock()
		return
	}
	p.watching = true
	p.mu.Unlock()
	p.conn.rememberPresence(p.channelName)
	go func() {
		_, err := p.conn.request(context.Background(), func(id uint64) any {
			return &presenceSubscribeFrame{channel: p.channelName, id: id}
		})
		if err != nil && isCapabilityError(err) && !p.events.hasAny() {
			p.mu.Lock()
			p.watching = false
			p.mu.Unlock()
			p.conn.forgetPresence(p.channelName)
		}
	}()
}

// maybeStopWatching drops the presence watcher once no presence listeners remain, to
// keep idle channels free.
func (p *Presence) maybeStopWatching() {
	p.mu.Lock()
	if !p.watching || p.events.hasAny() {
		p.mu.Unlock()
		return
	}
	p.watching = false
	p.mu.Unlock()
	p.conn.forgetPresence(p.channelName)
	go func() {
		_, _ = p.conn.request(context.Background(), func(id uint64) any {
			return &presenceUnsubscribeFrame{channel: p.channelName, id: id}
		})
	}()
}

// reenterOnReconnect re-announces this connection's presence after a reconnect:
// re-enter whatever was entered. Presence watching is restored separately by the
// connection re-sending its presence subscriptions.
func (p *Presence) reenterOnReconnect() {
	p.mu.Lock()
	entered := p.entered
	p.mu.Unlock()
	if entered != nil {
		go func() { _ = p.send(context.Background(), PresenceEnter, entered.data) }()
	}
}

// onDetached forgets presence state when the channel detaches (the server's unsub
// closed the watcher).
func (p *Presence) onDetached() {
	p.mu.Lock()
	p.watching = false
	p.entered = nil
	p.mu.Unlock()
	p.conn.forgetPresence(p.channelName)
}

// emitPresence dispatches a presence frame from the Connection transport, decrypting
// its data first when a cipher is set. A failed decrypt is dropped rather than
// delivered as garbage.
func (p *Presence) emitPresence(frame *presenceEventFrame) {
	event := &PresenceEvent{
		Channel:      frame.channel,
		Action:       frame.action,
		ClientID:     frame.clientID,
		ConnectionID: frame.connectionID,
		Data:         frame.data,
		Encoding:     frame.encoding,
		Timestamp:    int64(frame.timestamp),
	}
	if p.cipher != nil && IsCipherEncoding(event.Encoding) {
		plaintext, err := p.cipher.Decrypt(event.Encoding, event.Data)
		if err != nil {
			log.Printf("[realtime] failed to decrypt presence on channel %s: %v", p.channelName, err)
			return
		}
		event.Data = plaintext
		event.Encoding = ""
	}
	p.conn.dispatch.enqueue(func() {
		p.events.emit(event.Action, event)
	})
}

// send applies one presence transition, attaching the channel first and encrypting the
// payload so the edge only sees ciphertext (matching messages).
func (p *Presence) send(ctx context.Context, action PresenceAction, data any) error {
	if err := p.channel.Attach(ctx); err != nil {
		return err
	}
	var payload json.RawMessage
	var encoding string
	if data != nil {
		if p.cipher != nil {
			encrypted, err := p.cipher.Encrypt(data)
			if err != nil {
				return err
			}
			raw, err := json.Marshal(encrypted.Data)
			if err != nil {
				return fmt.Errorf("realtime: marshal encrypted presence data: %w", err)
			}
			payload = raw
			encoding = encrypted.Encoding
		} else {
			raw, err := json.Marshal(data)
			if err != nil {
				return fmt.Errorf("realtime: marshal presence data: %w", err)
			}
			payload = raw
		}
	}
	_, err := p.conn.request(ctx, func(id uint64) any {
		return &presenceFrame{channel: p.channelName, action: action, data: payload, encoding: encoding, id: id}
	})
	return err
}
