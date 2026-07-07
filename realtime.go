// Package realtime is the Go SDK for the Foony Realtime service: the WebSocket
// [Client] (a Channels.Get(name) map with per-channel Subscribe / Publish / Presence),
// the request/response [Rest] client for backends, [CreateJWT] for server-side token
// minting, and the [Cipher] helpers for end-to-end encryption.
package realtime

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

// Limits on one BatchPublish call, matching the service's.
const (
	maxBatchChannels = 100
	maxBatchMessages = 1000
)

// channelSettings collects the per-channel options.
type channelSettings struct {
	cipher *Cipher
	batch  *BatchOptions
}

// ChannelOption customizes a channel on its first Channels.Get.
type ChannelOption func(*channelSettings)

// WithCipher enables end-to-end payload encryption on the channel with the given
// [Cipher]. This prevents the Foony backend from seeing the plaintext data of messages
// published to this channel. The cipher key should be kept private and never shared
// with the public or our backend.
func WithCipher(cipher *Cipher) ChannelOption {
	return func(settings *channelSettings) {
		settings.cipher = cipher
	}
}

// WithBatchOptions tunes the channel's auto-batching, overriding the Options.Batch
// default.
func WithBatchOptions(batch BatchOptions) ChannelOption {
	return func(settings *channelSettings) {
		settings.batch = &batch
	}
}

// BatchSpec is one batch-publish spec: send Messages to each of Channels.
type BatchSpec struct {
	// Channels are the channel names to which Messages will be published.
	Channels []string
	// Messages are published to every channel in Channels.
	Messages []BatchMessage
}

// BatchChannelResult is one channel's outcome from [Client.BatchPublish].
type BatchChannelResult struct {
	// Channel is the channel this result is for.
	Channel string
	// Err is set when this channel's publish failed.
	Err error
}

// BatchPublishResult is the per-channel result set from [Client.BatchPublish].
type BatchPublishResult struct {
	// SuccessCount is the number of channels published successfully.
	SuccessCount int
	// FailureCount is the number of channels that failed to publish.
	FailureCount int
	// Results has one entry per channel. Err is set when that channel's publish
	// failed.
	Results []BatchChannelResult
}

// Client is the realtime client and the entry point for app code. It owns one WebSocket
// [Connection] (opened lazily on first use) and a map of [Channel] instances retrieved
// via Channels.Get(name). See https://foony.io/docs/getting-started for the auth
// options and a full walkthrough.
//
//	// Prefer AuthCallback over Key for apps distributed to users.
//	client, err := realtime.New(realtime.Options{
//		AuthCallback: func(ctx context.Context) (string, error) {
//			return fetchTokenFromYourServer(ctx)
//		},
//	})
//	channel := client.Channels.Get("chat:room-1")
//	channel.Subscribe(func(message *realtime.Message) {
//		fmt.Println(message.Name, string(message.Data))
//	})
//	err = channel.Publish(ctx, "greeting", map[string]string{"text": "hi"})
type Client struct {
	// Connection is the underlying transport. Listen on lifecycle state with
	// Connection.On and read it with Connection.State.
	Connection *Connection
	// Auth is the token-minting namespace. It signs with the client's key. See
	// https://foony.io/docs/auth for when to mint tokens yourself.
	Auth *Auth
	// Channels is the map-like accessor for channels: a stable instance per name.
	Channels *Channels
}

// Channels is the channel registry of one [Client]: Get returns the stable instance
// for a name, Release removes it.
type Channels struct {
	conn         *Connection
	batchDefault *BatchOptions

	mu     sync.Mutex
	byName map[string]*Channel
}

// New builds a [Client] from options. Exactly one of Options.Key, Options.Token, or
// Options.AuthCallback must be set, or New returns an error. The connection is opened
// lazily on first use. Call [Client.Connect] to open it eagerly.
func New(options Options) (*Client, error) {
	conn, err := newConnection(options)
	if err != nil {
		return nil, err
	}
	client := &Client{
		Connection: conn,
		Auth:       &Auth{resolveKey: func() string { return options.Key }},
		Channels: &Channels{
			conn:         conn,
			batchDefault: options.Batch,
			byName:       make(map[string]*Channel),
		},
	}
	return client, nil
}

// Connect eagerly opens the WebSocket and completes the auth handshake. Calling this is
// optional: channels connect and attach lazily on first use. Connect is idempotent, and
// concurrent calls wait on the same in-flight attempt. It returns nil once the
// connection is connected, and the handshake error when auth fails (for example a bad
// key, or an expired static Token with no AuthCallback to re-mint one).
func (c *Client) Connect(ctx context.Context) error {
	return c.Connection.Connect(ctx)
}

// Close closes the WebSocket and releases every channel. The connection is closed when
// it returns. Publishes still awaiting an ack fail with a "connection closed" error.
func (c *Client) Close() {
	c.Channels.mu.Lock()
	names := make([]string, 0, len(c.Channels.byName))
	for name := range c.Channels.byName {
		names = append(names, name)
	}
	c.Channels.mu.Unlock()
	for _, name := range names {
		c.Channels.Release(name)
	}
	c.Connection.Close()
}

// BatchPublish publishes messages to many channels in one call. Each [BatchSpec] sends
// its Messages to each of its Channels, and all messages to a single channel go as one
// idempotent batch frame. BatchPublish is publish-only and does not handle end-to-end
// encryption. If you need that, use [Channel.Publish] (which is the preferred method of
// publishing all messages).
//
// A BatchPublish is limited to at most 100 distinct channels per call and at most 1000
// messages per channel. It returns an error before sending anything when a limit is
// exceeded. Otherwise it returns a [BatchPublishResult]: a channel that fails shows up
// there as an Err entry rather than failing the call, so one channel failing does not
// fail the others.
func (c *Client) BatchPublish(ctx context.Context, specs ...BatchSpec) (*BatchPublishResult, error) {
	// Merge the specs per channel first: several specs naming the same channel merge
	// into one batch, and that merged batch is what must fit the limits.
	perChannel := make(map[string][]BatchMessage)
	var order []string
	for _, spec := range specs {
		for _, channel := range spec.Channels {
			if _, ok := perChannel[channel]; !ok {
				order = append(order, channel)
			}
			perChannel[channel] = append(perChannel[channel], spec.Messages...)
		}
	}
	if len(perChannel) > maxBatchChannels {
		return nil, fmt.Errorf("realtime: BatchPublish: at most %d channels per request", maxBatchChannels)
	}
	for channel, messages := range perChannel {
		if len(messages) > maxBatchMessages {
			return nil, fmt.Errorf("realtime: BatchPublish: at most %d messages per channel per request (channel %q has %d)", maxBatchMessages, channel, len(messages))
		}
	}

	results := make([]BatchChannelResult, len(order))
	var wg sync.WaitGroup
	for i, channel := range order {
		wg.Add(1)
		go func(i int, channel string, messages []BatchMessage) {
			defer wg.Done()
			members := make([]wireMember, 0, len(messages))
			var err error
			for _, message := range messages {
				raw, marshalErr := json.Marshal(message.Data)
				if marshalErr != nil {
					err = fmt.Errorf("realtime: marshal publish data: %w", marshalErr)
					break
				}
				members = append(members, wireMember{name: message.Name, data: raw})
			}
			if err == nil {
				err = c.Connection.publish(ctx, &publishFrame{
					channel: channel,
					data:    json.RawMessage(`null`),
					members: members,
				})
			}
			results[i] = BatchChannelResult{Channel: channel, Err: err}
		}(i, channel, perChannel[channel])
	}
	wg.Wait()
	result := &BatchPublishResult{Results: results}
	for _, entry := range results {
		if entry.Err != nil {
			result.FailureCount++
		} else {
			result.SuccessCount++
		}
	}
	return result, nil
}

// Get returns the [Channel] named name, creating it on first use. The same name always
// returns the same instance. name is 1 to 255 characters from "A-Z a-z 0-9 : - _" and
// may not start with a ':'. Colons express hierarchy (e.g. "chat:rooms:42"), dots are
// not allowed. Get panics on an invalid name: the server's grammar is enforced
// client-side so a bad name fails loudly here instead of attach-looping against
// BadFrame rejections.
//
// Options (e.g. [WithCipher]) apply when the channel is first created. Passing
// different options to a later Get of the same name returns the existing instance
// unchanged.
func (chans *Channels) Get(name string, options ...ChannelOption) *Channel {
	if !validChannelName(name) {
		panic(fmt.Sprintf("realtime: Channels.Get: invalid channel name %q (allowed: A-Z a-z 0-9 : - _, at most 255 characters, not starting with ':')", name))
	}
	chans.mu.Lock()
	defer chans.mu.Unlock()
	if existing, ok := chans.byName[name]; ok {
		return existing
	}
	var settings channelSettings
	for _, option := range options {
		option(&settings)
	}
	batch := settings.batch
	if batch == nil {
		batch = chans.batchDefault
	}
	channel := newChannel(chans.conn, name, settings.cipher, batch)
	chans.byName[name] = channel
	return channel
}

// Release releases the channel named name. The channel is detached and removed from the
// client, so a later Get of the same name returns a fresh instance. A no-op when no
// channel with that name exists.
func (chans *Channels) Release(name string) {
	chans.mu.Lock()
	channel, ok := chans.byName[name]
	if ok {
		delete(chans.byName, name)
	}
	chans.mu.Unlock()
	if !ok {
		return
	}
	chans.conn.unregisterChannel(name)
	// Remove the channel's connection state listener, or every released instance would
	// be retained (and keep running its state machine) for the life of the client.
	channel.dispose()
	go func() { _ = channel.Detach(context.Background()) }()
}
