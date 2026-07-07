# realtime-go

Go SDK for the Foony Realtime service. A small client for the wire protocol implemented
by `services/realtime-saas`: connect, subscribe, publish, presence, and history, plus a
REST client for backends. Feature parity with
[`@foony/realtime`](https://github.com/Foony-Limited/realtime-js) (the TypeScript SDK).

## Install

```bash
go get github.com/Foony-Limited/realtime-go
```

The module's only dependency is `github.com/coder/websocket`.

## Quick start

```go
import (
	"context"
	"fmt"

	realtime "github.com/Foony-Limited/realtime-go"
)

client, err := realtime.New(realtime.Options{
	// Prefer AuthCallback over Key for apps distributed to users.
	AuthCallback: func(ctx context.Context) (string, error) {
		return fetchTokenFromYourServer(ctx)
	},
})
if err != nil {
	return err
}
defer client.Close()

channel := client.Channels.Get("chat:room-1")

channel.Subscribe(func(message *realtime.Message) {
	fmt.Println("chat message:", string(message.Data))
})

err = channel.Publish(ctx, "chat", map[string]string{"text": "hello world"})

channel.Presence.Subscribe(func(event *realtime.PresenceEvent) {
	fmt.Println(event.Action, event.ClientID, string(event.Data))
})
err = channel.Presence.Enter(ctx, map[string]string{"name": "Alice"})
```

On a trusted server you can pass `Key` directly instead of `AuthCallback`. Your backend
mints browser/client JWTs locally with `realtime.CreateJWT` (it signs with your Realtime
API key, with no network call), or asks the service to mint one with
`rest.Auth.RequestToken`. Either way the API key stays on your backend.

## Local development against the realtime backend

Start the backend following `services/realtime-saas/README.md`. Then mint a dev token:

```bash
cd services/realtime-saas
JWT_SIGNING_KEY=local-dev-key go run ./cmd/devtoken -app foony -client alice
```

Use the printed token in the SDK:

```go
client, err := realtime.New(realtime.Options{
	Endpoint: "ws://localhost:3000",
	Token:    os.Getenv("FOONY_REALTIME_DEV_TOKEN"),
})
```

Omit `Endpoint` in production to use `wss://realtime.foony.io`.

## Channel names

Channel names are 1 to 255 ASCII characters from `A-Z a-z 0-9 : - _` and cannot start
with a `:`. Use colons to express hierarchy (`chat:rooms:42`). Dots are not allowed.
`Channels.Get` panics on an invalid name (and the server rejects one with error code
`40001`, `CodeBadFrame`).

## API surface

- `realtime.New(options)` — top-level `Client`. Owns the WebSocket; channels attach
  lazily.
- `client.Channels.Get(name)` — returns a stable `*Channel` for that name.
- `channel.Subscribe(fn)` — message listener. Returns an unsubscribe func.
- `channel.Subscribe(fn, "greeting")` — message listener for specific message names.
- `channel.On(fn)` / `channel.OnEvent(realtime.ChannelEvent(realtime.ChannelAttached), fn)` —
  channel lifecycle state listeners.
- `channel.Publish(ctx, name, data)` — publish one message. Returns on ack. Payloads are
  marshaled with `encoding/json`; delivered `Message.Data` is a `json.RawMessage`.
- `channel.PublishBatch(ctx, messages)` — publish an atomic batch (stored, deduped, and
  billed as one message).
- `channel.History(ctx, params)` — recent messages, with serial-cursor paging.
- `channel.Presence.Subscribe(fn)` / `channel.Presence.On(action, fn)` — presence
  listeners.
- `channel.Presence.Enter/Update/Leave(ctx, data)` — mutate this connection's
  membership.
- `client.Connection.On(fn)` / `client.Connection.OnState(state, fn)` /
  `client.Connection.Once(state, fn)` — observe connection state.
- `client.BatchPublish(ctx, specs...)` — publish to up to 100 channels in one call.
- `realtime.CreateJWT(key, params)` — mint a capability-scoped JWT locally.
- `realtime.NewCipher` + `realtime.WithCipher` — end-to-end encryption (AES-GCM); the
  edge only ever sees ciphertext.
- `realtime.NewRest(options)` — HTTP client for publish, history, presence, and token
  minting without a connection (see [REST](#rest)).

Errors from the service are `*realtime.ServerError` values carrying the numeric protocol
code:

```go
var serverErr *realtime.ServerError
if errors.As(err, &serverErr) && serverErr.Code == realtime.CodeCapability {
	// the token does not grant this action
}
```

## REST

For backends and integrations that publish or read without holding a connection open
(cron jobs, serverless functions, webhooks), use the `Rest` client. It talks to the same
service over HTTPS, and its publishes are identical to WebSocket publishes for
subscribers, history, and billing.

```go
rest, err := realtime.NewRest(realtime.RestOptions{Key: os.Getenv("REALTIME_API_KEY")})
channel := rest.Channels.Get("chat:room:42")

// Publish one message, or several (stored and delivered as one atomic batch).
_, err = channel.Publish(ctx, "greeting", map[string]string{"text": "hello"})

// History, newest first. Page through older messages with Next.
page, err := channel.History(ctx, realtime.RestHistoryParams{Limit: 100})
for _, message := range page.Items {
	fmt.Println(message.Name, string(message.Data))
}
for page.HasNext() {
	page, err = page.Next(ctx)
}

// Current presence members.
members, err := channel.Presence.Get(ctx, realtime.RestPresenceParams{})

// Mint a client JWT from your API key (for handing to browser clients).
details, err := rest.Auth.RequestToken(ctx, realtime.TokenParams{
	ClientID:   "user-42",
	Capability: realtime.Capability{"chat:*": {"subscribe", "publish"}},
})
```

Auth accepts the same options as the realtime client: `Key` (server-side), `Token`, or
`AuthCallback` (refreshed automatically when the service reports it expired). Channels
accept the same `WithCipher` option for end-to-end encryption. Errors are
`*realtime.RestError` values carrying the numeric protocol `Code` plus the HTTP
`StatusCode`.

## Reconnect

When the connection drops unexpectedly the client retries with exponential backoff (1s,
2s, 4s, ..., capped at 30s). Everything is restored automatically on reconnect:
subscriptions are re-issued (with a resume cursor, so missed messages within retention
are replayed), presence watchers are re-opened, and whatever presence membership this
connection had entered is re-entered. Call `Presence.Leave` if you no longer want to be
present.

Set `DisableAutoReconnect: true` to disable retries entirely (useful in tests).

Publishes made while the connection is establishing or temporarily down are queued
locally and flushed on the next successful (re)connect, so a publish during a brief blip
returns nil rather than an error. A publish that was already in flight when the
connection dropped is resent on reconnect too. Every publish carries a stable
client-assigned id, so the server collapses any duplicate that a resend would otherwise
create (exactly-once). Set `DisableQueueing: true` to disable buffering/resend and fail
such publishes immediately.

## Concurrency

All exported methods are safe for concurrent use. Listeners run one at a time, in event
order, on a dispatcher goroutine owned by the client, so a listener may call blocking
SDK methods (like `Publish`) without deadlocking message delivery. Blocking calls take a
`context.Context`; canceling it abandons the caller's wait, not the underlying
operation.

## Tests

```bash
go test ./...
```

Runs wire golden tests (shared byte vectors with the TypeScript SDK and the server, so
the codecs cannot drift) plus in-process end-to-end tests that drive the SDK against a
fake edge over a real WebSocket. No external services required.

## License

[Apache-2.0](./LICENSE) © Foony Limited
