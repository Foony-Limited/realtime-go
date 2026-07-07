# Changelog

All notable changes to `realtime-go` are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## 0.1.1

### Fixed

- A lost message arriving in the same instant as the channel's attach
  confirmation could skip the automatic gap heal, leaving the gap unfilled until
  the next lost message. Gap healing now also runs while the channel is still
  attaching.

## 0.1.0

### Added

- Initial release, at feature parity with `@foony/realtime` 0.14.0 (the TypeScript
  SDK).
- `Client` WebSocket client: lazy connect, auto-reconnect with exponential backoff,
  keep-alive pings with dead-link detection, and connection-id resume so presence
  membership survives brief drops.
- Channels with `Subscribe` (all messages or by name), `Publish` with always-on
  auto-batching, atomic `PublishBatch`, `History` with serial-cursor paging, and
  automatic gap healing (a lost message triggers a surgical fetch, not a re-subscribe).
- Offline publish queueing: publishes made while disconnected are queued and resent on
  reconnect under a stable message id (exactly-once). Disable with `DisableQueueing`.
- Presence: `Enter` / `Update` / `Leave`, listener-driven server watchers, and
  automatic re-entry after a reconnect.
- End-to-end encryption (`NewCipher`, `WithCipher`, `GenerateRandomKey`): AES-GCM
  payloads the Foony backend cannot read, compatible with the TypeScript SDK's cipher.
- `CreateJWT` for local, capability-scoped token minting from an API key.
- `Rest` client for backends: publish, paged history, presence snapshots,
  `RequestToken`, and `Time`, with automatic token refresh on 401 when using
  `AuthCallback`.
- Cross-language golden tests pinning the binary wire codec to the same byte vectors
  the TypeScript SDK and the server use.
