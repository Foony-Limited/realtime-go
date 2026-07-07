package realtime

// REST client for Foony Realtime: request/response access to the same service the
// WebSocket Client talks to. Use it from backends and integrations that publish or read
// without holding a connection open (cron jobs, serverless functions, webhooks).
//
// Publishes made here are indistinguishable from WebSocket publishes to subscribers,
// history, and billing. Channel encryption works the same way: pass the shared cipher
// to Channels.Get and payloads are encrypted before they leave the process.

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// RestOptions configures a [Rest] client. One of Key, Token, or AuthCallback is
// required.
type RestOptions struct {
	// Endpoint is the service host or an absolute http(s) URL. Defaults to
	// "realtime.foony.io", which resolves to https://realtime.foony.io.
	Endpoint string
	// Key is a Realtime API key in "appSlug.publicKeyId:privateKey" form. This is the
	// preferred (and simplest) auth method for server-side callers. The key is a
	// long-lived secret, so keep it server-side.
	Key string
	// Token is a static JWT, sent as a Bearer token. Mutually exclusive with
	// AuthCallback.
	Token string
	// AuthCallback returns a fresh JWT. Called before the first request and again
	// whenever the service reports the current token expired.
	AuthCallback func(ctx context.Context) (string, error)
	// ClientID is the default clientId stamped on published messages that don't set
	// their own. Only useful with key auth, to attribute a backend's publishes to a
	// user. When omitted, the service attributes each publish to the authenticated
	// identity, so token-auth callers never need to set this: the token's clientId
	// applies automatically, and a differing value is rejected.
	ClientID string
	// HTTPClient overrides the HTTP client. Mostly useful in tests. Defaults to
	// http.DefaultClient.
	HTTPClient *http.Client
}

// RestPublishMessage is one message to publish over REST.
type RestPublishMessage struct {
	// Name is the application-level event name.
	Name string `json:"name"`
	// Data is the JSON-serializable payload.
	Data any `json:"data"`
	// ClientID attributes the message to a clientId. Only useful with key auth, which
	// may name any user. Token auth needs no value here (the token's clientId applies
	// automatically) and anything else is rejected.
	ClientID string `json:"clientId,omitempty"`
	// ID is a stable id reused across resends so the server can drop duplicates of
	// the same publish. Single-message publishes only.
	ID string `json:"id,omitempty"`
	// Ephemeral marks the message fire-and-forget: delivered live but excluded from
	// history and resume.
	Ephemeral bool `json:"ephemeral,omitempty"`
}

// PublishResult is the result of a successful REST publish.
type PublishResult struct {
	// MessageID is the server-assigned (or echoed) message id. A batch publish shares
	// one id.
	MessageID string `json:"messageId"`
	// Serial is the contiguous per-channel serial for durable publishes. Zero for
	// ephemeral ones.
	Serial uint64 `json:"serial"`
}

// RestMessage is one message returned from [RestChannel.History].
type RestMessage struct {
	// ID is the message id. Batch members share their batch's id.
	ID string `json:"id"`
	// Name is the application-level event name.
	Name string `json:"name"`
	// Data is the payload (decrypted when the channel has a cipher).
	Data json.RawMessage `json:"data"`
	// Timestamp is the publish time, in ms since the Unix epoch.
	Timestamp int64 `json:"timestamp"`
	// ClientID is the publisher's clientId.
	ClientID string `json:"clientId"`
	// Encoding is the remaining payload encoding, e.g. a cipher tag when no cipher is
	// configured.
	Encoding string `json:"encoding"`
	// Serial is the contiguous per-channel serial (zero for unsequenced messages).
	Serial uint64 `json:"serial"`
}

// PresenceMember is one current member returned from [RestPresence.Get].
type PresenceMember struct {
	// ClientID is the member's clientId.
	ClientID string `json:"clientId"`
	// ConnectionID is the member's connection id (one clientId may hold several).
	ConnectionID string `json:"connectionId"`
	// Action is always "present" in a snapshot.
	Action string `json:"action"`
	// Data is the presence payload (decrypted when the channel has a cipher).
	Data json.RawMessage `json:"data"`
	// Encoding is the remaining payload encoding when the data could not be decoded.
	Encoding string `json:"encoding"`
	// Timestamp is when the member last entered or updated, in ms since the Unix
	// epoch.
	Timestamp int64 `json:"timestamp"`
}

// RestHistoryParams are the query params for [RestChannel.History].
type RestHistoryParams struct {
	// Limit is the page size. The service default is 100 when zero.
	Limit int
	// Before is an exclusive serial cursor: return only messages with a serial
	// strictly below it. Pass the oldest Serial you already have (see
	// [RestMessage.Serial]) to page backward.
	Before uint64
	// Direction is "backwards" (newest first, the default) or "forwards" (oldest
	// first).
	Direction string
}

// RestPresenceParams are the query params for [RestPresence.Get].
type RestPresenceParams struct {
	// Limit caps the number of members returned.
	Limit int
	// ClientID returns only members with this clientId.
	ClientID string
	// ConnectionID returns only the member on this connection.
	ConnectionID string
}

// TokenParams are the params for [RestAuth.RequestToken].
type TokenParams struct {
	// ClientID is the clientId the token authenticates as. Required.
	ClientID string
	// TTL is the token lifetime. Defaults to one hour when zero, and the service caps
	// it at 24 hours.
	TTL time.Duration
	// Capability is the capability to grant. It must be a subset of the key's own
	// capability, which is also the default when nil.
	Capability Capability
}

// TokenDetails is a service-issued token plus the metadata needed to cache it until
// expiry.
type TokenDetails struct {
	// Token is the signed JWT to authenticate WebSocket or REST calls with.
	Token string `json:"token"`
	// KeyName is the name of the key that requested it, "appSlug.publicKeyId".
	KeyName string `json:"keyName"`
	// Issued is the issue time, in ms since the Unix epoch.
	Issued int64 `json:"issued"`
	// Expires is the expiry time, in ms since the Unix epoch.
	Expires int64 `json:"expires"`
	// ClientID is the clientId the token authenticates as.
	ClientID string `json:"clientId"`
	// Capability is the granted capability as a JSON string.
	Capability string `json:"capability"`
}

// RestError is the error for any non-2xx REST response. Check it with errors.As.
type RestError struct {
	// Code is the machine-readable protocol code (the same table as the Code*
	// constants).
	Code int
	// StatusCode is the HTTP status of the response.
	StatusCode int
	// Message is a human-readable error description.
	Message string
}

// Error formats the error with its protocol code and HTTP status.
func (e *RestError) Error() string {
	return fmt.Sprintf("realtime: rest error %d (http %d): %s", e.Code, e.StatusCode, e.Message)
}

// PaginatedResult is one page of a paginated response. Items is the current page, and
// Next fetches the following page (older messages for a newest-first history).
type PaginatedResult[T any] struct {
	// Items are the items on this page.
	Items []T

	nextPath string
	load     func(ctx context.Context, path string) (*PaginatedResult[T], error)
}

// HasNext reports whether another page exists.
func (p *PaginatedResult[T]) HasNext() bool {
	return p.nextPath != ""
}

// IsLast reports whether this is the final page.
func (p *PaginatedResult[T]) IsLast() bool {
	return p.nextPath == ""
}

// Next fetches the next page. It returns nil, nil when this is the last one, and a
// [RestError] when the fetch fails.
func (p *PaginatedResult[T]) Next(ctx context.Context) (*PaginatedResult[T], error) {
	if p.nextPath == "" {
		return nil, nil
	}
	return p.load(ctx, p.nextPath)
}

// Rest is the REST client. Construct it with [NewRest] and use Channels.Get(name) for
// publish, history, and presence reads. Use the WebSocket [Client] instead when you
// need to receive live messages.
//
//	// Server-side: an API key is the simplest auth method here.
//	rest, err := realtime.NewRest(realtime.RestOptions{Key: os.Getenv("REALTIME_API_KEY")})
//	channel := rest.Channels.Get("chat:room-1")
//	_, err = channel.Publish(ctx, "greeting", map[string]string{"text": "hi"})
//	page, err := channel.History(ctx, realtime.RestHistoryParams{Limit: 10})
//	fmt.Println(page.Items)
type Rest struct {
	// Auth mints tokens against the service, authenticated by this client's key.
	Auth *RestAuth
	// Channels is the map-like accessor for channels: a stable instance per name.
	Channels *RestChannels

	options RestOptions
	baseURL string

	mu sync.Mutex
	// cachedToken is the JWT from AuthCallback, replaced when the service reports it
	// expired.
	cachedToken string
}

// RestChannels is the channel registry of one [Rest] client.
type RestChannels struct {
	rest *Rest

	mu     sync.Mutex
	byName map[string]*RestChannel
}

// NewRest builds a [Rest] client. It returns an error unless one of RestOptions.Key,
// RestOptions.Token, or RestOptions.AuthCallback is set.
func NewRest(options RestOptions) (*Rest, error) {
	if options.Key == "" && options.Token == "" && options.AuthCallback == nil {
		return nil, errors.New("realtime: NewRest: one of Key, Token, or AuthCallback is required")
	}
	rest := &Rest{
		options: options,
		baseURL: endpointToHTTPURL(options.Endpoint),
	}
	rest.Auth = &RestAuth{rest: rest}
	rest.Channels = &RestChannels{rest: rest, byName: make(map[string]*RestChannel)}
	return rest, nil
}

// Time fetches the current service time, in ms since the Unix epoch. Useful for
// measuring clock skew. It returns a [RestError] when the request fails.
func (r *Rest) Time(ctx context.Context) (int64, error) {
	body, _, err := r.request(ctx, http.MethodGet, "/time", nil, false)
	if err != nil {
		return 0, err
	}
	var times []int64
	if err := json.Unmarshal(body, &times); err != nil || len(times) == 0 {
		return 0, &RestError{Code: CodeServer, StatusCode: http.StatusInternalServerError, Message: "malformed /time response"}
	}
	return times[0], nil
}

// Get returns the [RestChannel] named name, creating it on first use. Options (e.g.
// [WithCipher]) apply when the channel is first created.
func (chans *RestChannels) Get(name string, options ...ChannelOption) *RestChannel {
	chans.mu.Lock()
	defer chans.mu.Unlock()
	if existing, ok := chans.byName[name]; ok {
		return existing
	}
	var settings channelSettings
	for _, option := range options {
		option(&settings)
	}
	channel := &RestChannel{Name: name, rest: chans.rest, cipher: settings.cipher}
	channel.Presence = &RestPresence{rest: chans.rest, channel: name, cipher: settings.cipher}
	chans.byName[name] = channel
	return channel
}

// Release removes the channel instance for name. A no-op when it doesn't exist.
func (chans *RestChannels) Release(name string) {
	chans.mu.Lock()
	delete(chans.byName, name)
	chans.mu.Unlock()
}

// request performs one authenticated request and returns the body plus the Link
// header's rel="next" target. Non-2xx responses return a [RestError]. When auth came
// from AuthCallback and the service reports the token invalid or expired, a fresh token
// is fetched and the request retried once.
func (r *Rest) request(ctx context.Context, method, path string, body []byte, withAuth bool) (respBody []byte, linkNext string, err error) {
	return r.requestRetry(ctx, method, path, body, withAuth, false)
}

func (r *Rest) requestRetry(ctx context.Context, method, path string, body []byte, withAuth, retried bool) ([]byte, string, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, r.baseURL+path, reader)
	if err != nil {
		return nil, "", err
	}
	if withAuth {
		header, err := r.authorizationHeader(ctx)
		if err != nil {
			return nil, "", err
		}
		req.Header.Set("Authorization", header)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	client := r.options.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", err
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		restErr := errorFromResponse(resp.StatusCode, respBody)
		if restErr.StatusCode == http.StatusUnauthorized && r.options.AuthCallback != nil && !retried {
			r.mu.Lock()
			r.cachedToken = ""
			r.mu.Unlock()
			return r.requestRetry(ctx, method, path, body, withAuth, true)
		}
		return nil, "", restErr
	}
	return respBody, parseLinkNext(resp.Header.Get("Link")), nil
}

// authorizationHeader resolves the Authorization header for the configured credential.
func (r *Rest) authorizationHeader(ctx context.Context) (string, error) {
	if r.options.Key != "" {
		return "Basic " + base64.StdEncoding.EncodeToString([]byte(r.options.Key)), nil
	}
	if r.options.Token != "" {
		return "Bearer " + r.options.Token, nil
	}
	r.mu.Lock()
	cached := r.cachedToken
	r.mu.Unlock()
	if cached == "" {
		token, err := r.options.AuthCallback(ctx)
		if err != nil {
			return "", fmt.Errorf("realtime: auth callback: %w", err)
		}
		r.mu.Lock()
		r.cachedToken = token
		r.mu.Unlock()
		cached = token
	}
	return "Bearer " + cached, nil
}

// RestChannel is a channel handle for REST operations: publish, history, and presence.
// Obtained from Channels.Get(name). It holds no server-side state.
type RestChannel struct {
	// Name is the channel name this instance is bound to.
	Name string
	// Presence reads this channel's current members.
	Presence *RestPresence

	rest   *Rest
	cipher *Cipher
}

// Publish publishes one message from an event name plus payload. On a channel with a
// cipher, the payload is end-to-end encrypted before it is sent. It returns the
// [PublishResult] once the service has accepted the message durably, and a [RestError]
// when the request fails, for example a key without the publish capability.
func (ch *RestChannel) Publish(ctx context.Context, name string, data any) (*PublishResult, error) {
	return ch.PublishMessages(ctx, RestPublishMessage{Name: name, Data: data})
}

// PublishMessages publishes one or more [RestPublishMessage] values, which can also set
// ClientID, ID, or Ephemeral. Several messages are stored and delivered as one atomic
// batch under one id.
func (ch *RestChannel) PublishMessages(ctx context.Context, messages ...RestPublishMessage) (*PublishResult, error) {
	if len(messages) == 0 {
		return nil, errors.New("realtime: PublishMessages: at least one message is required")
	}
	encoded := make([]map[string]any, len(messages))
	for i, message := range messages {
		body, err := ch.encodeMessage(message)
		if err != nil {
			return nil, err
		}
		encoded[i] = body
	}
	var payload any = encoded
	if len(encoded) == 1 {
		payload = encoded[0]
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	respBody, _, err := ch.rest.request(ctx, http.MethodPost, "/channels/"+url.PathEscape(ch.Name)+"/messages", body, true)
	if err != nil {
		return nil, err
	}
	var result PublishResult
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("realtime: malformed publish response: %w", err)
	}
	return &result, nil
}

// History reads the channel's message history, newest first by default. Batch publishes
// come back as one item per message, sharing the batch's id and serial. On a channel
// with a cipher, messages are decrypted before they are returned. How far back history
// reaches depends on each message's retention, see https://foony.io/docs/history. It
// returns one page. Page through older messages with the result's Next. It returns a
// [RestError] when history cannot be read.
func (ch *RestChannel) History(ctx context.Context, params RestHistoryParams) (*PaginatedResult[RestMessage], error) {
	query := url.Values{}
	if params.Limit > 0 {
		query.Set("limit", strconv.Itoa(params.Limit))
	}
	if params.Before > 0 {
		query.Set("before", strconv.FormatUint(params.Before, 10))
	}
	if params.Direction != "" {
		query.Set("direction", params.Direction)
	}
	suffix := ""
	if encoded := query.Encode(); encoded != "" {
		suffix = "?" + encoded
	}
	return ch.historyPage(ctx, "/channels/"+url.PathEscape(ch.Name)+"/messages"+suffix)
}

// historyPage loads one history page and wires up Next to load the following one.
func (ch *RestChannel) historyPage(ctx context.Context, path string) (*PaginatedResult[RestMessage], error) {
	body, linkNext, err := ch.rest.request(ctx, http.MethodGet, path, nil, true)
	if err != nil {
		return nil, err
	}
	var items []RestMessage
	if len(body) > 0 {
		if err := json.Unmarshal(body, &items); err != nil {
			return nil, fmt.Errorf("realtime: malformed history response: %w", err)
		}
	}
	if ch.cipher != nil {
		for i := range items {
			ch.decodeMessage(&items[i])
		}
	}
	return &PaginatedResult[RestMessage]{Items: items, nextPath: linkNext, load: ch.historyPage}, nil
}

// encodeMessage applies the channel cipher (when configured) to one outgoing message.
func (ch *RestChannel) encodeMessage(message RestPublishMessage) (map[string]any, error) {
	body := map[string]any{"name": message.Name, "data": message.Data}
	clientID := message.ClientID
	if clientID == "" {
		clientID = ch.rest.options.ClientID
	}
	if clientID != "" {
		body["clientId"] = clientID
	}
	if message.ID != "" {
		body["id"] = message.ID
	}
	if message.Ephemeral {
		body["ephemeral"] = true
	}
	if ch.cipher != nil {
		encrypted, err := ch.cipher.Encrypt(message.Data)
		if err != nil {
			return nil, err
		}
		body["data"] = encrypted.Data
		body["encoding"] = encrypted.Encoding
	}
	return body, nil
}

// decodeMessage decrypts one history item in place when the channel cipher can read it.
// An undecryptable item (a rotated key, another key's publish) is left undecoded with
// its Encoding intact rather than failing the whole page.
func (ch *RestChannel) decodeMessage(item *RestMessage) {
	if !IsCipherEncoding(item.Encoding) {
		return
	}
	plaintext, err := ch.cipher.Decrypt(item.Encoding, item.Data)
	if err != nil {
		return
	}
	item.Data = plaintext
	item.Encoding = ""
}

// RestPresence reads presence for one channel, from the channel's Presence field.
type RestPresence struct {
	rest    *Rest
	channel string
	cipher  *Cipher
}

// Get fetches the channel's current members. The snapshot is complete (presence sets
// are bounded), so the result is a single page. Members' Data is decrypted when the
// channel has a cipher. It returns a [RestError] when the request fails.
func (p *RestPresence) Get(ctx context.Context, params RestPresenceParams) (*PaginatedResult[PresenceMember], error) {
	query := url.Values{}
	if params.Limit > 0 {
		query.Set("limit", strconv.Itoa(params.Limit))
	}
	if params.ClientID != "" {
		query.Set("clientId", params.ClientID)
	}
	if params.ConnectionID != "" {
		query.Set("connectionId", params.ConnectionID)
	}
	suffix := ""
	if encoded := query.Encode(); encoded != "" {
		suffix = "?" + encoded
	}
	body, _, err := p.rest.request(ctx, http.MethodGet, "/channels/"+url.PathEscape(p.channel)+"/presence"+suffix, nil, true)
	if err != nil {
		return nil, err
	}
	var members []PresenceMember
	if len(body) > 0 {
		if err := json.Unmarshal(body, &members); err != nil {
			return nil, fmt.Errorf("realtime: malformed presence response: %w", err)
		}
	}
	if p.cipher != nil {
		for i := range members {
			p.decodeMember(&members[i])
		}
	}
	return &PaginatedResult[PresenceMember]{Items: members}, nil
}

// decodeMember decrypts one member's data in place when the channel cipher can read it.
// An undecryptable entry is left undecoded with its Encoding intact rather than failing
// the whole snapshot.
func (p *RestPresence) decodeMember(member *PresenceMember) {
	if !IsCipherEncoding(member.Encoding) {
		return
	}
	plaintext, err := p.cipher.Decrypt(member.Encoding, member.Data)
	if err != nil {
		return
	}
	member.Data = plaintext
	member.Encoding = ""
}

// RestAuth mints tokens against the service, from a [Rest] client's Auth field.
type RestAuth struct {
	rest *Rest
}

// RequestToken asks the service to mint a client JWT from this client's API key. The
// granted capability must be a subset of the key's own. It returns the [TokenDetails],
// whose Expires lets callers cache the token. It returns an error when this client has
// no Key, and a [RestError] when the service refuses, for example a capability outside
// the key's grant. See https://foony.io/docs/auth for the full token flow.
func (a *RestAuth) RequestToken(ctx context.Context, params TokenParams) (*TokenDetails, error) {
	key := a.rest.options.Key
	if key == "" {
		return nil, errors.New("realtime: RequestToken: an API key is required")
	}
	colon := strings.Index(key, ":")
	if colon <= 0 {
		// Without this, a colon-less key would silently build a mangled URL and
		// surface as a confusing 401 from the service.
		return nil, errors.New(`realtime: RequestToken: malformed API key (expected "appSlug.publicKeyId:privateKey")`)
	}
	keyName := key[:colon]
	body := map[string]any{"clientId": params.ClientID}
	if params.TTL > 0 {
		body["ttl"] = params.TTL.Milliseconds()
	}
	if params.Capability != nil {
		body["capability"] = params.Capability
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	respBody, _, err := a.rest.request(ctx, http.MethodPost, "/keys/"+url.PathEscape(keyName)+"/requestToken", encoded, true)
	if err != nil {
		return nil, err
	}
	var details TokenDetails
	if err := json.Unmarshal(respBody, &details); err != nil {
		return nil, fmt.Errorf("realtime: malformed requestToken response: %w", err)
	}
	return &details, nil
}

// endpointToHTTPURL resolves an endpoint (bare host or absolute URL) to the REST base
// URL.
func endpointToHTTPURL(endpoint string) string {
	if endpoint == "" {
		endpoint = DefaultEndpoint
	}
	if strings.HasPrefix(endpoint, "http://") || strings.HasPrefix(endpoint, "https://") {
		return strings.TrimSuffix(endpoint, "/")
	}
	return "https://" + endpoint
}

// linkNextPattern extracts the target of a rel="next" Link header entry.
var linkNextPattern = regexp.MustCompile(`<([^>]+)>\s*;\s*rel="next"`)

// parseLinkNext extracts the rel="next" target from a Link response header, if present.
func parseLinkNext(header string) string {
	if header == "" {
		return ""
	}
	for _, part := range strings.Split(header, ",") {
		if match := linkNextPattern.FindStringSubmatch(part); match != nil {
			return match[1]
		}
	}
	return ""
}

// errorFromResponse builds a [RestError] from a non-2xx response, tolerating non-JSON
// bodies.
func errorFromResponse(statusCode int, body []byte) *RestError {
	fallback := &RestError{Code: CodeServer, StatusCode: statusCode, Message: fmt.Sprintf("request failed with status %d", statusCode)}
	var parsed struct {
		Error struct {
			Message    string `json:"message"`
			Code       int    `json:"code"`
			StatusCode int    `json:"statusCode"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return fallback
	}
	if parsed.Error.Message == "" || parsed.Error.Code == 0 {
		return fallback
	}
	status := parsed.Error.StatusCode
	if status == 0 {
		status = statusCode
	}
	return &RestError{Code: parsed.Error.Code, StatusCode: status, Message: parsed.Error.Message}
}
