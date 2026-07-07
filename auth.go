package realtime

// Token minting for trusted callers that hold a Realtime API key.
//
// CreateJWT signs a compact HS256 token locally with the key's secret, with no network
// round-trip. A trusted backend mints a short-lived, capability-scoped token for a
// less-trusted client, which uses it as its AuthCallback result. The edge verifies the
// signature against the same key secret on the WebSocket handshake. The key secret
// never leaves the backend, and the token carries no secret material. See
// https://foony.io/docs/auth for the full flow.

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Capability maps a channel pattern to its allowed operations (e.g.
// {"chat:site:*": {"subscribe", "publish"}}).
type Capability map[string][]string

// defaultJWTTTL is the default token lifetime: one hour. Short enough to bound a leaked
// token.
const defaultJWTTTL = time.Hour

// CreateJWTParams describe the token to mint.
type CreateJWTParams struct {
	// Capability is the capability the token grants (e.g.
	// {"chat:site:*": {"subscribe"}}). Must be a subset of the signing key's own
	// capability or the edge rejects it on connect. Exactly one of Capability and
	// CapabilityJSON must be set.
	Capability Capability
	// CapabilityJSON is the capability as a pre-serialized JSON string, as an
	// alternative to Capability.
	CapabilityJSON string
	// ClientID identifies the end user the token represents. Echoed back as the
	// connection's client id.
	ClientID string
	// TTL is the token lifetime. Defaults to one hour when zero, short enough to bound
	// a leaked token.
	TTL time.Duration
}

// CreateJWT signs a JWT locally with key, with no network call. The token's kid header
// is the public key name ("appSlug.publicKeyId") so the edge can look up the secret to
// verify it. The payload carries only the subject, capability, and expiry, and no
// secret. It returns the compact "header.payload.signature" string, and an error when
// the key is missing or malformed, or when TTL is negative.
//
//	// In your token endpoint. The key stays server-side.
//	token, err := realtime.CreateJWT(os.Getenv("FOONY_API_KEY"), realtime.CreateJWTParams{
//		ClientID:   userID,
//		Capability: realtime.Capability{"chat:" + userID + ":*": {"subscribe", "publish"}},
//	})
func CreateJWT(key string, params CreateJWTParams) (string, error) {
	if key == "" {
		return "", errors.New("realtime: CreateJWT: an API key is required")
	}
	keyName, secret, err := splitAPIKey(key)
	if err != nil {
		return "", err
	}
	capability := params.CapabilityJSON
	if capability == "" {
		if params.Capability == nil {
			return "", errors.New("realtime: CreateJWT: set one of Capability and CapabilityJSON")
		}
		serialized, err := json.Marshal(params.Capability)
		if err != nil {
			return "", fmt.Errorf("realtime: CreateJWT: marshal capability: %w", err)
		}
		capability = string(serialized)
	}
	ttl := params.TTL
	if ttl == 0 {
		ttl = defaultJWTTTL
	}
	if ttl < 0 {
		return "", errors.New("realtime: CreateJWT: TTL must be positive")
	}
	now := time.Now().Unix()
	header := map[string]string{"alg": "HS256", "typ": "JWT", "kid": keyName}
	payload := map[string]any{
		"sub": params.ClientID,
		"cap": capability,
		"iat": now,
		"exp": now + int64((ttl+time.Second-1)/time.Second),
	}
	headerJSON, err := json.Marshal(header)
	if err != nil {
		return "", err
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	signingInput := base64.RawURLEncoding.EncodeToString(headerJSON) + "." + base64.RawURLEncoding.EncodeToString(payloadJSON)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(signingInput))
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

// Auth is the token-minting namespace on a [Client]. It signs with the client's key.
// See https://foony.io/docs/auth for when to mint tokens yourself.
type Auth struct {
	resolveKey func() string
}

// CreateJWT mints a short-lived JWT scoped to params.Capability, signed with the
// client's API key. This is local, with no network call. It returns an error when the
// client was not constructed with a key — use the package-level [CreateJWT] to sign
// with an explicit key.
func (a *Auth) CreateJWT(params CreateJWTParams) (string, error) {
	key := a.resolveKey()
	if key == "" {
		return "", errors.New("realtime: Auth.CreateJWT: no API key available — construct the client with Options.Key or use the package-level CreateJWT")
	}
	return CreateJWT(key, params)
}

// splitAPIKey splits "appSlug.publicKeyId:privateKey" into the public key name and
// secret.
func splitAPIKey(key string) (keyName, secret string, err error) {
	colon := strings.Index(key, ":")
	if colon <= 0 || colon == len(key)-1 {
		return "", "", errors.New(`realtime: malformed API key (expected "appSlug.publicKeyId:privateKey")`)
	}
	keyName = key[:colon]
	secret = key[colon+1:]
	if !strings.Contains(keyName, ".") {
		return "", "", errors.New(`realtime: malformed API key name (expected "appSlug.publicKeyId")`)
	}
	return keyName, secret, nil
}
