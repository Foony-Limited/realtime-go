package realtime

// Client-side payload encryption for channels. End-to-end in the sense that the
// realtime edge only ever sees ciphertext. The key is shared between clients out of
// band and never sent to the server.
//
// The payload itself stays in the message's existing data field. How to read it is
// described by a separate encoding string (HTTP Content-Encoding style). For an
// encrypted message that's "cipher+aes-256-gcm/base64", and data is the base64 of
// iv + ciphertext + tag. Decoding unwinds the /-separated transforms right-to-left. The
// edge passes encoding through opaquely, and only the SDKs interpret it.

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// Encoding tokens describing an encrypted payload.
const (
	cipherToken256 = "cipher+aes-256-gcm"
	cipherToken128 = "cipher+aes-128-gcm"
	base64Token    = "base64"
)

// ivBytes is the AES-GCM nonce length in bytes (96-bit, the recommended size).
const ivBytes = 12

// CipherAlgorithm is the algorithm label accepted in [CipherParams]. The key length
// picks 128 vs 256.
type CipherAlgorithm string

// The supported cipher algorithms.
const (
	// AES256GCM is AES-256-GCM (a 32-byte key).
	AES256GCM CipherAlgorithm = "aes-256-gcm"
	// AES128GCM is AES-128-GCM (a 16-byte key).
	AES128GCM CipherAlgorithm = "aes-128-gcm"
)

// CipherParams are the parameters for channel encryption. Pass the built [Cipher] to
// Channels.Get with [WithCipher]. The key should be kept private and never shared with
// the public or our backend.
type CipherParams struct {
	// Key is the secret key as raw bytes: 16 (AES-128) or 32 (AES-256). Exactly one of
	// Key and KeyBase64 must be set.
	Key []byte
	// KeyBase64 is the secret key as a base64 string, as produced by
	// [GenerateRandomKey]. Exactly one of Key and KeyBase64 must be set.
	KeyBase64 string
	// Algorithm optionally declares the intended strength. The key length is what
	// actually selects AES-128 vs AES-256, and [NewCipher] returns an error when this
	// label contradicts it.
	Algorithm CipherAlgorithm
}

// EncryptResult is the output of [Cipher.Encrypt]: a transport encoding and the
// encrypted data.
type EncryptResult struct {
	// Encoding is the transport encoding describing Data, e.g.
	// "cipher+aes-256-gcm/base64".
	Encoding string
	// Data is the base64 of iv + ciphertext + tag.
	Data string
}

// IsCipherEncoding reports whether encoding indicates a ciphered payload that needs a
// [Cipher] to read.
func IsCipherEncoding(encoding string) bool {
	if encoding == "" {
		return false
	}
	for _, token := range strings.Split(encoding, "/") {
		if token == cipherToken256 || token == cipherToken128 {
			return true
		}
	}
	return false
}

// GenerateRandomKey generates a random base64-encoded AES key. Share the returned
// string between the clients that should be able to read a channel. Never send this to
// our backend. bits is the key size: 128 or 256 (use 256 unless you have a reason not
// to).
func GenerateRandomKey(bits int) (string, error) {
	if bits != 128 && bits != 256 {
		return "", fmt.Errorf("realtime: GenerateRandomKey: bits must be 128 or 256, got %d", bits)
	}
	key := make([]byte, bits/8)
	if _, err := rand.Read(key); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(key), nil
}

// Cipher is an AES-GCM cipher for one channel. It encrypts a JSON-serializable value
// into an (encoding, data) pair and back.
type Cipher struct {
	key  []byte
	aead cipher.AEAD
}

// NewCipher builds a [Cipher] from params. It returns an error when the key is not 16
// or 32 bytes, or when [CipherParams.Algorithm] contradicts the key length (a caller
// asking for AES-256 with a 16-byte key must not silently get AES-128).
func NewCipher(params CipherParams) (*Cipher, error) {
	if (len(params.Key) > 0) == (params.KeyBase64 != "") {
		return nil, errors.New("realtime: NewCipher: set exactly one of Key and KeyBase64")
	}
	key := params.Key
	if params.KeyBase64 != "" {
		decoded, err := base64.StdEncoding.DecodeString(params.KeyBase64)
		if err != nil {
			return nil, fmt.Errorf("realtime: NewCipher: bad base64 key: %w", err)
		}
		key = decoded
	}
	if len(key) != 16 && len(key) != 32 {
		return nil, fmt.Errorf("realtime: NewCipher: key must be 16 or 32 bytes (AES-128/256), got %d", len(key))
	}
	if params.Algorithm != "" {
		expected := 16
		if params.Algorithm == AES256GCM {
			expected = 32
		}
		if len(key) != expected {
			return nil, fmt.Errorf("realtime: NewCipher: %s needs a %d-byte key, got %d", params.Algorithm, expected, len(key))
		}
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &Cipher{key: key, aead: aead}, nil
}

// Encrypt encrypts a JSON-serializable value with a fresh random IV and returns the
// (encoding, data) pair to put on the wire.
func (c *Cipher) Encrypt(value any) (*EncryptResult, error) {
	plaintext, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("realtime: Cipher.Encrypt: marshal: %w", err)
	}
	iv := make([]byte, ivBytes)
	if _, err := rand.Read(iv); err != nil {
		return nil, err
	}
	// Seal appends ciphertext + tag after the IV, producing iv + ciphertext + tag in
	// one buffer (the same layout WebCrypto produces for the JS SDK).
	blob := c.aead.Seal(iv, iv, plaintext, nil)
	token := cipherToken128
	if len(c.key) == 32 {
		token = cipherToken256
	}
	return &EncryptResult{
		Encoding: token + "/" + base64Token,
		Data:     base64.StdEncoding.EncodeToString(blob),
	}, nil
}

// Decrypt decrypts a data payload carried under encoding back to its plaintext JSON.
// data is the raw JSON value from the wire (a base64 JSON string). It returns an error
// on a bad key, tampering, or an unsupported encoding.
func (c *Cipher) Decrypt(encoding string, data json.RawMessage) (json.RawMessage, error) {
	if !IsCipherEncoding(encoding) {
		return nil, fmt.Errorf("realtime: Cipher.Decrypt: not a cipher encoding: %q", encoding)
	}
	var encoded string
	if err := json.Unmarshal(data, &encoded); err != nil {
		return nil, errors.New("realtime: Cipher.Decrypt: encrypted data must be a base64 string")
	}
	blob, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("realtime: Cipher.Decrypt: bad base64: %w", err)
	}
	if len(blob) < ivBytes {
		return nil, errors.New("realtime: Cipher.Decrypt: payload shorter than the IV")
	}
	plaintext, err := c.aead.Open(nil, blob[:ivBytes], blob[ivBytes:], nil)
	if err != nil {
		return nil, fmt.Errorf("realtime: Cipher.Decrypt: %w", err)
	}
	return plaintext, nil
}
