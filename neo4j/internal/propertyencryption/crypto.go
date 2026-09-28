/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package propertyencryption

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
)

const (
	// KeySize is the AES key size in bytes.
	KeySize = 32
	// IVSize is the AES-GCM initialisation vector size in bytes.
	IVSize = 12
	// TagSize is the AES-GCM authentication tag size in bytes.
	TagSize = 16
)

// ErrAuthentication reports a cipher output that failed authentication, which AES-GCM cannot
// attribute to the wrong key, the wrong AAD, or tampering.
var ErrAuthentication = errors.New("the encrypted value could not be authenticated, " +
	"the key or the additional authenticated data may be wrong, or the value may have been altered")

// DataKey is an AES-GCM cipher built from an AES-256 key. It is built once per key, keeping
// the AES key schedule off the path of every call.
type DataKey struct {
	aead cipher.AEAD
}

// NewDataKey prepares the cipher for key, which must be AES-256.
func NewDataKey(key []byte) (*DataKey, error) {
	if len(key) != KeySize {
		return nil, fmt.Errorf("an AES-256 key must be %d bytes but is %d", KeySize, len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("preparing the property encryption cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("preparing the property encryption cipher: %w", err)
	}
	return &DataKey{aead: aead}, nil
}

// Seal encrypts plaintext, returning the ciphertext with the authentication tag appended.
// aad may be empty.
func (k *DataKey) Seal(iv, plaintext, aad []byte) ([]byte, error) {
	if len(iv) != IVSize {
		return nil, fmt.Errorf("the initialisation vector must be %d bytes but is %d", IVSize, len(iv))
	}
	return k.aead.Seal(nil, iv, plaintext, aad), nil
}

// Open authenticates and decrypts a cipher output. aad must be the bytes supplied to Seal.
func (k *DataKey) Open(iv, cipherOutput, aad []byte) ([]byte, error) {
	if len(iv) != IVSize {
		return nil, fmt.Errorf("the initialisation vector must be %d bytes but is %d", IVSize, len(iv))
	}
	if len(cipherOutput) < TagSize {
		return nil, fmt.Errorf("the cipher output must be at least %d bytes but is %d",
			TagSize, len(cipherOutput))
	}
	plaintext, err := k.aead.Open(nil, iv, cipherOutput, aad)
	if err != nil {
		// Reporting which of the possible causes applies would leak information.
		return nil, ErrAuthentication
	}
	return plaintext, nil
}

// NewDEK draws a fresh data encryption key.
func NewDEK() ([]byte, error) {
	dek := make([]byte, KeySize)
	if _, err := rand.Read(dek); err != nil {
		return nil, fmt.Errorf("generating a data encryption key: %w", err)
	}
	return dek, nil
}

// NewIV draws a fresh initialisation vector. AES-GCM security depends on never reusing one
// with the same key, so the source must stay cryptographically secure.
func NewIV() ([]byte, error) {
	iv := make([]byte, IVSize)
	if _, err := rand.Read(iv); err != nil {
		return nil, fmt.Errorf("generating an initialisation vector: %w", err)
	}
	return iv, nil
}
