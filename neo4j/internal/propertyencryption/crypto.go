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
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
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
	// keyDerivationInfo separates property encryption keys from other keys derived from the
	// same data encryption key.
	keyDerivationInfo = "neo4j/property-encryption/v1"
)

// ErrAuthentication reports a cipher output that failed authentication, which AES-GCM cannot
// attribute to the wrong key, the wrong AAD, or tampering.
var ErrAuthentication = errors.New("the encrypted value could not be authenticated, " +
	"the key or the additional authenticated data may be wrong, or the value may have been altered")

// DataKey is an AES-GCM cipher derived from a data encryption key. Derivation happens once
// per data encryption key, keeping HKDF and the AES key schedule off the path of every call.
type DataKey struct {
	aead cipher.AEAD
}

// DeriveDataKey expands a data encryption key into the key used for property encryption,
// using HKDF-SHA256 with an empty salt.
func DeriveDataKey(dek []byte) (*DataKey, error) {
	if len(dek) == 0 {
		return nil, errors.New("the key encapsulation service returned an empty data encryption key")
	}
	derived, err := hkdf.Key(sha256.New, dek, nil, keyDerivationInfo, KeySize)
	if err != nil {
		return nil, fmt.Errorf("deriving the property encryption key: %w", err)
	}
	block, err := aes.NewCipher(derived)
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

// WrapKey encapsulates a data encryption key under a local key encryption key, returning the
// encapsulation and the initialisation vector needed to reverse it.
//
// The key encryption key is used directly rather than through HKDF, which the encapsulation
// format requires for a key to remain usable across drivers.
func WrapKey(kek, dek []byte) (encapsulation, iv []byte, err error) {
	aead, err := localAEAD(kek)
	if err != nil {
		return nil, nil, err
	}
	iv, err = NewIV()
	if err != nil {
		return nil, nil, err
	}
	return aead.Seal(nil, iv, dek, nil), iv, nil
}

// UnwrapKey reverses WrapKey.
func UnwrapKey(kek, encapsulation, iv []byte) ([]byte, error) {
	aead, err := localAEAD(kek)
	if err != nil {
		return nil, err
	}
	if len(iv) != IVSize {
		return nil, fmt.Errorf("the key encapsulation initialisation vector must be %d bytes but is %d",
			IVSize, len(iv))
	}
	dek, err := aead.Open(nil, iv, encapsulation, nil)
	if err != nil {
		return nil, ErrAuthentication
	}
	return dek, nil
}

func localAEAD(kek []byte) (cipher.AEAD, error) {
	if len(kek) != KeySize {
		return nil, fmt.Errorf("the key encryption key must be %d bytes but is %d", KeySize, len(kek))
	}
	block, err := aes.NewCipher(kek)
	if err != nil {
		return nil, fmt.Errorf("preparing the key encapsulation cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("preparing the key encapsulation cipher: %w", err)
	}
	return aead, nil
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
