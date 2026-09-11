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
	"time"

	ipe "github.com/neo4j/neo4j-go-driver/v6/neo4j/internal/propertyencryption"
)

// Default cache settings for an EnvelopeProfile.
const (
	DefaultKeyAliasCacheTTL  = ipe.DefaultKeyAliasCacheTTL
	DefaultKeyAliasCacheSize = ipe.DefaultKeyAliasCacheSize
	DefaultKeyCacheTTL       = ipe.DefaultKeyCacheTTL
	DefaultKeyCacheSize      = ipe.DefaultKeyCacheSize
)

// Profile describes how a set of values is encrypted. Every encrypted value records the name
// of the profile that produced it. EnvelopeProfile is the only implementation.
//
// Profile is part of the property encryption preview feature (see README on what it means in
// terms of support and compatibility guarantees).
type Profile interface {
	validate() error
}

// EnvelopeProfile encrypts values with a data encryption key and protects that key
// separately, so values are encrypted locally while the key protecting them can be held
// elsewhere, and a key management service is consulted per key rather than per value.
//
// Data encryption keys must be created before use, see Encryption.Keys.
//
// EnvelopeProfile is part of the property encryption preview feature (see README on what it
// means in terms of support and compatibility guarantees).
type EnvelopeProfile struct {
	// Name identifies this profile and is recorded with every value it encrypts. It must be
	// unique within a driver and must match on any other driver expected to read those
	// values.
	//
	// Required.
	Name string
	// EncapsulationService protects data encryption keys.
	//
	// Required.
	EncapsulationService KeyEncapsulationService
	// KeyRepository stores data encryption keys in their protected form.
	//
	// Required.
	KeyRepository EncapsulatedKeyRepository
	// KeyCacheTTL is how long a decapsulated data encryption key is held in memory.
	//
	// default: DefaultKeyCacheTTL
	KeyCacheTTL time.Duration
	// KeyCacheSize is the most data encryption keys held in memory at once.
	//
	// default: DefaultKeyCacheSize
	KeyCacheSize int
	// KeyAliasCacheTTL is how long an alias is assumed to still point at the same key, and
	// so the delay before a rotation performed elsewhere is noticed.
	//
	// default: DefaultKeyAliasCacheTTL
	KeyAliasCacheTTL time.Duration
	// KeyAliasCacheSize is the most alias mappings held in memory at once.
	//
	// default: DefaultKeyAliasCacheSize
	KeyAliasCacheSize int
}

func (p EnvelopeProfile) validate() error {
	switch {
	case p.Name == "":
		return &Error{Message: "a property encryption profile must have a name"}
	case p.EncapsulationService == nil:
		return &Error{Message: "property encryption profile " + p.Name +
			" has no EncapsulationService"}
	case p.KeyRepository == nil:
		return &Error{Message: "property encryption profile " + p.Name + " has no KeyRepository"}
	case p.KeyCacheTTL < 0 || p.KeyAliasCacheTTL < 0:
		return &Error{Message: "property encryption profile " + p.Name +
			" has a negative cache time to live"}
	case p.KeyCacheSize < 0 || p.KeyAliasCacheSize < 0:
		return &Error{Message: "property encryption profile " + p.Name +
			" has a negative cache size"}
	}
	return nil
}

// withDefaults applies the default cache settings to any left unset.
func (p EnvelopeProfile) withDefaults() EnvelopeProfile {
	if p.KeyCacheTTL == 0 {
		p.KeyCacheTTL = DefaultKeyCacheTTL
	}
	if p.KeyCacheSize == 0 {
		p.KeyCacheSize = DefaultKeyCacheSize
	}
	if p.KeyAliasCacheTTL == 0 {
		p.KeyAliasCacheTTL = DefaultKeyAliasCacheTTL
	}
	if p.KeyAliasCacheSize == 0 {
		p.KeyAliasCacheSize = DefaultKeyAliasCacheSize
	}
	return p
}
