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
	"context"
	"errors"
)

// ErrKeyNotFound is returned by an EncapsulatedKeyRepository when no key is stored under the
// requested id or alias.
//
// ErrKeyNotFound is part of the property encryption preview feature (see README on what it
// means in terms of support and compatibility guarantees).
var ErrKeyNotFound = errors.New("no encapsulated key was found")

// EncapsulatedKey is a data encryption key in protected form.
//
// EncapsulatedKey is part of the property encryption preview feature (see README on what it
// means in terms of support and compatibility guarantees).
type EncapsulatedKey struct {
	// ID identifies this key permanently. It is recorded with every value encrypted under
	// the key and is how the key is found again when decrypting, so it must be unique and
	// must never be reused.
	ID string
	// Alias is the name this key was created under, or empty. Unlike ID, an alias may later
	// be moved to a different key.
	Alias string
	// Encapsulation is the protected form of the key, as produced by a
	// KeyEncapsulationService.
	Encapsulation []byte
	// Metadata is what the KeyEncapsulationService needs back in order to recover the key.
	Metadata map[string]string
}

// EncapsulationResult is a new data encryption key together with its protected form.
//
// EncapsulationResult is part of the property encryption preview feature (see README on what
// it means in terms of support and compatibility guarantees).
type EncapsulationResult struct {
	// Key is the data encryption key. The driver derives its encryption key from this and
	// does not retain the value itself.
	Key []byte
	// Encapsulation is the protected form of Key.
	Encapsulation []byte
	// Metadata is whatever Decapsulate needs to recover Key, such as an initialisation
	// vector. It is stored with the encapsulation and passed back unchanged.
	Metadata map[string]string
}

// KeyEncapsulationService protects data encryption keys, typically by wrapping them with a
// key held in a key management service.
//
// Implementations are called while encrypting and decrypting, so they must be safe for
// concurrent use and should honour the context. Decapsulated keys are cached, so a round trip
// here is not paid per value.
//
// Errors that are already driver errors are returned to the caller unchanged, keeping a
// retryable failure retryable. Anything else is wrapped in an Error.
//
// See NewLocalKeyEncapsulationService for an implementation using a key held by the
// application.
//
// KeyEncapsulationService is part of the property encryption preview feature (see README on
// what it means in terms of support and compatibility guarantees).
type KeyEncapsulationService interface {
	// Encapsulate supplies a new data encryption key and its protected form. The
	// implementation chooses the key, since some schemes derive rather than generate it.
	Encapsulate(ctx context.Context, options map[string]string) (EncapsulationResult, error)
	// Decapsulate recovers a data encryption key from its protected form, given the metadata
	// Encapsulate returned for it.
	Decapsulate(ctx context.Context, encapsulation []byte, metadata map[string]string) ([]byte, error)
}

// EncapsulatedKeyRepository stores the data encryption keys an EnvelopeProfile uses. Keys are
// held in encapsulated form, so the repository never sees key material.
//
// Implementations are called while encrypting and decrypting, so they must be safe for
// concurrent use. Lookups should return ErrKeyNotFound when nothing matches.
//
// EncapsulatedKeyRepository is part of the property encryption preview feature (see README on
// what it means in terms of support and compatibility guarantees).
type EncapsulatedKeyRepository interface {
	// FindByID returns the key stored under id, or ErrKeyNotFound.
	FindByID(ctx context.Context, id string) (EncapsulatedKey, error)
	// FindByAlias returns the key currently bound to alias, or ErrKeyNotFound.
	FindByAlias(ctx context.Context, alias string) (EncapsulatedKey, error)
	// Save stores a new key under alias and returns it with the id it was assigned. The id
	// must be unique and must never be reused.
	Save(ctx context.Context, alias string, encapsulation []byte, metadata map[string]string) (EncapsulatedKey, error)
	// AddAlias binds alias to the key stored under id, moving it from another key if it is
	// already in use.
	AddAlias(ctx context.Context, id, alias string) error
	// DeleteAlias unbinds alias from the key stored under id.
	DeleteAlias(ctx context.Context, id, alias string) error
	// DeleteByID removes the key stored under id. Values encrypted under it can no longer be
	// decrypted.
	DeleteByID(ctx context.Context, id string) error
}
