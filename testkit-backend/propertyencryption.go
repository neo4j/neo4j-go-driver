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

package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/neo4j/neo4j-go-driver/v6/neo4j/propertyencryption"
)

// TestKit has no way to supply a key encapsulation service or key repository, so the backend
// provides them and drives the driver's API against them.

// testkitKeyRepository is an in-memory EncapsulatedKeyRepository.
type testkitKeyRepository struct {
	mutex   sync.Mutex
	keys    map[string]propertyencryption.EncapsulatedKey
	aliases map[string]string
	nextID  int
}

func newTestkitKeyRepository() *testkitKeyRepository {
	return &testkitKeyRepository{
		keys:    map[string]propertyencryption.EncapsulatedKey{},
		aliases: map[string]string{},
	}
}

func (r *testkitKeyRepository) FindByID(
	_ context.Context, id string) (propertyencryption.EncapsulatedKey, error) {

	r.mutex.Lock()
	defer r.mutex.Unlock()
	key, ok := r.keys[id]
	if !ok {
		return propertyencryption.EncapsulatedKey{}, propertyencryption.ErrKeyNotFound
	}
	return key, nil
}

func (r *testkitKeyRepository) FindByAlias(
	_ context.Context, alias string) (propertyencryption.EncapsulatedKey, error) {

	r.mutex.Lock()
	defer r.mutex.Unlock()
	id, ok := r.aliases[alias]
	if !ok {
		return propertyencryption.EncapsulatedKey{}, propertyencryption.ErrKeyNotFound
	}
	return r.keys[id], nil
}

func (r *testkitKeyRepository) Save(
	_ context.Context, alias string, encapsulation []byte,
	metadata map[string]string) (propertyencryption.EncapsulatedKey, error) {

	r.mutex.Lock()
	defer r.mutex.Unlock()
	id := strconv.Itoa(r.nextID)
	r.nextID++
	return r.store(id, alias, encapsulation, metadata), nil
}

func (r *testkitKeyRepository) AddAlias(_ context.Context, id, alias string) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if _, ok := r.keys[id]; !ok {
		return propertyencryption.ErrKeyNotFound
	}
	r.aliases[alias] = id
	return nil
}

func (r *testkitKeyRepository) DeleteAlias(_ context.Context, _, alias string) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	delete(r.aliases, alias)
	return nil
}

func (r *testkitKeyRepository) DeleteByID(_ context.Context, id string) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	delete(r.keys, id)
	return nil
}

// importKey seeds the repository with a key made elsewhere, under an id TestKit chooses.
func (r *testkitKeyRepository) importKey(
	id, alias string, encapsulation []byte,
	metadata map[string]string) propertyencryption.EncapsulatedKey {

	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.store(id, alias, encapsulation, metadata)
}

// store records a key. The caller must hold the mutex.
func (r *testkitKeyRepository) store(
	id, alias string, encapsulation []byte,
	metadata map[string]string) propertyencryption.EncapsulatedKey {

	key := propertyencryption.EncapsulatedKey{
		ID:            id,
		Alias:         alias,
		Encapsulation: encapsulation,
		Metadata:      metadata,
	}
	r.keys[id] = key
	r.aliases[alias] = id
	return key
}

// propertyEncryptionState holds the repositories the backend created for a driver.
type propertyEncryptionState struct {
	repositories map[string]*testkitKeyRepository
}

// buildPropertyEncryptionProfiles turns the propertyEncryptionProfiles field of a NewDriver
// request into configured profiles, and returns the repositories backing them.
func buildPropertyEncryptionProfiles(
	raw any) ([]propertyencryption.Profile, *propertyEncryptionState, error) {

	entries, ok := raw.([]any)
	if !ok {
		return nil, nil, fmt.Errorf("propertyEncryptionProfiles must be a list, got %T", raw)
	}

	profiles := make([]propertyencryption.Profile, 0, len(entries))
	state := &propertyEncryptionState{repositories: map[string]*testkitKeyRepository{}}
	for _, entry := range entries {
		fields, ok := entry.(map[string]any)
		if !ok {
			return nil, nil, fmt.Errorf("a property encryption profile must be an object, got %T", entry)
		}
		name, ok := fields["name"].(string)
		if !ok {
			return nil, nil, fmt.Errorf("a property encryption profile must have a name")
		}

		// The deterministic tests pin the key encryption key.
		kek := make([]byte, 32)
		if raw, given := fields["kek"]; given && raw != nil {
			decoded, err := decodeTestkitHex(raw)
			if err != nil {
				return nil, nil, fmt.Errorf("profile %s has an unreadable kek: %w", name, err)
			}
			kek = decoded
		} else if _, err := rand.Read(kek); err != nil {
			return nil, nil, err
		}

		service, err := propertyencryption.NewLocalKeyEncapsulationService(kek)
		if err != nil {
			return nil, nil, err
		}
		repository := newTestkitKeyRepository()
		state.repositories[name] = repository
		profiles = append(profiles, propertyencryption.EnvelopeProfile{
			Name:                 name,
			EncapsulationService: service,
			KeyRepository:        repository,
		})
	}
	return profiles, state, nil
}

// decodeTestkitHex reads the space separated hex TestKit uses for byte fields, for example
// "0a 1b 2c".
func decodeTestkitHex(raw any) ([]byte, error) {
	text, ok := raw.(string)
	if !ok {
		return nil, fmt.Errorf("expected a hex string, got %T", raw)
	}
	return hex.DecodeString(strings.ReplaceAll(text, " ", ""))
}

func encodeTestkitHex(value []byte) string {
	return addSpacesToHex(hex.EncodeToString(value))
}

// optionalString reads a field that TestKit may send as null.
func optionalString(data map[string]any, key string) string {
	if value, ok := data[key].(string); ok {
		return value
	}
	return ""
}

// keyReference builds the reference from the mutually exclusive keyAlias and keyId fields.
func keyReference(data map[string]any) (propertyencryption.KeyReference, error) {
	alias := optionalString(data, "keyAlias")
	id := optionalString(data, "keyId")
	switch {
	case alias != "" && id != "":
		return propertyencryption.KeyReference{}, fmt.Errorf("keyAlias and keyId are mutually exclusive")
	case alias != "":
		return propertyencryption.KeyAlias(alias), nil
	case id != "":
		return propertyencryption.KeyID(id), nil
	default:
		return propertyencryption.KeyReference{}, fmt.Errorf("one of keyAlias or keyId is required")
	}
}
