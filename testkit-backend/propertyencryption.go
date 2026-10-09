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
	"strings"

	"github.com/neo4j/neo4j-go-driver/v6/neo4j/propertyencryption"
)

type keyRepositoryReply struct {
	name string
	data map[string]any
}

// remoteKeyRepository is an EncapsulatedKeyRecordRepository served by TestKit.
type remoteKeyRepository struct {
	backend *backend
	// id is what TestKit knows this repository by.
	id string
}

func (r *remoteKeyRepository) FindByID(
	_ context.Context, id string) (propertyencryption.EncapsulatedKeyRecord, error) {

	reply, err := r.call("EncapsulatedKeyRepositoryFindByIdRequest", map[string]any{"keyId": id})
	if err != nil {
		return propertyencryption.EncapsulatedKeyRecord{}, err
	}
	return toFoundKeyRecord(reply["record"])
}

func (r *remoteKeyRepository) FindByAlias(
	_ context.Context, alias string) (propertyencryption.EncapsulatedKeyRecord, error) {

	reply, err := r.call("EncapsulatedKeyRepositoryFindByAliasRequest", map[string]any{"alias": alias})
	if err != nil {
		return propertyencryption.EncapsulatedKeyRecord{}, err
	}
	return toFoundKeyRecord(reply["record"])
}

func (r *remoteKeyRepository) Create(
	_ context.Context, alias string, encapsulation []byte,
	metadata map[string]string) (propertyencryption.EncapsulatedKeyRecord, error) {

	reply, err := r.call("EncapsulatedKeyRepositoryCreateRequest", map[string]any{
		"alias":         nullIfUnbound(alias),
		"encapsulation": encodeTestkitHex(encapsulation),
		"metadata":      metadata,
	})
	if err != nil {
		return propertyencryption.EncapsulatedKeyRecord{}, err
	}
	return toKeyRecord(reply["record"])
}

func (r *remoteKeyRepository) SetAlias(_ context.Context, id, alias string) error {
	_, err := r.call("EncapsulatedKeyRepositorySetAliasRequest", map[string]any{
		"keyId": id,
		"alias": nullIfUnbound(alias),
	})
	return err
}

func (r *remoteKeyRepository) DeleteByID(_ context.Context, id string) error {
	_, err := r.call("EncapsulatedKeyRepositoryDeleteRequest", map[string]any{"keyId": id})
	return err
}

// importKey registers a key made elsewhere, under an id TestKit chooses.
func (r *remoteKeyRepository) importKey(
	id, alias string, encapsulation []byte,
	metadata map[string]string) (propertyencryption.EncapsulatedKeyRecord, error) {

	reply, err := r.call("EncapsulatedKeyRepositoryImportRequest", map[string]any{
		"keyId":         id,
		"alias":         nullIfUnbound(alias),
		"encapsulation": encodeTestkitHex(encapsulation),
		"metadata":      metadata,
	})
	if err != nil {
		return propertyencryption.EncapsulatedKeyRecord{}, err
	}
	return toKeyRecord(reply["record"])
}

// call sends one reverse request and waits for TestKit to answer it.
func (r *remoteKeyRepository) call(name string, fields map[string]any) (map[string]any, error) {
	id := r.backend.nextId()
	fields["id"] = id
	fields["repositoryId"] = r.id
	r.backend.writeResponse(name, fields)

	for r.backend.process() {
		reply, ok := r.backend.keyRepositoryReplies[id]
		if !ok {
			continue
		}
		delete(r.backend.keyRepositoryReplies, id)
		if reply.name == "EncapsulatedKeyRepositoryErrorCompleted" {
			return nil, toKeyRepositoryError(reply.data)
		}
		return reply.data, nil
	}
	return nil, fmt.Errorf("TestKit closed before answering %s", name)
}

func toKeyRepositoryError(data map[string]any) error {
	errorType := optionalString(data, "errorType")
	detail := optionalString(data, "detail")
	switch errorType {
	case "KeyNotFound":
		return propertyencryption.ErrKeyNotFound
	case "AliasInUse":
		return fmt.Errorf("alias %s is in use", detail)
	default:
		return fmt.Errorf("the key repository failed with %s for %s", errorType, detail)
	}
}

// toFoundKeyRecord reads a lookup answer, null meaning no such key.
func toFoundKeyRecord(raw any) (propertyencryption.EncapsulatedKeyRecord, error) {
	if raw == nil {
		return propertyencryption.EncapsulatedKeyRecord{}, propertyencryption.ErrKeyNotFound
	}
	return toKeyRecord(raw)
}

func toKeyRecord(raw any) (propertyencryption.EncapsulatedKeyRecord, error) {
	fields, ok := raw.(map[string]any)
	if !ok {
		return propertyencryption.EncapsulatedKeyRecord{}, fmt.Errorf(
			"expected a key record, got %T", raw)
	}
	encapsulation, err := decodeTestkitHex(fields["encapsulation"])
	if err != nil {
		return propertyencryption.EncapsulatedKeyRecord{}, err
	}
	return propertyencryption.EncapsulatedKeyRecord{
		EncapsulatedKey: propertyencryption.EncapsulatedKey{
			ID:    optionalString(fields, "id"),
			Alias: optionalString(fields, "alias"),
		},
		Encapsulation: encapsulation,
		Metadata:      toKeyMetadata(fields["metadata"]),
	}, nil
}

func toKeyMetadata(raw any) map[string]string {
	metadata := map[string]string{}
	if fields, ok := raw.(map[string]any); ok {
		for name, value := range fields {
			metadata[name] = fmt.Sprintf("%v", value)
		}
	}
	return metadata
}

// nullIfUnbound sends an empty alias as the null TestKit expects.
func nullIfUnbound(alias string) any {
	if alias == "" {
		return nil
	}
	return alias
}

// propertyEncryptionState holds the repositories the backend created for a driver.
type propertyEncryptionState struct {
	repositories map[string]*remoteKeyRepository
}

// ids returns the repository ids to announce to TestKit.
func (s *propertyEncryptionState) ids() []string {
	ids := make([]string, 0, len(s.repositories))
	for _, repository := range s.repositories {
		ids = append(ids, repository.id)
	}
	return ids
}

// repositoryFor returns the repository for a profile, or the only one when name is empty.
func (s *propertyEncryptionState) repositoryFor(name string) (*remoteKeyRepository, error) {
	if name == "" {
		if len(s.repositories) != 1 {
			return nil, fmt.Errorf(
				"%d property encryption profiles are configured, so one must be named",
				len(s.repositories))
		}
		for _, repository := range s.repositories {
			return repository, nil
		}
	}
	repository := s.repositories[name]
	if repository == nil {
		return nil, fmt.Errorf("no property encryption profile is named %s", name)
	}
	return repository, nil
}

// buildPropertyEncryptionProfiles turns the propertyEncryptionProfiles field of a NewDriver
// request into configured profiles, and returns the repositories backing them.
func (b *backend) buildPropertyEncryptionProfiles(
	raw any) ([]propertyencryption.Profile, *propertyEncryptionState, error) {

	entries, ok := raw.([]any)
	if !ok {
		return nil, nil, fmt.Errorf("propertyEncryptionProfiles must be a list, got %T", raw)
	}

	profiles := make([]propertyencryption.Profile, 0, len(entries))
	state := &propertyEncryptionState{repositories: map[string]*remoteKeyRepository{}}
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
		repository := &remoteKeyRepository{backend: b, id: b.nextId()}
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
