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

// Package propertyencryption encrypts and decrypts individual Neo4j property values in the
// application, so that a value chosen for encryption reaches the database only as ciphertext.
// The encrypted bytes are portable across Neo4j drivers.
//
// Profiles are configured through config.Config.PropertyEncryptionProfiles and reached
// through neo4j.Driver.PropertyEncryption.
//
// Property encryption is a preview feature (see README on what it means in terms of support
// and compatibility guarantees).
package propertyencryption

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	ipe "github.com/neo4j/neo4j-go-driver/v6/neo4j/internal/propertyencryption"
)

// keyReferenceKind distinguishes the two ways a data encryption key can be named.
type keyReferenceKind int

const (
	keyReferenceNone keyReferenceKind = iota
	keyReferenceAlias
	keyReferenceID
)

// KeyReference names the data encryption key to encrypt with, built with KeyAlias or KeyID.
// The zero value names no key and is rejected.
//
// KeyReference is part of the property encryption preview feature (see README on what it
// means in terms of support and compatibility guarantees).
type KeyReference struct {
	kind  keyReferenceKind
	value string
}

// KeyAlias names a key by the alias it was created under, which picks up a rotation once the
// alias moves to a new key.
//
// KeyAlias is part of the property encryption preview feature (see README on what it means in
// terms of support and compatibility guarantees).
func KeyAlias(alias string) KeyReference {
	return KeyReference{kind: keyReferenceAlias, value: alias}
}

// KeyID names a key by its permanent id, pinning encryption to that key.
//
// KeyID is part of the property encryption preview feature (see README on what it means in
// terms of support and compatibility guarantees).
func KeyID(id string) KeyReference {
	return KeyReference{kind: keyReferenceID, value: id}
}

// EncryptRequest describes one value to encrypt.
//
// EncryptRequest is part of the property encryption preview feature (see README on what it
// means in terms of support and compatibility guarantees).
type EncryptRequest struct {
	// Value is the value to encrypt. It must be a Neo4j property type.
	//
	// Required.
	Value any
	// Key names the data encryption key to use. Build it with KeyAlias or KeyID.
	//
	// Required.
	Key KeyReference
	// Profile names the encryption profile to use. It may be left empty when exactly one
	// profile is configured.
	//
	// Optional.
	Profile string
}

// Encryption encrypts and decrypts Neo4j property values, and is obtained from
// neo4j.Driver.PropertyEncryption. It is safe for concurrent use and needs no connection.
//
// Encryption is part of the property encryption preview feature (see README on what it means
// in terms of support and compatibility guarantees).
type Encryption struct {
	profiles map[string]*profileState
	// sole is the only profile's name when exactly one is configured, so that callers may
	// leave the profile out.
	sole string
	// names lists the configured profiles for error messages.
	names []string
	newIV func() ([]byte, error)
}

// profileState is a configured profile and its caches.
type profileState struct {
	profile    EnvelopeProfile
	aliasIndex *ipe.Cache[string]
	keyCache   *ipe.Cache[*ipe.DataKey]
}

// New builds an Encryption from profiles. Applications normally configure profiles through
// config.Config.PropertyEncryptionProfiles and reach the result through
// neo4j.Driver.PropertyEncryption, but New allows encrypting and decrypting without a driver.
//
// New is part of the property encryption preview feature (see README on what it means in
// terms of support and compatibility guarantees).
func New(profiles []Profile) (*Encryption, error) {
	encryption := &Encryption{
		profiles: make(map[string]*profileState, len(profiles)),
		names:    make([]string, 0, len(profiles)),
		newIV:    ipe.NewIV,
	}
	for _, profile := range profiles {
		if profile == nil {
			return nil, &Error{Message: "a property encryption profile is nil"}
		}
		if err := profile.validate(); err != nil {
			return nil, err
		}
		envelope, ok := profile.(EnvelopeProfile)
		if !ok {
			return nil, &Error{Message: fmt.Sprintf(
				"unsupported property encryption profile type %T", profile)}
		}
		if _, duplicate := encryption.profiles[envelope.Name]; duplicate {
			return nil, &Error{Message: "more than one property encryption profile is named " +
				envelope.Name}
		}
		envelope = envelope.withDefaults()
		encryption.profiles[envelope.Name] = &profileState{
			profile:    envelope,
			aliasIndex: ipe.NewCache[string](envelope.KeyAliasIndexTTL, envelope.KeyAliasIndexSize),
			keyCache:   ipe.NewCache[*ipe.DataKey](envelope.KeyCacheTTL, envelope.KeyCacheSize),
		}
		encryption.names = append(encryption.names, envelope.Name)
	}
	sort.Strings(encryption.names)
	if len(encryption.names) == 1 {
		encryption.sole = encryption.names[0]
	}
	return encryption, nil
}

// Encrypt encrypts a property value, returning bytes that carry everything needed to decrypt
// them. How those bytes are stored is left to the application.
//
// Encrypting the same value twice produces different bytes, so encrypted values cannot be
// compared or searched in Cypher.
//
// Use EncryptWithAAD to bind the value to a context.
func (e *Encryption) Encrypt(ctx context.Context, request EncryptRequest) ([]byte, error) {
	return e.encrypt(ctx, request, nil)
}

// EncryptWithAAD encrypts a property value, binding it to aad, which DecryptWithAAD then
// needs to decrypt it. aad is authenticated but not encrypted, and is stored with the value.
//
// aad accepts bool, string, integer, []byte, dbtype.Date, dbtype.LocalTime, dbtype.Time,
// dbtype.Point2D, dbtype.Point3D and dbtype.UUID. Other property types are excluded because
// two equal values can encode differently.
//
// aad is not normalised, so normalise a string that may arrive in more than one Unicode form
// before encrypting and decrypting.
func (e *Encryption) EncryptWithAAD(
	ctx context.Context, request EncryptRequest, aad any) ([]byte, error) {

	if aad == nil {
		return nil, &Error{Message: "no additional authenticated data was supplied, " +
			"use Encrypt to encrypt without it"}
	}
	return e.encrypt(ctx, request, aad)
}

func (e *Encryption) encrypt(ctx context.Context, request EncryptRequest, aad any) ([]byte, error) {
	state, err := e.profileFor(request.Profile)
	if err != nil {
		return nil, err
	}
	if request.Key.kind == keyReferenceNone {
		return nil, &Error{Message: "no encryption key was named, use KeyAlias or KeyID"}
	}

	encoded, err := ipe.EncodeValue(request.Value)
	if err != nil {
		return nil, asError("the value cannot be encrypted", err)
	}

	var metadata ipe.Metadata
	var aadBytes []byte
	if aad != nil {
		encodedAAD, aadErr := ipe.EncodeAAD(aad)
		if aadErr != nil {
			return nil, asError("the additional authenticated data cannot be used", aadErr)
		}
		aadBytes = encodedAAD.Bytes
		metadata.SetBytes(ipe.MetadataAAD, aadBytes)
		metadata.SetInt(ipe.MetadataAADEncodingSchemeMajor, int64(encodedAAD.Baseline.Major))
		metadata.SetInt(ipe.MetadataAADEncodingSchemeMinor, int64(encodedAAD.Baseline.Minor))
	}

	keyID, dataKey, err := state.resolve(ctx, request.Key)
	if err != nil {
		return nil, err
	}

	iv, err := e.newIV()
	if err != nil {
		return nil, &Error{Message: "could not create an initialisation vector", Cause: err}
	}
	cipherOutput, err := dataKey.Seal(iv, encoded.Bytes, aadBytes)
	if err != nil {
		return nil, &Error{Message: "could not encrypt the value", Cause: err}
	}

	metadata.SetString(ipe.MetadataKeyID, keyID)
	metadata.SetBytes(ipe.MetadataIV, iv)

	encrypted, err := ipe.EncodeEncrypted(ipe.Encrypted{
		ProfileType:    ipe.ProfileTypeEnvelope,
		ProfileVersion: ipe.EnvelopeProfileVersion,
		ProfileName:    state.profile.Name,
		CipherOutput:   cipherOutput,
		TypeName:       encoded.TypeName,
		Baseline:       encoded.Baseline,
		Metadata:       metadata,
	})
	if err != nil {
		return nil, &Error{Message: "could not assemble the encrypted value", Cause: err}
	}
	return encrypted, nil
}

// Decrypt decrypts a value produced by Encrypt, authenticating it against the additional
// authenticated data stored with it. A value that cannot be authenticated fails rather than
// returning a partially trusted result.
//
// A value encrypted with a type or encoding this driver does not know decrypts to a
// *dbtype.UnsupportedType rather than failing.
//
// Use DecryptWithAAD to authenticate against caller-supplied data instead.
func (e *Encryption) Decrypt(ctx context.Context, encrypted []byte) (any, error) {
	return e.decrypt(ctx, encrypted, nil, false)
}

// DecryptWithAAD decrypts a value, authenticating it against aad rather than the additional
// authenticated data stored with it. A value that was bound to another context is rejected.
//
// aad must be the same value, not an equivalent one, since it is not normalised before it is
// encoded.
func (e *Encryption) DecryptWithAAD(ctx context.Context, encrypted []byte, aad any) (any, error) {
	if aad == nil {
		return nil, &Error{Message: "no additional authenticated data was supplied, " +
			"use Decrypt to authenticate against the data stored with the value"}
	}
	return e.decrypt(ctx, encrypted, aad, true)
}

func (e *Encryption) decrypt(ctx context.Context, encrypted []byte, aad any, explicit bool) (any, error) {
	structure, err := ipe.DecodeEncrypted(encrypted)
	if err != nil {
		return nil, asError("the value is not a Neo4j encrypted value", err)
	}

	state, err := e.profileFor(structure.ProfileName)
	if err != nil {
		return nil, err
	}

	iv, ok := structure.Metadata.Bytes(ipe.MetadataIV)
	if !ok {
		return nil, &Error{Message: "the encrypted value has no initialisation vector"}
	}
	keyID, ok := structure.Metadata.String(ipe.MetadataKeyID)
	if !ok {
		return nil, &Error{Message: "the encrypted value does not say which key encrypted it"}
	}

	aadBytes, err := aadFor(structure, aad, explicit)
	if err != nil {
		return nil, err
	}

	_, dataKey, err := state.resolve(ctx, KeyID(keyID))
	if err != nil {
		return nil, err
	}

	plaintext, err := dataKey.Open(iv, structure.CipherOutput, aadBytes)
	if err != nil {
		return nil, &Error{Message: "could not decrypt the value", Cause: err}
	}

	value, err := ipe.DecodeValue(plaintext, structure.TypeName, structure.Baseline)
	if err != nil {
		return nil, asError("the decrypted value could not be read", err)
	}
	return value, nil
}

// aadFor produces the bytes to authenticate against.
func aadFor(structure ipe.Encrypted, aad any, explicit bool) ([]byte, error) {
	if !explicit {
		stored, _ := structure.Metadata.Bytes(ipe.MetadataAAD)
		return stored, nil
	}

	encoded, err := ipe.EncodeAAD(aad)
	if err != nil {
		return nil, asError("the additional authenticated data cannot be used", err)
	}

	// Reproducing the bytes under a different encoding would fail to authenticate without
	// saying why.
	major, hasMajor := structure.Metadata.Int(ipe.MetadataAADEncodingSchemeMajor)
	minor, hasMinor := structure.Metadata.Int(ipe.MetadataAADEncodingSchemeMinor)
	if !hasMajor || !hasMinor {
		if _, bound := structure.Metadata.Bytes(ipe.MetadataAAD); !bound {
			return nil, &Error{Message: "this value was not encrypted with additional " +
				"authenticated data, decrypt it with Decrypt"}
		}
		return encoded.Bytes, nil
	}

	recorded := ipe.Version{Major: int(major), Minor: int(minor)}
	if encoded.Baseline != recorded {
		return nil, &Error{Message: fmt.Sprintf(
			"the additional authenticated data has to be encoded with Bolt Value Encoding "+
				"Scheme %s to match this value, but this driver encodes it with %s",
			recorded, encoded.Baseline)}
	}
	return encoded.Bytes, nil
}

// Keys returns the KeyManager for a profile. Leave profileName empty when one profile is
// configured.
func (e *Encryption) Keys(profileName string) (*KeyManager, error) {
	state, err := e.profileFor(profileName)
	if err != nil {
		return nil, err
	}
	return &KeyManager{state: state}, nil
}

// profileFor resolves a profile by name, or the only one when the name is empty.
func (e *Encryption) profileFor(name string) (*profileState, error) {
	if name == "" {
		switch {
		case len(e.profiles) == 0:
			return nil, &Error{Message: "no property encryption profiles are configured, " +
				"set config.Config.PropertyEncryptionProfiles"}
		case e.sole == "":
			return nil, &Error{Message: "more than one property encryption profile is " +
				"configured (" + strings.Join(e.names, ", ") + "), so one must be named"}
		}
		name = e.sole
	}
	state, ok := e.profiles[name]
	if !ok {
		if len(e.names) == 0 {
			return nil, &Error{Message: "no property encryption profile is named " + name +
				", none are configured"}
		}
		return nil, &Error{Message: "no property encryption profile is named " + name +
			", the configured profiles are " + strings.Join(e.names, ", ")}
	}
	return state, nil
}

// resolve finds the data encryption key a reference names, consulting the caches first.
func (s *profileState) resolve(ctx context.Context, reference KeyReference) (string, *ipe.DataKey, error) {
	if reference.kind == keyReferenceAlias {
		return s.resolveByAlias(ctx, reference.value)
	}
	return s.resolveByID(ctx, reference.value)
}

func (s *profileState) resolveByID(ctx context.Context, id string) (string, *ipe.DataKey, error) {
	if dataKey, ok := s.keyCache.Get(id); ok {
		return id, dataKey, nil
	}

	record, err := s.profile.KeyRepository.FindByID(ctx, id)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return "", nil, &Error{Message: "no encryption key has id " + id +
				" in profile " + s.profile.Name, Cause: err}
		}
		return "", nil, wrap("could not look up encryption key "+id, err)
	}

	dataKey, err := s.decapsulate(ctx, record)
	if err != nil {
		return "", nil, err
	}
	s.keyCache.Put(id, dataKey)
	return id, dataKey, nil
}

func (s *profileState) resolveByAlias(ctx context.Context, alias string) (string, *ipe.DataKey, error) {
	if id, ok := s.aliasIndex.Get(alias); ok {
		if dataKey, ok := s.keyCache.Get(id); ok {
			return id, dataKey, nil
		}
		// The key has gone, so the mapping is old enough to re-read too.
		s.aliasIndex.Remove(alias)
	}

	record, err := s.profile.KeyRepository.FindByAlias(ctx, alias)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return "", nil, &Error{Message: "no encryption key has alias " + alias +
				" in profile " + s.profile.Name, Cause: err}
		}
		return "", nil, wrap("could not look up encryption key alias "+alias, err)
	}
	if record.ID == "" {
		return "", nil, &Error{
			Message: "the key repository returned a key with no id for alias " + alias}
	}

	dataKey, err := s.decapsulate(ctx, record)
	if err != nil {
		return "", nil, err
	}
	s.aliasIndex.Put(alias, record.ID)
	s.keyCache.Put(record.ID, dataKey)
	return record.ID, dataKey, nil
}

func (s *profileState) decapsulate(ctx context.Context, record EncapsulatedKeyRecord) (*ipe.DataKey, error) {
	dek, err := s.profile.EncapsulationService.Decapsulate(ctx, record.Encapsulation, record.Metadata)
	if err != nil {
		return nil, wrap("could not unwrap encryption key "+record.ID, err)
	}
	dataKey, err := ipe.NewDataKey(dek)
	if err != nil {
		return nil, &Error{Message: "could not prepare encryption key " + record.ID, Cause: err}
	}
	return dataKey, nil
}

// KeyManager manages the data encryption keys an EnvelopeProfile encrypts with, and is
// obtained from Encryption.Keys.
//
// KeyManager is part of the property encryption preview feature (see README on what it means
// in terms of support and compatibility guarantees).
type KeyManager struct {
	state *profileState
}

// Create makes a data encryption key, protects it with the profile's KeyEncapsulationService
// and stores it under alias, returning it without any key material.
//
// An empty alias leaves the key unbound, reachable only by id. Creating a key under an alias
// already in use rotates it: the alias moves to the new key, while values encrypted under the
// old one still decrypt, each having recorded its key id.
//
// options is passed to the KeyEncapsulationService and stored with the key, for services that
// need to record which key protected it. It may be nil.
func (m *KeyManager) Create(
	ctx context.Context, alias string, options map[string]string) (EncapsulatedKey, error) {

	profile := m.state.profile
	if options == nil {
		options = map[string]string{}
	}
	result, err := profile.EncapsulationService.Encapsulate(ctx, options)
	if err != nil {
		return EncapsulatedKey{}, wrap("could not create an encryption key", err)
	}
	// Fail before storing a key that cannot be used.
	dataKey, err := ipe.NewDataKey(result.Key)
	if err != nil {
		return EncapsulatedKey{}, &Error{
			Message: "the key encapsulation service returned an unusable key", Cause: err}
	}

	record, err := profile.KeyRepository.Create(ctx, alias, result.Encapsulation, result.Metadata)
	if err != nil {
		return EncapsulatedKey{}, wrap("could not store the new encryption key", err)
	}
	if record.ID == "" {
		return EncapsulatedKey{}, &Error{
			Message: "the key repository stored the new encryption key without giving it an id"}
	}

	if alias != "" {
		m.state.aliasIndex.Put(alias, record.ID)
	}
	m.state.keyCache.Put(record.ID, dataKey)
	return record.EncapsulatedKey, nil
}

// FindByAlias returns the key currently bound to alias, or an error wrapping ErrKeyNotFound
// when nothing is bound to it.
func (m *KeyManager) FindByAlias(ctx context.Context, alias string) (EncapsulatedKey, error) {
	record, err := m.state.profile.KeyRepository.FindByAlias(ctx, alias)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return EncapsulatedKey{}, &Error{Message: "no encryption key has alias " + alias +
				" in profile " + m.state.profile.Name, Cause: err}
		}
		return EncapsulatedKey{}, wrap("could not look up encryption key alias "+alias, err)
	}
	return record.EncapsulatedKey, nil
}

// SetAlias binds alias to the key stored under id, moving it from another key if it is
// already in use. Use DeleteAlias to unbind one.
func (m *KeyManager) SetAlias(ctx context.Context, id, alias string) error {
	if alias == "" {
		return &Error{Message: "an encryption key alias cannot be empty, use DeleteAlias"}
	}
	return m.setAlias(ctx, id, alias)
}

// DeleteAlias unbinds whatever alias the key stored under id currently has. The key stays,
// reachable by id, and values encrypted under it still decrypt.
func (m *KeyManager) DeleteAlias(ctx context.Context, id string) error {
	return m.setAlias(ctx, id, "")
}

func (m *KeyManager) setAlias(ctx context.Context, id, alias string) error {
	if id == "" {
		return &Error{Message: "an encryption key id cannot be empty"}
	}
	if err := m.state.profile.KeyRepository.SetAlias(ctx, id, alias); err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return &Error{Message: "no encryption key has id " + id +
				" in profile " + m.state.profile.Name, Cause: err}
		}
		return wrap("could not set the alias of encryption key "+id, err)
	}
	// Any cached mapping may now point at the wrong key.
	m.state.aliasIndex.Clear()
	return nil
}

// DeleteByID removes the key stored under id. Values already encrypted under it can no longer
// be decrypted.
func (m *KeyManager) DeleteByID(ctx context.Context, id string) error {
	if id == "" {
		return &Error{Message: "an encryption key id cannot be empty"}
	}
	if err := m.state.profile.KeyRepository.DeleteByID(ctx, id); err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return &Error{Message: "no encryption key has id " + id +
				" in profile " + m.state.profile.Name, Cause: err}
		}
		return wrap("could not delete encryption key "+id, err)
	}
	m.state.keyCache.Remove(id)
	m.state.aliasIndex.Clear()
	return nil
}
