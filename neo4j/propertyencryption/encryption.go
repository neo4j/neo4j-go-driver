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
	// AAD binds the value to a context, such as the node it belongs to. It is authenticated
	// but not encrypted, and is stored with the value. Passing it again to
	// Encryption.DecryptWithAAD is what makes a value moved to another context fail to
	// decrypt.
	//
	// AAD accepts bool, string, integer, []byte, dbtype.Date, dbtype.LocalTime, dbtype.Time,
	// dbtype.Point2D, dbtype.Point3D and dbtype.UUID. The remaining property types are
	// excluded because two equal values can encode differently.
	//
	// Values are not normalised, so a string that may arrive in more than one Unicode form
	// should be normalised before both encrypting and decrypting.
	//
	// Optional.
	AAD any
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
	aliasCache *ipe.Cache[string]
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
			aliasCache: ipe.NewCache[string](envelope.KeyAliasCacheTTL, envelope.KeyAliasCacheSize),
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
func (e *Encryption) Encrypt(ctx context.Context, request EncryptRequest) ([]byte, error) {
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
	var aad []byte
	if request.AAD != nil {
		encodedAAD, aadErr := ipe.EncodeAAD(request.AAD)
		if aadErr != nil {
			return nil, asError("the additional authenticated data cannot be used", aadErr)
		}
		aad = encodedAAD.Bytes
		metadata.SetBytes(ipe.MetadataAAD, aad)
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
	cipherOutput, err := dataKey.Seal(iv, encoded.Bytes, aad)
	if err != nil {
		return nil, &Error{Message: "could not encrypt the value", Cause: err}
	}

	metadata.SetString(ipe.MetadataKeyID, keyID)
	metadata.SetBytes(ipe.MetadataIV, iv)

	encrypted, err := ipe.EncodeEncrypted(ipe.Encrypted{
		ProfileName:  state.profile.Name,
		CipherOutput: cipherOutput,
		TypeName:     encoded.TypeName,
		Baseline:     encoded.Baseline,
		Metadata:     metadata,
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
	id := reference.value
	if reference.kind == keyReferenceAlias {
		var err error
		id, err = s.resolveAlias(ctx, reference.value)
		if err != nil {
			return "", nil, err
		}
	}

	if dataKey, ok := s.keyCache.Get(id); ok {
		return id, dataKey, nil
	}

	key, err := s.profile.KeyRepository.FindByID(ctx, id)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return "", nil, &Error{Message: "no encryption key has id " + id +
				" in profile " + s.profile.Name, Cause: err}
		}
		return "", nil, wrap("could not look up encryption key "+id, err)
	}

	dataKey, err := s.decapsulate(ctx, key)
	if err != nil {
		return "", nil, err
	}
	s.keyCache.Put(id, dataKey)
	return id, dataKey, nil
}

func (s *profileState) resolveAlias(ctx context.Context, alias string) (string, error) {
	if id, ok := s.aliasCache.Get(alias); ok {
		return id, nil
	}

	key, err := s.profile.KeyRepository.FindByAlias(ctx, alias)
	if err != nil {
		if errors.Is(err, ErrKeyNotFound) {
			return "", &Error{Message: "no encryption key has alias " + alias +
				" in profile " + s.profile.Name, Cause: err}
		}
		return "", wrap("could not look up encryption key alias "+alias, err)
	}
	if key.ID == "" {
		return "", &Error{Message: "the key repository returned a key with no id for alias " + alias}
	}

	s.aliasCache.Put(alias, key.ID)
	// The encapsulation is already to hand, so there is no need to look the key up again.
	if _, cached := s.keyCache.Get(key.ID); !cached {
		dataKey, err := s.decapsulate(ctx, key)
		if err != nil {
			return "", err
		}
		s.keyCache.Put(key.ID, dataKey)
	}
	return key.ID, nil
}

func (s *profileState) decapsulate(ctx context.Context, key EncapsulatedKey) (*ipe.DataKey, error) {
	dek, err := s.profile.EncapsulationService.Decapsulate(ctx, key.Encapsulation, key.Metadata)
	if err != nil {
		return nil, wrap("could not unwrap encryption key "+key.ID, err)
	}
	dataKey, err := ipe.DeriveDataKey(dek)
	if err != nil {
		return nil, &Error{Message: "could not prepare encryption key " + key.ID, Cause: err}
	}
	return dataKey, nil
}

// KeyManager creates the data encryption keys an EnvelopeProfile encrypts with, and is
// obtained from Encryption.Keys.
//
// KeyManager is part of the property encryption preview feature (see README on what it means
// in terms of support and compatibility guarantees).
type KeyManager struct {
	state *profileState
}

// Create makes a data encryption key, protects it with the profile's
// KeyEncapsulationService and stores it under alias, returning it without any key material.
//
// Calling Create with an alias already in use rotates it: the alias moves to the new key,
// while values encrypted under the old one still decrypt, each having recorded its key id.
func (m *KeyManager) Create(ctx context.Context, alias string) (EncapsulatedKey, error) {
	if alias == "" {
		return EncapsulatedKey{}, &Error{Message: "an encryption key alias cannot be empty"}
	}

	profile := m.state.profile
	result, err := profile.EncapsulationService.Encapsulate(ctx, map[string]string{})
	if err != nil {
		return EncapsulatedKey{}, wrap("could not create an encryption key", err)
	}
	if len(result.Key) == 0 {
		return EncapsulatedKey{}, &Error{
			Message: "the key encapsulation service returned no key material"}
	}
	// Fail before storing a key that cannot be used.
	dataKey, err := ipe.DeriveDataKey(result.Key)
	if err != nil {
		return EncapsulatedKey{}, &Error{
			Message: "the key encapsulation service returned an unusable key", Cause: err}
	}

	key, err := profile.KeyRepository.Save(ctx, alias, result.Encapsulation, result.Metadata)
	if err != nil {
		return EncapsulatedKey{}, wrap("could not store the new encryption key", err)
	}
	if key.ID == "" {
		return EncapsulatedKey{}, &Error{
			Message: "the key repository stored the new encryption key without giving it an id"}
	}

	m.state.aliasCache.Put(alias, key.ID)
	m.state.keyCache.Put(key.ID, dataKey)
	return key, nil
}
