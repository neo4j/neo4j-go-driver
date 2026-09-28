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
	"encoding/base64"

	"github.com/neo4j/neo4j-go-driver/v6/neo4j/internal/errorutil"
	ipe "github.com/neo4j/neo4j-go-driver/v6/neo4j/internal/propertyencryption"
)

// localMetadataIV records the initialisation vector a key was wrapped with.
const localMetadataIV = "iv"

// LocalKeyEncapsulationService protects data encryption keys with AES-GCM under a key held by
// the application.
//
// The key encryption key must then be stored and rotated by the application, and anything
// able to read it can read every value encrypted under the profile. A KeyEncapsulationService
// backed by a key management service avoids both.
//
// LocalKeyEncapsulationService is part of the property encryption preview feature (see README
// on what it means in terms of support and compatibility guarantees).
type LocalKeyEncapsulationService struct {
	kek *ipe.DataKey
}

// NewLocalKeyEncapsulationService returns a service that wraps data encryption keys with kek,
// which must be 32 bytes. kek is not retained, so the caller may clear it.
//
// NewLocalKeyEncapsulationService is part of the property encryption preview feature (see
// README on what it means in terms of support and compatibility guarantees).
func NewLocalKeyEncapsulationService(kek []byte) (*LocalKeyEncapsulationService, error) {
	key, err := ipe.NewDataKey(kek)
	if err != nil {
		return nil, &errorutil.UsageError{Message: "invalid local key encryption key: " + err.Error()}
	}
	return &LocalKeyEncapsulationService{kek: key}, nil
}

// Encapsulate generates a data encryption key and wraps it with the key encryption key.
func (s *LocalKeyEncapsulationService) Encapsulate(
	_ context.Context, _ map[string]string) (KeyEncapsulationResult, error) {

	dek, err := ipe.NewDEK()
	if err != nil {
		return KeyEncapsulationResult{}, &Error{Message: "could not create a data encryption key", Cause: err}
	}
	iv, err := ipe.NewIV()
	if err != nil {
		return KeyEncapsulationResult{}, &Error{Message: "could not wrap the data encryption key", Cause: err}
	}
	encapsulation, err := s.kek.Seal(iv, dek, nil)
	if err != nil {
		return KeyEncapsulationResult{}, &Error{Message: "could not wrap the data encryption key", Cause: err}
	}
	return KeyEncapsulationResult{
		Key:           dek,
		Encapsulation: encapsulation,
		Metadata:      map[string]string{localMetadataIV: base64.StdEncoding.EncodeToString(iv)},
	}, nil
}

// Decapsulate unwraps a data encryption key with the key encryption key.
func (s *LocalKeyEncapsulationService) Decapsulate(
	_ context.Context, encapsulation []byte, metadata map[string]string) ([]byte, error) {

	encoded, ok := metadata[localMetadataIV]
	if !ok {
		return nil, &Error{Message: "the encapsulated key has no " + localMetadataIV +
			" metadata and was not wrapped by a local key encapsulation service"}
	}
	iv, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, &Error{Message: "the encapsulated key has unreadable " + localMetadataIV +
			" metadata", Cause: err}
	}
	dek, err := s.kek.Open(iv, encapsulation, nil)
	if err != nil {
		return nil, &Error{Message: "could not unwrap the data encryption key, " +
			"the key encryption key may be wrong", Cause: err}
	}
	return dek, nil
}
