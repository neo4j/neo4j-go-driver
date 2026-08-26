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
	"fmt"

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
	kek []byte
}

// NewLocalKeyEncapsulationService returns a service that wraps data encryption keys with kek,
// which must be 32 bytes. kek is copied, so the caller may reuse or clear it.
//
// NewLocalKeyEncapsulationService is part of the property encryption preview feature (see
// README on what it means in terms of support and compatibility guarantees).
func NewLocalKeyEncapsulationService(kek []byte) (*LocalKeyEncapsulationService, error) {
	if len(kek) != ipe.KeySize {
		return nil, &Error{Message: fmt.Sprintf(
			"a local key encryption key must be %d bytes but is %d", ipe.KeySize, len(kek))}
	}
	return &LocalKeyEncapsulationService{kek: append([]byte(nil), kek...)}, nil
}

// Encapsulate generates a data encryption key and wraps it with the key encryption key.
func (s *LocalKeyEncapsulationService) Encapsulate(
	_ context.Context, _ map[string]string) (EncapsulationResult, error) {

	dek, err := ipe.NewDEK()
	if err != nil {
		return EncapsulationResult{}, &Error{Message: "could not create a data encryption key", Cause: err}
	}
	encapsulation, iv, err := ipe.WrapKey(s.kek, dek)
	if err != nil {
		return EncapsulationResult{}, &Error{Message: "could not wrap the data encryption key", Cause: err}
	}
	return EncapsulationResult{
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
	dek, err := ipe.UnwrapKey(s.kek, encapsulation, iv)
	if err != nil {
		return nil, &Error{Message: "could not unwrap the data encryption key, " +
			"the key encryption key may be wrong", Cause: err}
	}
	return dek, nil
}
