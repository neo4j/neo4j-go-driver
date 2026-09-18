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

package propertyencryption_test

import (
	"context"
	"fmt"
	"os"

	"github.com/neo4j/neo4j-go-driver/v6/neo4j"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j/config"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j/propertyencryption"
)

// ExampleEncryption shows how to encrypt a property before storing it and decrypt it after
// reading it back.
func ExampleEncryption() {
	ctx := context.Background()

	// A key encryption key would normally come from a key management service.
	kek := make([]byte, 32)
	keyService, err := propertyencryption.NewLocalKeyEncapsulationService(kek)
	if err != nil {
		panic(err)
	}

	driver, err := neo4j.NewDriver(getUrl(), neo4j.BasicAuth("neo4j", "password", ""),
		func(c *config.Config) {
			c.PropertyEncryptionProfiles = []propertyencryption.Profile{
				propertyencryption.EnvelopeProfile{
					Name:                 "customer-pii",
					EncapsulationService: keyService,
					KeyRepository:        newKeyRepository(),
				},
			}
		})
	if err != nil {
		panic(err)
	}
	defer driver.Close(ctx)

	encryption := driver.PropertyEncryption()

	// Data encryption keys are created once, not per value.
	keys, err := encryption.Keys("customer-pii")
	if err != nil {
		panic(err)
	}
	if _, err = keys.Create(ctx, "current", nil); err != nil {
		panic(err)
	}

	// Encrypt, binding the value to the customer it belongs to.
	encrypted, err := encryption.EncryptWithAAD(ctx, propertyencryption.EncryptRequest{
		Value: "078-05-1120",
		Key:   propertyencryption.KeyAlias("current"),
	}, "customer-1")
	if err != nil {
		panic(err)
	}

	// Store the encrypted bytes like any other property.
	_, err = neo4j.ExecuteQuery(ctx, driver,
		"CREATE (c:Customer {id: $id, ssn: $ssn})",
		map[string]any{"id": "customer-1", "ssn": encrypted},
		neo4j.EagerResultTransformer)
	if err != nil {
		panic(err)
	}

	result, err := neo4j.ExecuteQuery(ctx, driver,
		"MATCH (c:Customer {id: $id}) RETURN c.ssn AS ssn",
		map[string]any{"id": "customer-1"},
		neo4j.EagerResultTransformer)
	if err != nil {
		panic(err)
	}
	stored, _, err := neo4j.GetRecordValue[[]byte](result.Records[0], "ssn")
	if err != nil {
		panic(err)
	}

	// Decrypt with the AAD it was encrypted with.
	ssn, err := encryption.DecryptWithAAD(ctx, stored, "customer-1")
	if err != nil {
		panic(err)
	}

	fmt.Println(ssn)
}

// newKeyRepository returns a repository backed by a map. A real one would persist keys, in
// Neo4j or anywhere else the application already stores data.
func newKeyRepository() propertyencryption.EncapsulatedKeyRecordRepository {
	return &exampleKeyRepository{
		keys:    map[string]propertyencryption.EncapsulatedKeyRecord{},
		aliases: map[string]string{},
	}
}

type exampleKeyRepository struct {
	keys    map[string]propertyencryption.EncapsulatedKeyRecord
	aliases map[string]string
	nextID  int
}

func (r *exampleKeyRepository) FindByID(
	_ context.Context, id string) (propertyencryption.EncapsulatedKeyRecord, error) {

	record, ok := r.keys[id]
	if !ok {
		return propertyencryption.EncapsulatedKeyRecord{}, propertyencryption.ErrKeyNotFound
	}
	return record, nil
}

func (r *exampleKeyRepository) FindByAlias(
	_ context.Context, alias string) (propertyencryption.EncapsulatedKeyRecord, error) {

	id, ok := r.aliases[alias]
	if !ok {
		return propertyencryption.EncapsulatedKeyRecord{}, propertyencryption.ErrKeyNotFound
	}
	return r.keys[id], nil
}

func (r *exampleKeyRepository) Create(
	_ context.Context, alias string, encapsulation []byte,
	metadata map[string]string) (propertyencryption.EncapsulatedKeyRecord, error) {

	r.nextID++
	record := propertyencryption.EncapsulatedKeyRecord{
		EncapsulatedKey: propertyencryption.EncapsulatedKey{
			ID:    fmt.Sprintf("key-%d", r.nextID),
			Alias: alias,
		},
		Encapsulation: encapsulation,
		Metadata:      metadata,
	}
	r.keys[record.ID] = record
	if alias != "" {
		r.aliases[alias] = record.ID
	}
	return record, nil
}

func (r *exampleKeyRepository) SetAlias(_ context.Context, id, alias string) error {
	record, ok := r.keys[id]
	if !ok {
		return propertyencryption.ErrKeyNotFound
	}
	delete(r.aliases, record.Alias)
	record.Alias = alias
	r.keys[id] = record
	if alias != "" {
		r.aliases[alias] = id
	}
	return nil
}

func (r *exampleKeyRepository) DeleteByID(_ context.Context, id string) error {
	delete(r.keys, id)
	return nil
}

func getUrl() string {
	return fmt.Sprintf("%s://%s:%s", os.Getenv("TEST_NEO4J_SCHEME"), os.Getenv("TEST_NEO4J_HOST"), os.Getenv("TEST_NEO4J_PORT"))
}
