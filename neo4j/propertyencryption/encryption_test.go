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
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v6/neo4j/db"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j/dbtype"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j/internal/errorutil"
	ipe "github.com/neo4j/neo4j-go-driver/v6/neo4j/internal/propertyencryption"
)

// memoryRepository is an EncapsulatedKeyRecordRepository backed by a map, counting calls so
// caching can be asserted.
type memoryRepository struct {
	mutex   sync.Mutex
	keys    map[string]EncapsulatedKeyRecord
	aliases map[string]string
	nextID  int

	findByID    int
	findByAlias int
	saves       int
	setAliases  int
	deletes     int
	// err, when set, is returned by every method.
	err error
}

func newMemoryRepository() *memoryRepository {
	return &memoryRepository{keys: map[string]EncapsulatedKeyRecord{}, aliases: map[string]string{}}
}

func (r *memoryRepository) FindByID(_ context.Context, id string) (EncapsulatedKeyRecord, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.findByID++
	if r.err != nil {
		return EncapsulatedKeyRecord{}, r.err
	}
	key, ok := r.keys[id]
	if !ok {
		return EncapsulatedKeyRecord{}, ErrKeyNotFound
	}
	return key, nil
}

func (r *memoryRepository) FindByAlias(_ context.Context, alias string) (EncapsulatedKeyRecord, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.findByAlias++
	if r.err != nil {
		return EncapsulatedKeyRecord{}, r.err
	}
	id, ok := r.aliases[alias]
	if !ok {
		return EncapsulatedKeyRecord{}, ErrKeyNotFound
	}
	return r.keys[id], nil
}

func (r *memoryRepository) Create(
	_ context.Context, alias string, encapsulation []byte,
	metadata map[string]string) (EncapsulatedKeyRecord, error) {

	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.saves++
	if r.err != nil {
		return EncapsulatedKeyRecord{}, r.err
	}
	id := strconv.Itoa(r.nextID)
	r.nextID++
	record := EncapsulatedKeyRecord{
		EncapsulatedKey: EncapsulatedKey{ID: id, Alias: alias},
		Encapsulation:   encapsulation,
		Metadata:        metadata,
	}
	r.keys[id] = record
	if alias != "" {
		r.aliases[alias] = id
	}
	return record, nil
}

func (r *memoryRepository) SetAlias(_ context.Context, id, alias string) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.setAliases++
	record, ok := r.keys[id]
	if !ok {
		return ErrKeyNotFound
	}
	if record.Alias != "" {
		delete(r.aliases, record.Alias)
	}
	record.Alias = alias
	r.keys[id] = record
	if alias != "" {
		r.aliases[alias] = id
	}
	return nil
}

func (r *memoryRepository) DeleteByID(_ context.Context, id string) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.deletes++
	record, ok := r.keys[id]
	if !ok {
		return ErrKeyNotFound
	}
	delete(r.keys, id)
	delete(r.aliases, record.Alias)
	return nil
}

func (r *memoryRepository) counts() (findByID, findByAlias int) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.findByID, r.findByAlias
}

func (r *memoryRepository) saveCount() int {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.saves
}

func (r *memoryRepository) writeCounts() (setAliases, deletes int) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.setAliases, r.deletes
}

// countingService counts decapsulations, the call the key cache avoids.
type countingService struct {
	KeyEncapsulationService
	mutex          sync.Mutex
	decapsulations int
	err            error
}

func (s *countingService) Decapsulate(
	ctx context.Context, encapsulation []byte, metadata map[string]string) ([]byte, error) {

	s.mutex.Lock()
	s.decapsulations++
	err := s.err
	s.mutex.Unlock()
	if err != nil {
		return nil, err
	}
	return s.KeyEncapsulationService.Decapsulate(ctx, encapsulation, metadata)
}

func (s *countingService) count() int {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.decapsulations
}

func testKEK() []byte {
	kek := make([]byte, ipe.KeySize)
	for i := range kek {
		kek[i] = byte(i)
	}
	return kek
}

func newTestService(t *testing.T) *countingService {
	t.Helper()
	local, err := NewLocalKeyEncapsulationService(testKEK())
	if err != nil {
		t.Fatalf("NewLocalKeyEncapsulationService returned %v", err)
	}
	return &countingService{KeyEncapsulationService: local}
}

// newTestEncryption builds an Encryption with one profile per name, each with its own
// repository. The first profile's repository is returned.
func newTestEncryption(t *testing.T, names ...string) (*Encryption, *memoryRepository, *countingService) {
	t.Helper()

	service := newTestService(t)
	repository := newMemoryRepository()
	profiles := make([]Profile, 0, len(names))
	for i, name := range names {
		repo := repository
		if i > 0 {
			repo = newMemoryRepository()
		}
		profiles = append(profiles, EnvelopeProfile{
			Name:                 name,
			EncapsulationService: service,
			KeyRepository:        repo,
		})
	}
	encryption, err := New(profiles)
	if err != nil {
		t.Fatalf("New returned %v", err)
	}
	return encryption, repository, service
}

func createKey(t *testing.T, encryption *Encryption, profile, alias string) EncapsulatedKey {
	t.Helper()

	keys, err := encryption.Keys(profile)
	if err != nil {
		t.Fatalf("Keys returned %v", err)
	}
	key, err := keys.Create(context.Background(), alias, nil)
	if err != nil {
		t.Fatalf("Create returned %v", err)
	}
	return key
}

func TestEncryptDecryptRoundTrip(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	encryption, _, _ := newTestEncryption(t, "p")
	createKey(t, encryption, "", "k1")

	values := []any{
		true,
		int64(-42),
		3.25,
		"hello world",
		[]byte{0, 1, 2},
		[]any{int64(1), int64(2)},
		dbtype.UUID{1, 2, 3},
	}

	for _, value := range values {
		t.Run(fmt.Sprintf("%T", value), func(t *testing.T) {
			encrypted, err := encryption.Encrypt(ctx, EncryptRequest{Value: value, Key: KeyAlias("k1")})
			if err != nil {
				t.Fatalf("Encrypt returned %v", err)
			}
			decrypted, err := encryption.Decrypt(ctx, encrypted)
			if err != nil {
				t.Fatalf("Decrypt returned %v", err)
			}
			if fmt.Sprintf("%v", decrypted) != fmt.Sprintf("%v", value) {
				t.Errorf("round tripped %#v to %#v", value, decrypted)
			}
		})
	}
}

func TestEncryptIsNotDeterministic(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	encryption, _, _ := newTestEncryption(t, "p")
	createKey(t, encryption, "", "k1")

	request := EncryptRequest{Value: "hello world", Key: KeyAlias("k1")}
	first, err := encryption.Encrypt(ctx, request)
	if err != nil {
		t.Fatalf("Encrypt returned %v", err)
	}
	second, err := encryption.Encrypt(ctx, request)
	if err != nil {
		t.Fatalf("Encrypt returned %v", err)
	}
	if string(first) == string(second) {
		t.Fatal("encrypting the same value twice produced the same bytes, the iv is not fresh")
	}
}

func TestEncryptByKeyID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	encryption, _, _ := newTestEncryption(t, "p")
	key := createKey(t, encryption, "", "k1")

	encrypted, err := encryption.Encrypt(ctx, EncryptRequest{Value: "by id", Key: KeyID(key.ID)})
	if err != nil {
		t.Fatalf("Encrypt returned %v", err)
	}
	decrypted, err := encryption.Decrypt(ctx, encrypted)
	if err != nil {
		t.Fatalf("Decrypt returned %v", err)
	}
	if decrypted != "by id" {
		t.Errorf("decrypted to %#v", decrypted)
	}
}

func TestDecryptWithAAD(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	encryption, _, _ := newTestEncryption(t, "p")
	createKey(t, encryption, "", "k1")

	encrypted, err := encryption.EncryptWithAAD(ctx, EncryptRequest{
		Value: "aad-bound",
		Key:   KeyAlias("k1"),
	}, "row-42")
	if err != nil {
		t.Fatalf("EncryptWithAAD returned %v", err)
	}

	t.Run("matching aad", func(t *testing.T) {
		decrypted, err := encryption.DecryptWithAAD(ctx, encrypted, "row-42")
		if err != nil {
			t.Fatalf("DecryptWithAAD returned %v", err)
		}
		if decrypted != "aad-bound" {
			t.Errorf("decrypted to %#v", decrypted)
		}
	})

	t.Run("wrong aad", func(t *testing.T) {
		if _, err := encryption.DecryptWithAAD(ctx, encrypted, "row-999"); err == nil {
			t.Fatal("DecryptWithAAD accepted the wrong aad")
		}
	})

	t.Run("persisted aad", func(t *testing.T) {
		decrypted, err := encryption.Decrypt(ctx, encrypted)
		if err != nil {
			t.Fatalf("Decrypt returned %v", err)
		}
		if decrypted != "aad-bound" {
			t.Errorf("decrypted to %#v", decrypted)
		}
	})

	t.Run("nil aad", func(t *testing.T) {
		if _, err := encryption.DecryptWithAAD(ctx, encrypted, nil); err == nil {
			t.Fatal("DecryptWithAAD accepted a nil aad")
		}
	})
}

// TestDecryptWithAADOnAnUnboundValue checks supplying AAD for a value that has none is
// reported as such, not as an authentication failure.
func TestDecryptWithAADOnAnUnboundValue(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	encryption, _, _ := newTestEncryption(t, "p")
	createKey(t, encryption, "", "k1")

	encrypted, err := encryption.Encrypt(ctx, EncryptRequest{Value: "plain", Key: KeyAlias("k1")})
	if err != nil {
		t.Fatalf("Encrypt returned %v", err)
	}
	structure, err := ipe.DecodeEncrypted(encrypted)
	if err != nil {
		t.Fatalf("DecodeEncrypted returned %v", err)
	}
	if _, bound := structure.Metadata.Bytes(ipe.MetadataAAD); bound {
		t.Fatal("Encrypt stored additional authenticated data")
	}

	_, err = encryption.DecryptWithAAD(ctx, encrypted, "row-42")
	if err == nil {
		t.Fatal("DecryptWithAAD accepted an aad for a value that has none")
	}
	var encryptionErr *Error
	if !errors.As(err, &encryptionErr) {
		t.Fatalf("DecryptWithAAD returned %T, want an *Error", err)
	}
}

// TestCachesAvoidRepeatedKeyResolution checks repeated calls reach neither the repository
// nor the key encapsulation service.
func TestCachesAvoidRepeatedKeyResolution(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	encryption, repository, service := newTestEncryption(t, "p")
	createKey(t, encryption, "", "k1")

	// Creating the key already cached it, so nothing below should reach the repository or
	// the key encapsulation service at all.
	beforeID, beforeAlias := repository.counts()
	beforeDecapsulations := service.count()

	for i := 0; i < 50; i++ {
		encrypted, err := encryption.Encrypt(ctx, EncryptRequest{Value: i, Key: KeyAlias("k1")})
		if err != nil {
			t.Fatalf("Encrypt returned %v", err)
		}
		if _, err := encryption.Decrypt(ctx, encrypted); err != nil {
			t.Fatalf("Decrypt returned %v", err)
		}
	}

	afterID, afterAlias := repository.counts()
	if afterID != beforeID || afterAlias != beforeAlias {
		t.Errorf("the repository was consulted %d more times by id and %d more by alias, want none",
			afterID-beforeID, afterAlias-beforeAlias)
	}
	if got := service.count() - beforeDecapsulations; got != 0 {
		t.Errorf("the key was decapsulated %d more times, want none", got)
	}
}

// TestResolvingAnAliasDecapsulatesOnce checks the alias path does not fetch the same key
// twice, once by alias and again by id.
func TestResolvingAnAliasDecapsulatesOnce(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	service := newTestService(t)
	repository := newMemoryRepository()
	encryption, err := New([]Profile{EnvelopeProfile{
		Name:                 "p",
		EncapsulationService: service,
		KeyRepository:        repository,
	}})
	if err != nil {
		t.Fatalf("New returned %v", err)
	}

	// Seed the repository directly so nothing is cached, the way a second process would find
	// a key created elsewhere.
	result, err := service.Encapsulate(ctx, nil)
	if err != nil {
		t.Fatalf("Encapsulate returned %v", err)
	}
	if _, err := repository.Create(ctx, "k1", result.Encapsulation, result.Metadata); err != nil {
		t.Fatalf("Create returned %v", err)
	}

	if _, err := encryption.Encrypt(ctx, EncryptRequest{Value: "a", Key: KeyAlias("k1")}); err != nil {
		t.Fatalf("Encrypt returned %v", err)
	}
	if got := service.count(); got != 1 {
		t.Errorf("the key was decapsulated %d times for one alias lookup, want 1", got)
	}
	findByID, findByAlias := repository.counts()
	if findByAlias != 1 || findByID != 0 {
		t.Errorf("the repository saw %d alias and %d id lookups, want 1 and 0", findByAlias, findByID)
	}
}

func TestEncryptRejects(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	encryption, _, _ := newTestEncryption(t, "p")
	createKey(t, encryption, "", "k1")

	tests := []struct {
		name    string
		request EncryptRequest
	}{
		{name: "no key", request: EncryptRequest{Value: "a"}},
		{name: "unknown alias", request: EncryptRequest{Value: "a", Key: KeyAlias("nope")}},
		{name: "unknown id", request: EncryptRequest{Value: "a", Key: KeyID("nope")}},
		{name: "unknown profile", request: EncryptRequest{Value: "a", Key: KeyAlias("k1"), Profile: "nope"}},
		{name: "nil value", request: EncryptRequest{Key: KeyAlias("k1")}},
		{name: "map value", request: EncryptRequest{Value: map[string]any{}, Key: KeyAlias("k1")}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if _, err := encryption.Encrypt(ctx, test.request); err == nil {
				t.Fatal("Encrypt succeeded, want an error")
			}
		})
	}
}

func TestEncryptWithAADRejects(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	encryption, _, _ := newTestEncryption(t, "p")
	createKey(t, encryption, "", "k1")

	tests := []struct {
		name string
		aad  any
	}{
		{name: "nil", aad: nil},
		{name: "float", aad: 1.5},
		{name: "list", aad: []any{1}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if _, err := encryption.EncryptWithAAD(ctx, EncryptRequest{
				Value: "a", Key: KeyAlias("k1")}, test.aad); err == nil {
				t.Fatal("EncryptWithAAD succeeded, want an error")
			}
		})
	}
}

func TestDecryptRejects(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	encryption, _, _ := newTestEncryption(t, "p")
	createKey(t, encryption, "", "k1")

	encrypted, err := encryption.Encrypt(ctx, EncryptRequest{Value: "a", Key: KeyAlias("k1")})
	if err != nil {
		t.Fatalf("Encrypt returned %v", err)
	}

	t.Run("empty", func(t *testing.T) {
		t.Parallel()
		if _, err := encryption.Decrypt(ctx, nil); err == nil {
			t.Fatal("Decrypt accepted no bytes")
		}
	})
	t.Run("not an encrypted value", func(t *testing.T) {
		t.Parallel()
		if _, err := encryption.Decrypt(ctx, []byte("just a string")); err == nil {
			t.Fatal("Decrypt accepted arbitrary bytes")
		}
	})
	t.Run("tampered", func(t *testing.T) {
		t.Parallel()
		tampered := append([]byte(nil), encrypted...)
		tampered[len(tampered)-1] ^= 0xff
		if _, err := encryption.Decrypt(ctx, tampered); err == nil {
			t.Fatal("Decrypt accepted an altered value")
		}
	})
	t.Run("unknown profile", func(t *testing.T) {
		t.Parallel()
		other, _, _ := newTestEncryption(t, "different")
		if _, err := other.Decrypt(ctx, encrypted); err == nil {
			t.Fatal("Decrypt accepted a value from a profile it does not have")
		}
	})
	t.Run("unknown key", func(t *testing.T) {
		t.Parallel()
		// A profile of the same name but a repository that has never seen the key.
		other, _, _ := newTestEncryption(t, "p")
		if _, err := other.Decrypt(ctx, encrypted); err == nil {
			t.Fatal("Decrypt accepted a value whose key is unknown")
		}
	})
}

// TestDecryptWithADifferentKeyFails checks two profiles configured alike but with unrelated
// keys cannot read each other's values.
func TestDecryptWithADifferentKeyFails(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	first, _, _ := newTestEncryption(t, "p")
	createKey(t, first, "", "k1")
	encrypted, err := first.Encrypt(ctx, EncryptRequest{Value: "secret", Key: KeyAlias("k1")})
	if err != nil {
		t.Fatalf("Encrypt returned %v", err)
	}

	second, _, _ := newTestEncryption(t, "p")
	createKey(t, second, "", "k1")
	if _, err := second.Decrypt(ctx, encrypted); err == nil {
		t.Fatal("a value decrypted under an unrelated key")
	}
}

func TestProfileSelection(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("one profile may be left unnamed", func(t *testing.T) {
		t.Parallel()
		encryption, _, _ := newTestEncryption(t, "only")
		createKey(t, encryption, "", "k1")
		if _, err := encryption.Encrypt(ctx, EncryptRequest{Value: "a", Key: KeyAlias("k1")}); err != nil {
			t.Fatalf("Encrypt returned %v", err)
		}
	})

	t.Run("two profiles must be named", func(t *testing.T) {
		t.Parallel()
		encryption, _, _ := newTestEncryption(t, "p1", "p2")
		if _, err := encryption.Keys(""); err == nil {
			t.Fatal("Keys accepted no profile name with two profiles configured")
		}
		_, err := encryption.Encrypt(ctx, EncryptRequest{Value: "a", Key: KeyAlias("k1")})
		if err == nil {
			t.Fatal("Encrypt accepted no profile name with two profiles configured")
		}
	})

	t.Run("a key belongs to one profile", func(t *testing.T) {
		t.Parallel()
		encryption, _, _ := newTestEncryption(t, "p1", "p2")
		createKey(t, encryption, "p1", "k1")
		_, err := encryption.Encrypt(ctx, EncryptRequest{
			Value: "a", Key: KeyAlias("k1"), Profile: "p2",
		})
		if err == nil {
			t.Fatal("a key created in p1 was usable from p2")
		}
	})

	t.Run("no profiles configured", func(t *testing.T) {
		t.Parallel()
		encryption, err := New(nil)
		if err != nil {
			t.Fatalf("New returned %v", err)
		}
		if _, err := encryption.Keys(""); err == nil {
			t.Fatal("Keys succeeded with no profiles configured")
		}
		if _, err := encryption.Encrypt(ctx, EncryptRequest{Value: "a", Key: KeyAlias("k")}); err == nil {
			t.Fatal("Encrypt succeeded with no profiles configured")
		}
	})
}

func TestNewRejectsBadProfiles(t *testing.T) {
	t.Parallel()

	service := newTestService(t)
	repository := newMemoryRepository()
	valid := EnvelopeProfile{Name: "p", EncapsulationService: service, KeyRepository: repository}

	tests := []struct {
		name     string
		profiles []Profile
	}{
		{name: "nil profile", profiles: []Profile{nil}},
		{name: "no name", profiles: []Profile{EnvelopeProfile{
			EncapsulationService: service, KeyRepository: repository}}},
		{name: "no encapsulation service", profiles: []Profile{EnvelopeProfile{
			Name: "p", KeyRepository: repository}}},
		{name: "no key repository", profiles: []Profile{EnvelopeProfile{
			Name: "p", EncapsulationService: service}}},
		{name: "duplicate names", profiles: []Profile{valid, valid}},
		{name: "negative cache ttl", profiles: []Profile{EnvelopeProfile{
			Name: "p", EncapsulationService: service, KeyRepository: repository, KeyCacheTTL: -1}}},
		{name: "negative cache size", profiles: []Profile{EnvelopeProfile{
			Name: "p", EncapsulationService: service, KeyRepository: repository, KeyCacheSize: -1}}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if _, err := New(test.profiles); err == nil {
				t.Fatal("New accepted the profile, want an error")
			}
		})
	}
}

// TestDriverErrorsFromCallbacksArePropagated checks a driver error raised by a callback
// reaches the caller unwrapped, so that a retryable failure stays recognisable.
func TestDriverErrorsFromCallbacksArePropagated(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	neo4jErr := &db.Neo4jError{Code: "Neo.TransientError.General.DatabaseUnavailable"}

	t.Run("from the repository", func(t *testing.T) {
		t.Parallel()

		service := newTestService(t)
		repository := newMemoryRepository()
		encryption, err := New([]Profile{EnvelopeProfile{
			Name: "p", EncapsulationService: service, KeyRepository: repository}})
		if err != nil {
			t.Fatalf("New returned %v", err)
		}
		repository.err = neo4jErr

		_, err = encryption.Encrypt(ctx, EncryptRequest{Value: "a", Key: KeyAlias("k1")})
		if !errors.Is(err, neo4jErr) {
			t.Fatalf("Encrypt returned %v, want the repository's own error", err)
		}
		var wrapped *Error
		if errors.As(err, &wrapped) {
			t.Error("the driver error was wrapped, which would hide that it is retryable")
		}
	})

	t.Run("from the key encapsulation service", func(t *testing.T) {
		t.Parallel()

		service := newTestService(t)
		repository := newMemoryRepository()
		encryption, err := New([]Profile{EnvelopeProfile{
			Name: "p", EncapsulationService: service, KeyRepository: repository}})
		if err != nil {
			t.Fatalf("New returned %v", err)
		}
		createKey(t, encryption, "", "k1")

		// Force a cache miss so the service is consulted again.
		state := encryption.profiles["p"]
		state.keyCache = ipe.NewCache[*ipe.DataKey](DefaultKeyCacheTTL, DefaultKeyCacheSize)
		service.err = neo4jErr

		_, err = encryption.Encrypt(ctx, EncryptRequest{Value: "a", Key: KeyID("0")})
		if !errors.Is(err, neo4jErr) {
			t.Fatalf("Encrypt returned %v, want the service's own error", err)
		}
	})
}

func TestNonDriverErrorsFromCallbacksAreWrapped(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	service := newTestService(t)
	repository := newMemoryRepository()
	encryption, err := New([]Profile{EnvelopeProfile{
		Name: "p", EncapsulationService: service, KeyRepository: repository}})
	if err != nil {
		t.Fatalf("New returned %v", err)
	}

	plain := errors.New("the key store is on fire")
	repository.err = plain

	_, err = encryption.Encrypt(ctx, EncryptRequest{Value: "a", Key: KeyAlias("k1")})
	var wrapped *Error
	if !errors.As(err, &wrapped) {
		t.Fatalf("Encrypt returned %T, want an *Error", err)
	}
	if !errors.Is(err, plain) {
		t.Error("the original error was not preserved as the cause")
	}
}

// TestUsageErrorsArePropagated covers the other driver error a callback might raise.
func TestUsageErrorsArePropagated(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	service := newTestService(t)
	repository := newMemoryRepository()
	encryption, err := New([]Profile{EnvelopeProfile{
		Name: "p", EncapsulationService: service, KeyRepository: repository}})
	if err != nil {
		t.Fatalf("New returned %v", err)
	}
	usageErr := &errorutil.UsageError{Message: "bad usage"}
	repository.err = usageErr

	_, err = encryption.Encrypt(ctx, EncryptRequest{Value: "a", Key: KeyAlias("k1")})
	if !errors.Is(err, usageErr) {
		t.Fatalf("Encrypt returned %v, want the usage error", err)
	}
}

func TestKeyManagerCreate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// An empty alias leaves the key reachable only by id.
	t.Run("empty alias", func(t *testing.T) {
		t.Parallel()
		encryption, _, _ := newTestEncryption(t, "p")
		keys, err := encryption.Keys("")
		if err != nil {
			t.Fatalf("Keys returned %v", err)
		}
		key, err := keys.Create(ctx, "", nil)
		if err != nil {
			t.Fatalf("Create returned %v", err)
		}
		if key.ID == "" {
			t.Fatal("Create returned a key with no id")
		}
		if key.Alias != "" {
			t.Errorf("Alias is %q, want empty", key.Alias)
		}
		if _, err := encryption.Encrypt(ctx, EncryptRequest{
			Value: "a", Key: KeyID(key.ID)}); err != nil {
			t.Errorf("an unbound key could not encrypt: %v", err)
		}
		if _, err := encryption.Encrypt(ctx, EncryptRequest{
			Value: "a", Key: KeyAlias("")}); err == nil {
			t.Error("an unbound key was reachable by empty alias")
		}
	})

	t.Run("rotation keeps old values readable", func(t *testing.T) {
		t.Parallel()

		encryption, _, _ := newTestEncryption(t, "p")
		createKey(t, encryption, "", "k1")
		before, err := encryption.Encrypt(ctx, EncryptRequest{Value: "old", Key: KeyAlias("k1")})
		if err != nil {
			t.Fatalf("Encrypt returned %v", err)
		}

		// Creating the alias again makes a new key and moves the alias to it.
		createKey(t, encryption, "", "k1")
		after, err := encryption.Encrypt(ctx, EncryptRequest{Value: "new", Key: KeyAlias("k1")})
		if err != nil {
			t.Fatalf("Encrypt returned %v", err)
		}

		decrypted, err := encryption.Decrypt(ctx, before)
		if err != nil {
			t.Fatalf("a value encrypted before rotation no longer decrypts: %v", err)
		}
		if decrypted != "old" {
			t.Errorf("decrypted to %#v", decrypted)
		}
		if decrypted, err = encryption.Decrypt(ctx, after); err != nil || decrypted != "new" {
			t.Errorf("decrypted to %#v with %v", decrypted, err)
		}
	})

	t.Run("repository without an id", func(t *testing.T) {
		t.Parallel()

		service := newTestService(t)
		encryption, err := New([]Profile{EnvelopeProfile{
			Name: "p", EncapsulationService: service, KeyRepository: idlessRepository{newMemoryRepository()},
		}})
		if err != nil {
			t.Fatalf("New returned %v", err)
		}
		keys, err := encryption.Keys("")
		if err != nil {
			t.Fatalf("Keys returned %v", err)
		}
		if _, err := keys.Create(ctx, "k1", nil); err == nil {
			t.Fatal("Create accepted a key with no id, which could never be decrypted")
		}
	})

	t.Run("encapsulation service returns a key that is not AES-256", func(t *testing.T) {
		t.Parallel()

		for _, size := range []int{0, 16, 31, 33} {
			repository := newMemoryRepository()
			encryption, err := New([]Profile{EnvelopeProfile{
				Name:                 "p",
				EncapsulationService: shortKeyService{size: size},
				KeyRepository:        repository,
			}})
			if err != nil {
				t.Fatalf("New returned %v", err)
			}
			keys, err := encryption.Keys("")
			if err != nil {
				t.Fatalf("Keys returned %v", err)
			}
			if _, err := keys.Create(ctx, "k1", nil); err == nil {
				t.Errorf("Create accepted a %d byte key", size)
			}
			if repository.saveCount() != 0 {
				t.Errorf("a %d byte key reached the repository", size)
			}
		}
	})
}

// shortKeyService returns a key of the wrong size.
type shortKeyService struct {
	KeyEncapsulationService
	size int
}

func (s shortKeyService) Encapsulate(
	_ context.Context, _ map[string]string) (KeyEncapsulationResult, error) {

	return KeyEncapsulationResult{
		Key:           make([]byte, s.size),
		Encapsulation: []byte{1, 2, 3},
		Metadata:      map[string]string{},
	}, nil
}

// idlessRepository returns keys without an id, which would leave every value it encrypted
// undecryptable.
type idlessRepository struct{ *memoryRepository }

func (idlessRepository) Create(
	_ context.Context, alias string, encapsulation []byte,
	metadata map[string]string) (EncapsulatedKeyRecord, error) {

	return EncapsulatedKeyRecord{
		EncapsulatedKey: EncapsulatedKey{Alias: alias},
		Encapsulation:   encapsulation,
		Metadata:        metadata,
	}, nil
}

func TestEncryptionIsSafeForConcurrentUse(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	encryption, _, _ := newTestEncryption(t, "p")
	createKey(t, encryption, "", "k1")

	var waitGroup sync.WaitGroup
	errs := make(chan error, 64)
	for worker := 0; worker < 8; worker++ {
		waitGroup.Add(1)
		go func(worker int) {
			defer waitGroup.Done()
			for i := 0; i < 100; i++ {
				value := fmt.Sprintf("worker %d value %d", worker, i)
				encrypted, err := encryption.EncryptWithAAD(ctx, EncryptRequest{
					Value: value, Key: KeyAlias("k1")}, strconv.Itoa(worker))
				if err != nil {
					errs <- err
					return
				}
				decrypted, err := encryption.DecryptWithAAD(ctx, encrypted, strconv.Itoa(worker))
				if err != nil {
					errs <- err
					return
				}
				if decrypted != value {
					errs <- fmt.Errorf("decrypted %q to %#v", value, decrypted)
					return
				}
			}
		}(worker)
	}
	waitGroup.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestLocalKeyEncapsulationServiceRejectsShortKeys(t *testing.T) {
	t.Parallel()

	for _, size := range []int{0, 16, 24, 31, 33} {
		if _, err := NewLocalKeyEncapsulationService(make([]byte, size)); err == nil {
			t.Errorf("NewLocalKeyEncapsulationService accepted a %d byte key", size)
		}
	}
}

func TestLocalKeyEncapsulationServiceCopiesTheKey(t *testing.T) {
	t.Parallel()

	kek := testKEK()
	service, err := NewLocalKeyEncapsulationService(kek)
	if err != nil {
		t.Fatalf("NewLocalKeyEncapsulationService returned %v", err)
	}
	result, err := service.Encapsulate(context.Background(), nil)
	if err != nil {
		t.Fatalf("Encapsulate returned %v", err)
	}

	// Clearing the caller's slice must not affect the service.
	for i := range kek {
		kek[i] = 0
	}
	if _, err := service.Decapsulate(context.Background(), result.Encapsulation, result.Metadata); err != nil {
		t.Fatalf("Decapsulate returned %v after the caller cleared its key: %v", err, err)
	}
}

func TestLocalKeyEncapsulationServiceRejectsBadMetadata(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	service, err := NewLocalKeyEncapsulationService(testKEK())
	if err != nil {
		t.Fatalf("NewLocalKeyEncapsulationService returned %v", err)
	}
	result, err := service.Encapsulate(ctx, nil)
	if err != nil {
		t.Fatalf("Encapsulate returned %v", err)
	}

	if _, err := service.Decapsulate(ctx, result.Encapsulation, nil); err == nil {
		t.Error("Decapsulate accepted metadata with no iv")
	}
	if _, err := service.Decapsulate(ctx, result.Encapsulation, map[string]string{"iv": "not base64!"}); err == nil {
		t.Error("Decapsulate accepted an unreadable iv")
	}
}

func TestErrorUnwraps(t *testing.T) {
	t.Parallel()

	cause := errors.New("underlying")
	err := &Error{Message: "context", Cause: cause}
	if err.Error() != "context: underlying" {
		t.Errorf("Error() is %q", err.Error())
	}
	if !errors.Is(err, cause) {
		t.Error("the cause is not reachable through errors.Is")
	}
	if got := (&Error{Message: "alone"}).Error(); got != "alone" {
		t.Errorf("Error() with no cause is %q", got)
	}
}

// TestKeyManagerFindByAlias covers the lookup and the missing-alias error.
func TestKeyManagerFindByAlias(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	encryption, _, _ := newTestEncryption(t, "p")
	created := createKey(t, encryption, "", "k1")

	keys, err := encryption.Keys("")
	if err != nil {
		t.Fatalf("Keys returned %v", err)
	}

	found, err := keys.FindByAlias(ctx, "k1")
	if err != nil {
		t.Fatalf("FindByAlias returned %v", err)
	}
	if found.ID != created.ID || found.Alias != "k1" {
		t.Errorf("found %+v, want id %s alias k1", found, created.ID)
	}

	_, err = keys.FindByAlias(ctx, "nope")
	if err == nil {
		t.Fatal("FindByAlias accepted an unknown alias")
	}
	if !errors.Is(err, ErrKeyNotFound) {
		t.Errorf("FindByAlias returned %v, want it to wrap ErrKeyNotFound", err)
	}
}

// TestKeyManagerSetAlias moves an alias between keys and checks the cached mapping does not
// outlive the move.
func TestKeyManagerSetAlias(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	encryption, repository, _ := newTestEncryption(t, "p")
	first := createKey(t, encryption, "", "current")
	second := createKey(t, encryption, "", "next")

	keys, err := encryption.Keys("")
	if err != nil {
		t.Fatalf("Keys returned %v", err)
	}

	// Caches the alias mapping, so a stale one would be used below.
	if _, err := encryption.Encrypt(ctx, EncryptRequest{
		Value: "a", Key: KeyAlias("current")}); err != nil {
		t.Fatalf("Encrypt returned %v", err)
	}

	if err := keys.SetAlias(ctx, second.ID, "current"); err != nil {
		t.Fatalf("SetAlias returned %v", err)
	}
	found, err := keys.FindByAlias(ctx, "current")
	if err != nil {
		t.Fatalf("FindByAlias returned %v", err)
	}
	if found.ID != second.ID {
		t.Errorf("current resolves to %s, want %s", found.ID, second.ID)
	}

	encrypted, err := encryption.Encrypt(ctx, EncryptRequest{
		Value: "a", Key: KeyAlias("current")})
	if err != nil {
		t.Fatalf("Encrypt returned %v", err)
	}
	structure, err := ipe.DecodeEncrypted(encrypted)
	if err != nil {
		t.Fatalf("DecodeEncrypted returned %v", err)
	}
	keyID, _ := structure.Metadata.String(ipe.MetadataKeyID)
	if keyID != second.ID {
		t.Errorf("encrypted under %s, want the rotated-to key %s", keyID, second.ID)
	}
	if keyID == first.ID {
		t.Error("the alias index served the key the alias moved away from")
	}

	if err := keys.SetAlias(ctx, second.ID, ""); err == nil {
		t.Error("SetAlias accepted an empty alias, DeleteAlias is the way to unbind")
	}
	writes, _ := repository.writeCounts()
	if err := keys.SetAlias(ctx, "", "x"); err == nil {
		t.Error("SetAlias accepted an empty id")
	}
	if after, _ := repository.writeCounts(); after != writes {
		t.Error("SetAlias with an empty id reached the repository")
	}
	if err := keys.SetAlias(ctx, "no-such-key", "x"); err == nil {
		t.Error("SetAlias accepted an unknown id")
	}
}

// TestKeyManagerDeleteAlias unbinds an alias and leaves the key reachable by id.
func TestKeyManagerDeleteAlias(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	encryption, _, _ := newTestEncryption(t, "p")
	key := createKey(t, encryption, "", "k1")
	keys, err := encryption.Keys("")
	if err != nil {
		t.Fatalf("Keys returned %v", err)
	}

	if err := keys.DeleteAlias(ctx, key.ID); err != nil {
		t.Fatalf("DeleteAlias returned %v", err)
	}
	if _, err := keys.FindByAlias(ctx, "k1"); err == nil {
		t.Error("the alias still resolves after DeleteAlias")
	}
	if _, err := encryption.Encrypt(ctx, EncryptRequest{
		Value: "a", Key: KeyAlias("k1")}); err == nil {
		t.Error("encrypting by the deleted alias succeeded")
	}
	if _, err := encryption.Encrypt(ctx, EncryptRequest{
		Value: "a", Key: KeyID(key.ID)}); err != nil {
		t.Errorf("the key is no longer reachable by id: %v", err)
	}
}

// TestKeyManagerDeleteByID removes the key and makes its values undecryptable.
func TestKeyManagerDeleteByID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	encryption, repository, _ := newTestEncryption(t, "p")
	key := createKey(t, encryption, "", "k1")
	keys, err := encryption.Keys("")
	if err != nil {
		t.Fatalf("Keys returned %v", err)
	}

	encrypted, err := encryption.Encrypt(ctx, EncryptRequest{
		Value: "a", Key: KeyAlias("k1")})
	if err != nil {
		t.Fatalf("Encrypt returned %v", err)
	}

	if err := keys.DeleteByID(ctx, key.ID); err != nil {
		t.Fatalf("DeleteByID returned %v", err)
	}
	if _, err := encryption.Decrypt(ctx, encrypted); err == nil {
		t.Error("a value decrypted after its key was deleted, so the cache outlived the key")
	}
	if err := keys.DeleteByID(ctx, "no-such-key"); err == nil {
		t.Error("DeleteByID accepted an unknown id")
	}
	_, deletes := repository.writeCounts()
	if err := keys.DeleteByID(ctx, ""); err == nil {
		t.Error("DeleteByID accepted an empty id")
	}
	if _, after := repository.writeCounts(); after != deletes {
		t.Error("DeleteByID with an empty id reached the repository")
	}
}

// TestDisableKeyCacheResolvesEveryTime checks nothing is held in memory between calls.
func TestDisableKeyCacheResolvesEveryTime(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	service := newTestService(t)
	repository := newMemoryRepository()
	encryption, err := New([]Profile{EnvelopeProfile{
		Name:                 "p",
		EncapsulationService: service,
		KeyRepository:        repository,
		DisableKeyCache:      true,
	}})
	if err != nil {
		t.Fatalf("New returned %v", err)
	}
	keys, err := encryption.Keys("")
	if err != nil {
		t.Fatalf("Keys returned %v", err)
	}
	if _, err := keys.Create(ctx, "k1", nil); err != nil {
		t.Fatalf("Create returned %v", err)
	}

	before := service.count()
	for i := 0; i < 3; i++ {
		if _, err := encryption.Encrypt(ctx, EncryptRequest{
			Value: "a", Key: KeyAlias("k1")}); err != nil {
			t.Fatalf("Encrypt returned %v", err)
		}
	}
	if got := service.count() - before; got != 3 {
		t.Errorf("the key was decapsulated %d times for 3 encrypts, want 3", got)
	}

	_, findByAlias := repository.counts()
	if findByAlias < 3 {
		t.Errorf("the repository saw %d alias lookups for 3 encrypts, want at least 3", findByAlias)
	}
}

// TestResolveByAliasDropsAStaleMapping drops an alias mapping once its key has left the key
// cache.
func TestResolveByAliasDropsAStaleMapping(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	encryption, repository, _ := newTestEncryption(t, "p")
	key := createKey(t, encryption, "", "k1")

	state, err := encryption.profileFor("")
	if err != nil {
		t.Fatalf("profileFor returned %v", err)
	}
	if _, ok := state.aliasIndex.Get("k1"); !ok {
		t.Fatal("creating the key did not cache its alias mapping")
	}

	// As the key would leave on eviction or expiry.
	state.keyCache.Remove(key.ID)
	repository.err = ErrKeyNotFound

	if _, err := encryption.Encrypt(ctx, EncryptRequest{
		Value: "a", Key: KeyAlias("k1")}); err == nil {
		t.Fatal("Encrypt succeeded with an unresolvable alias")
	}
	if _, ok := state.aliasIndex.Get("k1"); ok {
		t.Error("the alias mapping survived its key leaving the key cache")
	}
}
