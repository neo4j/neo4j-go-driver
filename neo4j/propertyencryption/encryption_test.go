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

// memoryRepository is an EncapsulatedKeyRepository backed by a map, counting calls so that
// caching can be asserted.
type memoryRepository struct {
	mutex   sync.Mutex
	keys    map[string]EncapsulatedKey
	aliases map[string]string
	nextID  int

	findByID    int
	findByAlias int
	saves       int
	// err, when set, is returned by every method.
	err error
}

func newMemoryRepository() *memoryRepository {
	return &memoryRepository{keys: map[string]EncapsulatedKey{}, aliases: map[string]string{}}
}

func (r *memoryRepository) FindByID(_ context.Context, id string) (EncapsulatedKey, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.findByID++
	if r.err != nil {
		return EncapsulatedKey{}, r.err
	}
	key, ok := r.keys[id]
	if !ok {
		return EncapsulatedKey{}, ErrKeyNotFound
	}
	return key, nil
}

func (r *memoryRepository) FindByAlias(_ context.Context, alias string) (EncapsulatedKey, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.findByAlias++
	if r.err != nil {
		return EncapsulatedKey{}, r.err
	}
	id, ok := r.aliases[alias]
	if !ok {
		return EncapsulatedKey{}, ErrKeyNotFound
	}
	return r.keys[id], nil
}

func (r *memoryRepository) Save(
	_ context.Context, alias string, encapsulation []byte, metadata map[string]string) (EncapsulatedKey, error) {

	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.saves++
	if r.err != nil {
		return EncapsulatedKey{}, r.err
	}
	id := strconv.Itoa(r.nextID)
	r.nextID++
	key := EncapsulatedKey{ID: id, Alias: alias, Encapsulation: encapsulation, Metadata: metadata}
	r.keys[id] = key
	r.aliases[alias] = id
	return key, nil
}

func (r *memoryRepository) AddAlias(_ context.Context, id, alias string) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.aliases[alias] = id
	return nil
}

func (r *memoryRepository) DeleteAlias(_ context.Context, _, alias string) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	delete(r.aliases, alias)
	return nil
}

func (r *memoryRepository) DeleteByID(_ context.Context, id string) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	delete(r.keys, id)
	return nil
}

func (r *memoryRepository) counts() (findByID, findByAlias int) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.findByID, r.findByAlias
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
	key, err := keys.Create(context.Background(), alias)
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

	encrypted, err := encryption.Encrypt(ctx, EncryptRequest{
		Value: "aad-bound",
		AAD:   "row-42",
		Key:   KeyAlias("k1"),
	})
	if err != nil {
		t.Fatalf("Encrypt returned %v", err)
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
	if _, err := repository.Save(ctx, "k1", result.Encapsulation, result.Metadata); err != nil {
		t.Fatalf("Save returned %v", err)
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
		{name: "float aad", request: EncryptRequest{Value: "a", AAD: 1.5, Key: KeyAlias("k1")}},
		{name: "list aad", request: EncryptRequest{Value: "a", AAD: []any{1}, Key: KeyAlias("k1")}},
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

	t.Run("empty alias", func(t *testing.T) {
		t.Parallel()
		encryption, _, _ := newTestEncryption(t, "p")
		keys, err := encryption.Keys("")
		if err != nil {
			t.Fatalf("Keys returned %v", err)
		}
		if _, err := keys.Create(ctx, ""); err == nil {
			t.Fatal("Create accepted an empty alias")
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
		if _, err := keys.Create(ctx, "k1"); err == nil {
			t.Fatal("Create accepted a key with no id, which could never be decrypted")
		}
	})
}

// idlessRepository returns keys without an id, which would leave every value it encrypted
// undecryptable.
type idlessRepository struct{ *memoryRepository }

func (idlessRepository) Save(
	_ context.Context, alias string, encapsulation []byte, metadata map[string]string) (EncapsulatedKey, error) {
	return EncapsulatedKey{Alias: alias, Encapsulation: encapsulation, Metadata: metadata}, nil
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
				encrypted, err := encryption.Encrypt(ctx, EncryptRequest{
					Value: value, AAD: strconv.Itoa(worker), Key: KeyAlias("k1")})
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
