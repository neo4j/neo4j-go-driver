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
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"testing"
)

// The key material TestKit's deterministic fixtures are built with.
const (
	deterministicKEK = "f0de94eb5a2d4da6f17ea74b14e9e556" +
		"d367cb22b053e01798aa2677bfcf5761"
	deterministicEncapsulation = "9e1f562dee78c6c2d47f4378d2949774" +
		"c3a56339b824abaf276c4ca7fcf5a8cd" +
		"63976ae348104d6757b9e419bf9ea325"
	deterministicKeyIV  = "P02Pc7vInYIQ7k93"
	deterministicKeyID  = "testkit-key"
	deterministicName   = "deterministic"
	deterministicDEKHex = "9a108cc9bfff252dba716c60dfb3dfcc1194b03b24c1373bcf266882f3d6156b"
)

type deterministicFixture struct {
	value     any
	iv        string
	aad       any
	encrypted string
}

// TestKit's deterministic fixtures.
var deterministicFixtures = []deterministicFixture{
	{
		value: true,
		iv:    "000102030405060708090a0b",
		encrypted: "01b86588454e56454c4f5045018d6465" +
			"7465726d696e6973746963cc11766afe" +
			"8a94ffb2fd0e0bc10caed2471a028742" +
			"4f4f4c45414e0100a2826976cc0c0001" +
			"02030405060708090a0b866b65795f69" +
			"648b746573746b69742d6b6579",
	},
	{
		value: int64(32768),
		iv:    "0c0d0e0f1011121314151617",
		encrypted: "01b86588454e56454c4f5045018d6465" +
			"7465726d696e6973746963cc1569a784" +
			"263a80ca0892009f92ad16223f54f9ea" +
			"d06187494e54454745520100a2826976" +
			"cc0c0c0d0e0f1011121314151617866b" +
			"65795f69648b746573746b69742d6b65" +
			"79",
	},
	{
		value: 3.25,
		iv:    "18191a1b1c1d1e1f20212223",
		encrypted: "01b86588454e56454c4f5045018d6465" +
			"7465726d696e6973746963cc1992e03a" +
			"fb3af69544000a301a57d5f17255f243" +
			"10a3dabb5e6885464c4f41540100a282" +
			"6976cc0c18191a1b1c1d1e1f20212223" +
			"866b65795f69648b746573746b69742d" +
			"6b6579",
	},
	{
		value: "hello world",
		iv:    "2425262728292a2b2c2d2e2f",
		encrypted: "01b86588454e56454c4f5045018d6465" +
			"7465726d696e6973746963cc1c1c78d1" +
			"da0e71dc01f2a5d683cb925ef41e615f" +
			"6258c77f6ff2dda96486535452494e47" +
			"0100a2826976cc0c2425262728292a2b" +
			"2c2d2e2f866b65795f69648b74657374" +
			"6b69742d6b6579",
	},
	{
		value: []byte{0, 1, 2},
		iv:    "303132333435363738393a3b",
		encrypted: "01b86588454e56454c4f5045018d6465" +
			"7465726d696e6973746963cc15e2719b" +
			"8ded9e86fe2871fe9ee51e3730acd0ce" +
			"9e118542595445530100a2826976cc0c" +
			"303132333435363738393a3b866b6579" +
			"5f69648b746573746b69742d6b6579",
	},
	{
		value: []any{int64(1), int64(2)},
		iv:    "3c3d3e3f4041424344454647",
		encrypted: "01b86588454e56454c4f5045018d6465" +
			"7465726d696e6973746963cc131a8297" +
			"1faa128ae4267f07af8a7de4a80728f6" +
			"844c4953540100a2826976cc0c3c3d3e" +
			"3f4041424344454647866b65795f6964" +
			"8b746573746b69742d6b6579",
	},
	{
		value: "aad-bound",
		iv:    "48494a4b4c4d4e4f50515253",
		aad:   "row-42",
		encrypted: "01b86588454e56454c4f5045018d6465" +
			"7465726d696e6973746963cc1a9e19aa" +
			"f51fbb711fdeb241272be57efcd076c5" +
			"6200e29a4e44da86535452494e470100" +
			"a583616164cc0786726f772d3432d019" +
			"6161645f656e636f64696e675f736368" +
			"656d655f6d616a6f7201d0196161645f" +
			"656e636f64696e675f736368656d655f" +
			"6d696e6f7200826976cc0c48494a4b4c" +
			"4d4e4f50515253866b65795f69648b74" +
			"6573746b69742d6b6579",
	},
}

// TestUnwrapKeyMatchesFixture checks the local key encapsulation format, which other drivers
// must be able to produce and consume.
func TestUnwrapKeyMatchesFixture(t *testing.T) {
	t.Parallel()

	dek := unwrapFixtureKey(t)
	if got := hex.EncodeToString(dek); got != deterministicDEKHex {
		t.Fatalf("unwrapped to %s, want %s", got, deterministicDEKHex)
	}
}

// TestWrapKeyRoundTrip covers wrapping and unwrapping a freshly generated key.
func TestWrapKeyRoundTrip(t *testing.T) {
	t.Parallel()

	kek := mustHex(t, deterministicKEK)
	dek, err := NewDEK()
	if err != nil {
		t.Fatalf("NewDEK returned %v", err)
	}

	encapsulation, iv, err := WrapKey(kek, dek)
	if err != nil {
		t.Fatalf("WrapKey returned %v", err)
	}
	if len(iv) != IVSize {
		t.Errorf("iv is %d bytes, want %d", len(iv), IVSize)
	}
	if len(encapsulation) != KeySize+TagSize {
		t.Errorf("encapsulation is %d bytes, want %d", len(encapsulation), KeySize+TagSize)
	}

	unwrapped, err := UnwrapKey(kek, encapsulation, iv)
	if err != nil {
		t.Fatalf("UnwrapKey returned %v", err)
	}
	if !bytes.Equal(unwrapped, dek) {
		t.Errorf("unwrapped to %x, want %x", unwrapped, dek)
	}
}

// TestWrapKeyRejects covers a wrong key, a wrong size and a tampered encapsulation.
func TestWrapKeyRejects(t *testing.T) {
	t.Parallel()

	kek := mustHex(t, deterministicKEK)
	encapsulation := mustHex(t, deterministicEncapsulation)
	iv := mustBase64(t, deterministicKeyIV)

	t.Run("short key encryption key", func(t *testing.T) {
		t.Parallel()
		if _, _, err := WrapKey(kek[:16], make([]byte, KeySize)); err == nil {
			t.Fatal("WrapKey accepted a 128 bit key encryption key")
		}
	})
	t.Run("wrong key encryption key", func(t *testing.T) {
		t.Parallel()
		wrong := append([]byte(nil), kek...)
		wrong[0] ^= 0xff
		_, err := UnwrapKey(wrong, encapsulation, iv)
		if !errors.Is(err, ErrAuthentication) {
			t.Fatalf("UnwrapKey returned %v, want ErrAuthentication", err)
		}
	})
	t.Run("tampered encapsulation", func(t *testing.T) {
		t.Parallel()
		tampered := append([]byte(nil), encapsulation...)
		tampered[0] ^= 0xff
		_, err := UnwrapKey(kek, tampered, iv)
		if !errors.Is(err, ErrAuthentication) {
			t.Fatalf("UnwrapKey returned %v, want ErrAuthentication", err)
		}
	})
	t.Run("wrong iv size", func(t *testing.T) {
		t.Parallel()
		if _, err := UnwrapKey(kek, encapsulation, iv[:8]); err == nil {
			t.Fatal("UnwrapKey accepted an 8 byte iv")
		}
	})
}

// TestEncryptsToKnownBytes checks the exact bytes produced with the key and initialisation
// vector both pinned, covering value encoding, the cipher, metadata and structure encoding.
func TestEncryptsToKnownBytes(t *testing.T) {
	t.Parallel()

	key, err := NewDataKey(unwrapFixtureKey(t))
	if err != nil {
		t.Fatalf("NewDataKey returned %v", err)
	}

	for _, fixture := range deterministicFixtures {
		t.Run(fixture.encrypted[:24], func(t *testing.T) {
			t.Parallel()

			encoded, err := EncodeValue(fixture.value)
			if err != nil {
				t.Fatalf("EncodeValue(%#v) returned %v", fixture.value, err)
			}

			var metadata Metadata
			metadata.SetString(MetadataKeyID, deterministicKeyID)
			iv := mustHex(t, fixture.iv)
			metadata.SetBytes(MetadataIV, iv)

			var aad []byte
			if fixture.aad != nil {
				encodedAAD, err := EncodeAAD(fixture.aad)
				if err != nil {
					t.Fatalf("EncodeAAD(%#v) returned %v", fixture.aad, err)
				}
				aad = encodedAAD.Bytes
				metadata.SetBytes(MetadataAAD, aad)
				metadata.SetInt(MetadataAADEncodingSchemeMajor, int64(encodedAAD.Baseline.Major))
				metadata.SetInt(MetadataAADEncodingSchemeMinor, int64(encodedAAD.Baseline.Minor))
			}

			cipherOutput, err := key.Seal(iv, encoded.Bytes, aad)
			if err != nil {
				t.Fatalf("Seal returned %v", err)
			}

			got, err := EncodeEncrypted(Encrypted{
				ProfileType:    ProfileTypeEnvelope,
				ProfileVersion: EnvelopeProfileVersion,
				ProfileName:    deterministicName,
				CipherOutput:   cipherOutput,
				TypeName:       encoded.TypeName,
				Baseline:       encoded.Baseline,
				Metadata:       metadata,
			})
			if err != nil {
				t.Fatalf("EncodeEncrypted returned %v", err)
			}
			if hex.EncodeToString(got) != fixture.encrypted {
				t.Errorf("encrypted %#v to\n%x\nwant\n%s", fixture.value, got, fixture.encrypted)
			}
		})
	}
}

// TestDecryptsKnownBytes is the reverse of TestEncryptsToKnownBytes.
func TestDecryptsKnownBytes(t *testing.T) {
	t.Parallel()

	key, err := NewDataKey(unwrapFixtureKey(t))
	if err != nil {
		t.Fatalf("NewDataKey returned %v", err)
	}

	for _, fixture := range deterministicFixtures {
		t.Run(fixture.encrypted[:24], func(t *testing.T) {
			t.Parallel()

			encrypted, err := DecodeEncrypted(mustHex(t, fixture.encrypted))
			if err != nil {
				t.Fatalf("DecodeEncrypted returned %v", err)
			}
			iv, _ := encrypted.Metadata.Bytes(MetadataIV)
			aad, _ := encrypted.Metadata.Bytes(MetadataAAD)

			plaintext, err := key.Open(iv, encrypted.CipherOutput, aad)
			if err != nil {
				t.Fatalf("Open returned %v", err)
			}
			decoded, err := DecodeValue(plaintext, encrypted.TypeName, encrypted.Baseline)
			if err != nil {
				t.Fatalf("DecodeValue returned %v", err)
			}
			if !sameValue(decoded, fixture.value) {
				t.Errorf("decrypted to %#v, want %#v", decoded, fixture.value)
			}
		})
	}
}

// TestOpenRejects covers every way authentication can fail.
func TestOpenRejects(t *testing.T) {
	t.Parallel()

	key, err := NewDataKey(unwrapFixtureKey(t))
	if err != nil {
		t.Fatalf("NewDataKey returned %v", err)
	}
	iv := mustHex(t, "000102030405060708090a0b")
	cipherOutput, err := key.Seal(iv, []byte{0xc3}, []byte("context"))
	if err != nil {
		t.Fatalf("Seal returned %v", err)
	}

	t.Run("wrong aad", func(t *testing.T) {
		t.Parallel()
		_, err := key.Open(iv, cipherOutput, []byte("other"))
		if !errors.Is(err, ErrAuthentication) {
			t.Fatalf("Open returned %v, want ErrAuthentication", err)
		}
	})
	t.Run("missing aad", func(t *testing.T) {
		t.Parallel()
		_, err := key.Open(iv, cipherOutput, nil)
		if !errors.Is(err, ErrAuthentication) {
			t.Fatalf("Open returned %v, want ErrAuthentication", err)
		}
	})
	t.Run("wrong iv", func(t *testing.T) {
		t.Parallel()
		other := mustHex(t, "0c0d0e0f1011121314151617")
		_, err := key.Open(other, cipherOutput, []byte("context"))
		if !errors.Is(err, ErrAuthentication) {
			t.Fatalf("Open returned %v, want ErrAuthentication", err)
		}
	})
	t.Run("tampered ciphertext", func(t *testing.T) {
		t.Parallel()
		tampered := append([]byte(nil), cipherOutput...)
		tampered[0] ^= 0xff
		_, err := key.Open(iv, tampered, []byte("context"))
		if !errors.Is(err, ErrAuthentication) {
			t.Fatalf("Open returned %v, want ErrAuthentication", err)
		}
	})
	t.Run("tampered tag", func(t *testing.T) {
		t.Parallel()
		tampered := append([]byte(nil), cipherOutput...)
		tampered[len(tampered)-1] ^= 0xff
		_, err := key.Open(iv, tampered, []byte("context"))
		if !errors.Is(err, ErrAuthentication) {
			t.Fatalf("Open returned %v, want ErrAuthentication", err)
		}
	})
	t.Run("wrong key", func(t *testing.T) {
		t.Parallel()
		other, err := NewDataKey([]byte("a different data encryption key!"))
		if err != nil {
			t.Fatalf("NewDataKey returned %v", err)
		}
		_, err = other.Open(iv, cipherOutput, []byte("context"))
		if !errors.Is(err, ErrAuthentication) {
			t.Fatalf("Open returned %v, want ErrAuthentication", err)
		}
	})
	t.Run("cipher output shorter than the tag", func(t *testing.T) {
		t.Parallel()
		if _, err := key.Open(iv, cipherOutput[:TagSize-1], nil); err == nil {
			t.Fatal("Open accepted a cipher output with no room for a tag")
		}
	})
}

func TestSealRejectsWrongIVSize(t *testing.T) {
	t.Parallel()

	key, err := NewDataKey(unwrapFixtureKey(t))
	if err != nil {
		t.Fatalf("NewDataKey returned %v", err)
	}
	for _, size := range []int{0, 11, 13, 16} {
		if _, err := key.Seal(make([]byte, size), []byte{1}, nil); err == nil {
			t.Errorf("Seal accepted a %d byte iv", size)
		}
	}
}

// TestNewDataKeyRejectsKeysThatAreNotAes256 covers every size but 32 bytes.
func TestNewDataKeyRejectsKeysThatAreNotAes256(t *testing.T) {
	t.Parallel()

	for _, size := range []int{0, 1, 16, 24, 31, 33, 64} {
		var dek []byte
		if size > 0 {
			dek = make([]byte, size)
		}
		if _, err := NewDataKey(dek); err == nil {
			t.Errorf("NewDataKey accepted a %d byte data encryption key", size)
		}
	}

	if _, err := NewDataKey(make([]byte, KeySize)); err != nil {
		t.Errorf("NewDataKey rejected a %d byte key: %v", KeySize, err)
	}
}

// TestNewIVIsRandom checks the initialisation vector source is not stuck, since reuse with
// the same key breaks AES-GCM.
func TestNewIVIsRandom(t *testing.T) {
	t.Parallel()

	seen := make(map[string]struct{}, 1000)
	for i := 0; i < 1000; i++ {
		iv, err := NewIV()
		if err != nil {
			t.Fatalf("NewIV returned %v", err)
		}
		if len(iv) != IVSize {
			t.Fatalf("iv is %d bytes, want %d", len(iv), IVSize)
		}
		key := string(iv)
		if _, repeated := seen[key]; repeated {
			t.Fatalf("NewIV repeated %x", iv)
		}
		seen[key] = struct{}{}
	}
}

func unwrapFixtureKey(t *testing.T) []byte {
	t.Helper()

	dek, err := UnwrapKey(
		mustHex(t, deterministicKEK),
		mustHex(t, deterministicEncapsulation),
		mustBase64(t, deterministicKeyIV),
	)
	if err != nil {
		t.Fatalf("UnwrapKey returned %v", err)
	}
	return dek
}

func mustBase64(t *testing.T, s string) []byte {
	t.Helper()

	decoded, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		t.Fatalf("bad test base64 %q: %v", s, err)
	}
	return decoded
}
