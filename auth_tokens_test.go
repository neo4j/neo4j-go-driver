package neo4j_go_driver

import (
	"testing"
	"neo4j-go-driver/test-utils"
)

func TestNoAuthGeneratesEmptyAuthToken(t *testing.T) {
	token := NoAuth()

	tokenMap := token.tokens
	if tokenMap != nil {
		t.Errorf("NoAuth() returned %q, whereas an empty map was expected", tokenMap)
	}
}

func TestBasicAuth(t *testing.T) {
	token := BasicAuth("test", "1234")

	tokenMap := token.tokens
	if tokenMap == nil {
		t.Errorf("BasicAuth() returned an empty map")
	}

	test_utils.AssertLen(t, &tokenMap, 3)
	test_utils.AssertMapKeyValue(t, &tokenMap, keyScheme, "basic")
	test_utils.AssertMapKeyValue(t, &tokenMap, keyPrincipal, "test")
	test_utils.AssertMapKeyValue(t, &tokenMap, keyCredentials, "1234")
}

func TestBasicAuthWithRealm(t *testing.T) {
	token := BasicAuthWithRealm("test", "1234", "some_realm")

	tokenMap := token.tokens
	if tokenMap == nil {
		t.Errorf("BasicAuthWithRealm() returned an empty map")
	}

	test_utils.AssertLen(t, &tokenMap, 4)
	test_utils.AssertMapKeyValue(t, &tokenMap, keyScheme, "basic")
	test_utils.AssertMapKeyValue(t, &tokenMap, keyPrincipal, "test")
	test_utils.AssertMapKeyValue(t, &tokenMap, keyCredentials, "1234")
	test_utils.AssertMapKeyValue(t, &tokenMap, keyRealm, "some_realm")
}