package neo4j_go_driver

import (
	"testing"
)

func TestNoAuth(t *testing.T) {
	t.Run("token should contain only scheme=none", func(t *testing.T) {

        token := NoAuth()

        tokenMap := token.tokens

        assertNonNil(t, tokenMap)
        assertLen(t, &tokenMap, 1)
        assertMapContainsKeyValue(t, &tokenMap, keyScheme, schemeNone)
    })
}

func TestBasicAuth(t *testing.T) {
    t.Run("should not include realm if not provided", func(t *testing.T) {
        token := BasicAuth("test", "1234", "")

        tokenMap := token.tokens

        assertNonNil(t, tokenMap)
        assertLen(t, &tokenMap, 3)
        assertMapContainsKeyValue(t, &tokenMap, keyScheme, schemeBasic)
        assertMapContainsKeyValue(t, &tokenMap, keyPrincipal, "test")
        assertMapContainsKeyValue(t, &tokenMap, keyCredentials, "1234")
    })

    t.Run("should include realm", func(t *testing.T) {
        token := BasicAuth("test", "1234", "some_realm")

        tokenMap := token.tokens

        assertNonNil(t, tokenMap)
        assertLen(t, &tokenMap, 4)
        assertMapContainsKeyValue(t, &tokenMap, keyScheme, schemeBasic)
        assertMapContainsKeyValue(t, &tokenMap, keyPrincipal, "test")
        assertMapContainsKeyValue(t, &tokenMap, keyCredentials, "1234")
        assertMapContainsKeyValue(t, &tokenMap, keyRealm, "some_realm")
    })
}

func TestKerberosAuth(t *testing.T) {
    t.Run("should include provided ticket", func(t *testing.T) {
        token := KerberosAuth("ticket_data")

        tokenMap := token.tokens

        assertNonNil(t, tokenMap)
        assertLen(t, &tokenMap, 2)
        assertMapContainsKeyValue(t, &tokenMap, keyScheme, schemeKerberos)
        assertMapContainsKeyValue(t, &tokenMap, keyTicket, "ticket_data")
    })
}