package neo4j_go_driver

type AuthToken struct {
	tokens map[string]interface{}
}

const keyScheme string = "scheme"
const schemeBasic string = "basic"
const keyPrincipal string = "principal"
const keyCredentials string = "credentials"
const keyRealm string = "realm"

// Generates an empty authentication token
func NoAuth() AuthToken {
	return AuthToken{}
}

// Generates a basic authentication token with provided username and password
func BasicAuth(username string, password string) AuthToken {
	token := AuthToken{
		tokens: map[string]interface{}{
			keyScheme:      schemeBasic,
			keyPrincipal:   username,
			keyCredentials: password,
		},
	}

	return token
}

func BasicAuthWithRealm(username string, password string, realm string) AuthToken {
	token := AuthToken{
		tokens: map[string]interface{}{
			keyScheme:      schemeBasic,
			keyPrincipal:   username,
			keyCredentials: password,
			keyRealm:       realm,
		},
	}

	return token
}
