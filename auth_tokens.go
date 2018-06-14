package neo4j_go_driver

type AuthToken struct {
	tokens map[string]interface{}
}

const scheme string = "scheme"
const schemeBasic string = "basic"
const principal string = "principal"
const credentials string = "credentials"

// Generates an empty authentication token
func NoAuth() AuthToken {
	return AuthToken{}
}

// Generates a basic authentication token with provided username and password
func BasicAuth(username string, password string) AuthToken {
	token := AuthToken{
		tokens: map[string]interface{}{
			scheme:      schemeBasic,
			principal:   username,
			credentials: password,
		},
	}

	return token
}
