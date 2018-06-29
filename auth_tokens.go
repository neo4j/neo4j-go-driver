package neo4j_go_driver

type AuthToken struct {
    tokens map[string]interface{}
}

const keyScheme = "scheme"
const schemeNone = "none"
const schemeBasic = "basic"
const schemeKerberos = "kerberos"
const keyPrincipal = "principal"
const keyCredentials = "credentials"
const keyRealm = "realm"
const keyTicket = "ticket"

// Generates an empty authentication token
func NoAuth() AuthToken {
    return AuthToken{tokens: map[string]interface{}{
        keyScheme: schemeNone,
    }}
}

// Generates a basic authentication token with provided username and password
func BasicAuth(username string, password string, realm string) AuthToken {
    tokens := map[string]interface{}{
        keyScheme:      schemeBasic,
        keyPrincipal:   username,
        keyCredentials: password,
    }

    if realm != "" {
        tokens[keyRealm] = realm
    }

    return AuthToken{tokens: tokens}
}

// Generates a kerberos authentication token with provided base-64 encoded kerberos
// ticket
func KerberosAuth(ticket string) AuthToken {
    token := AuthToken{
        tokens: map[string]interface{}{
            keyScheme: schemeKerberos,
            keyTicket: ticket,
        },
    }

    return token
}

// Generates a custom authentication token with provided parameters
func CustomAuth(scheme string, username string, password string, realm *string, parameters *map[string]interface{}) AuthToken {
    tokens := map[string]interface{}{
        keyScheme:      scheme,
        keyPrincipal:   username,
        keyCredentials: password,
    }

    if realm != nil {
        tokens[keyRealm] = *realm
    }

    if parameters != nil {
        tokens["parameters"] = *parameters
    }

    return AuthToken{tokens: tokens}
}
