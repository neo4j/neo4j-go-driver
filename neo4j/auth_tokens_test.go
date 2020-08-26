/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package neo4j

import (
	"testing"
)

func TestNoAuth(t *testing.T) {
	token := NoAuth()

	if len(token.tokens) != 1 {
		t.Errorf("should only contain the key scheme")
	}

	if token.tokens[keyScheme] != schemeNone {
		t.Errorf("the key scheme should be 'none' %v", token.tokens[keyScheme])
	}
}

func TestBasicAuth(t *testing.T) {
	userName := "user"
	password := "password"
	realm := ""

	token := BasicAuth(userName, password, realm)

	if len(token.tokens) != 3 {
		t.Errorf("should contain 3 keys when no realm data was passed")
	}

	if token.tokens[keyScheme] != schemeBasic {
		t.Errorf("the key scheme should be 'basic' %v", token.tokens[keyScheme])
	}

	if token.tokens[keyPrincipal] != userName {
		t.Errorf("the key principal was not properly set %v", token.tokens[keyPrincipal])
	}

	if token.tokens[keyCredentials] != password {
		t.Errorf("the key credentials was not properly set %v", token.tokens[keyCredentials])
	}
}

func TestBasicAuthWithRealm(t *testing.T) {
	userName := "user"
	password := "password"
	realm := "test"

	token := BasicAuth(userName, password, realm)

	if len(token.tokens) != 4 {
		t.Errorf("should contain 4 keys when realm data was passed")
	}

	if token.tokens[keyScheme] != schemeBasic {
		t.Errorf("the key scheme should be 'basic' %v", token.tokens[keyScheme])
	}

	if token.tokens[keyPrincipal] != userName {
		t.Errorf("the key principal was not properly set %v", token.tokens[keyPrincipal])
	}

	if token.tokens[keyCredentials] != password {
		t.Errorf("the key credentials was not properly set %v", token.tokens[keyCredentials])
	}

	if token.tokens[keyRealm] != realm {
		t.Errorf("the key realm was not properly set %v", token.tokens[keyRealm])
	}
}

func TestKerberosAuth(t *testing.T) {
	ticket := "123456789"

	token := KerberosAuth(ticket)

	if len(token.tokens) != 2 {
		t.Errorf("should contain 2 keys")
	}

	if token.tokens[keyScheme] != schemeKerberos {
		t.Errorf("the key scheme should be 'kerberos' %v", token.tokens[keyScheme])
	}

	if token.tokens[keyTicket] != ticket {
		t.Errorf("the key ticket was not properly set %v", token.tokens[keyTicket])
	}
}

func TestCustomAuthWithNilParamters(t *testing.T) {
	scheme := "custom_scheme"
	userName := "user"
	password := "password"
	realm := "test"
	//parameters := map[string]interface{}{}

	token := CustomAuth(scheme, userName, password, realm, nil)

	if len(token.tokens) != 4 {
		t.Errorf("should contain 4 keys no parameters data was passed %v", len(token.tokens))
	}

	if token.tokens[keyScheme] != scheme {
		t.Errorf("the key scheme was not properly set %v", token.tokens[keyScheme])
	}

	if token.tokens[keyPrincipal] != userName {
		t.Errorf("the key principal was not properly set %v", token.tokens[keyPrincipal])
	}

	if token.tokens[keyCredentials] != password {
		t.Errorf("the key credentials was not properly set %v", token.tokens[keyCredentials])
	}

	if token.tokens[keyRealm] != realm {
		t.Errorf("the key realm was not properly set %v", token.tokens[keyRealm])
	}
}

func TestCustomAuthWithEmptyParameters(t *testing.T) {
	scheme := "custom_scheme"
	userName := "user"
	password := "password"
	realm := "test"
	parameters := map[string]interface{}{}

	token := CustomAuth(scheme, userName, password, realm, parameters)

	if len(token.tokens) != 5 {
		t.Errorf("should contain 5 keys when parameters data was passed %v", len(token.tokens))
	}

	if token.tokens[keyScheme] != scheme {
		t.Errorf("the key scheme was not properly set %v", token.tokens[keyScheme])
	}

	if token.tokens[keyPrincipal] != userName {
		t.Errorf("the key principal was not properly set %v", token.tokens[keyPrincipal])
	}

	if token.tokens[keyCredentials] != password {
		t.Errorf("the key credentials was not properly set %v", token.tokens[keyCredentials])
	}

	if token.tokens[keyRealm] != realm {
		t.Errorf("the key realm was not properly set %v", token.tokens[keyRealm])
	}
}

func TestCustomAuthWithParameters(t *testing.T) {
	scheme := "custom_scheme"
	userName := "user"
	password := "password"
	realm := "test"
	parameters := map[string]interface{}{
		"user_id":     "1234",
		"user_emails": []string{"a@b.com", "b@c.com"},
	}

	token := CustomAuth(scheme, userName, password, realm, parameters)

	if len(token.tokens) != 5 {
		t.Errorf("should contain 5 keys when parameters data was passed %v", len(token.tokens))
	}

	if token.tokens[keyScheme] != scheme {
		t.Errorf("the key scheme was not properly set %v", token.tokens[keyScheme])
	}

	if token.tokens[keyPrincipal] != userName {
		t.Errorf("the key principal was not properly set %v", token.tokens[keyPrincipal])
	}

	if token.tokens[keyCredentials] != password {
		t.Errorf("the key credentials was not properly set %v", token.tokens[keyCredentials])
	}

	if token.tokens[keyRealm] != realm {
		t.Errorf("the key realm was not properly set %v", token.tokens[keyRealm])
	}

	if token.tokens["parameters"] == nil {
		t.Errorf("the key parameters was not properly set %v", token.tokens["parameters"])
	}
}
