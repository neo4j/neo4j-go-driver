/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package neo4j

import "crypto/x509"

// TrustStrategy defines how the driver will establish trust with the neo4j instance
type TrustStrategy struct {
	certificates       []*x509.Certificate
	skipVerify         bool
	skipVerifyHostname bool
}

// TrustAny returns a trust strategy which skips trust verification (trusts any certificate
// provided) and whose hostname verification can be enabled/disabled based on the provided
// parameter
func TrustAny(verifyHostname bool) TrustStrategy {
	return TrustStrategy{
		certificates:       nil,
		skipVerify:         true,
		skipVerifyHostname: !verifyHostname,
	}
}

// TrustSystem returns a trust strategy which uses system provided certificates for
// trust verification and whose hostname verification can be enabled/disabled based
// on the provided parameter
func TrustSystem(verifyHostname bool) TrustStrategy {
	return TrustStrategy{
		certificates:       nil,
		skipVerify:         false,
		skipVerifyHostname: !verifyHostname,
	}
}

// TrustOnly returns a trust strategy which uses provided certificates for trust
// verification and whose hostname verification can be enabled/disabled based
// on the provided parameter
func TrustOnly(verifyHostname bool, certs ...*x509.Certificate) TrustStrategy {
	return TrustStrategy{
		certificates:       certs,
		skipVerify:         false,
		skipVerifyHostname: !verifyHostname,
	}
}
