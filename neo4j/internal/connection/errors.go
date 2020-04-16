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
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package connection

import (
	"fmt"
	"strings"
)

// Server reported authentication error.
type AuthenticationError struct {
	Msg string
}

func (e *AuthenticationError) Error() string {
	return fmt.Sprintf("Authentication error: %s", e.Msg)
}

const (
	DB_TRANS_ERR_CLASS = "TransientError"
	DB_CLIENT_ERR_CLAS = "ClientError"
)

type dbErrCls int

const (
	dbErrClsSentinel dbErrCls = iota
	dbErrClsClient
	dbErrClsTransient
	dbErrClsUnknown
)

// Database server failed to fullfill request.
type DatabaseError struct {
	Code string
	Msg  string
	cls  dbErrCls
}

func (e *DatabaseError) Error() string {
	return fmt.Sprintf("Server error: [%s] %s", e.Code, e.Msg)
}

func (e *DatabaseError) getCls() dbErrCls {
	// Code is on format: Neo.{classification}.X.Y like for example:
	// Neo.ClientError.General.ForbiddenOnReadOnlyDatabase
	if e.cls == dbErrClsSentinel {
		parts := strings.Split(e.Code, ".")
		if len(parts) < 2 {
			e.cls = dbErrClsUnknown
			return e.cls
		}
		if parts[0] != "Neo" {
			e.cls = dbErrClsUnknown
			return e.cls
		}
		switch parts[1] {
		case "TransientError":
			e.cls = dbErrClsTransient
			return e.cls
		case "ClientError":
			e.cls = dbErrClsClient
			return e.cls
		default:
			e.cls = dbErrClsUnknown
		}
	}
	return e.cls
}

func (e *DatabaseError) IsTransientError() bool {
	// TODO: More condition for this?
	return e.getCls() == dbErrClsClient
}

func (e *DatabaseError) IsClientError() bool {
	return e.getCls() == dbErrClsClient
}

// More specific client error
func (e *DatabaseError) IsSyntaxError() bool {
	return false
}

func (e *DatabaseError) IsArithmeticError() bool {
	return false
}

/*
424     if (strcmp(code_str, "Neo.ClientError.General.ForbiddenOnReadOnlyDatabase")==0) {
425         BoltRoutingPool_forget_writer(pool, server);
426     }
427     else if (strcmp(code_str, "Neo.ClientError.Cluster.NotALeader")==0) {
428         BoltRoutingPool_forget_writer(pool, server);
429     }
430     else if (strcmp(code_str, "Neo.TransientError.General.DatabaseUnavailable")==0) {
431         BoltRoutingPool_forget_server(pool, server);
432     }
*/
