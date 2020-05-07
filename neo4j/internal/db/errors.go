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

package db

import (
	"fmt"
	"strings"
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

func (e *DatabaseError) IsAuthentication() bool {
	return e.Code == "Neo.ClientError.Security.Unauthorized"
}

func (e *DatabaseError) IsTransient() bool {
	return e.getCls() == dbErrClsTransient
}

func (e *DatabaseError) IsRetriableTransient() bool {
	if e.getCls() != dbErrClsTransient {
		return false
	}
	switch e.Code {
	// Happens when client aborts transaction, should not retry
	case "Neo.TransientError.Transaction.Terminated", "Neo.TransientError.Transaction.LockClientStopped":
		return false
	}
	return true
}

func (e *DatabaseError) IsRetriableCluster() bool {
	switch e.Code {
	case "Neo.ClientError.Cluster.NotALeader", "Neo.ClientError.General.ForbiddenOnReadOnlyDatabase":
		return true
	}
	return false
}

func (e *DatabaseError) IsClient() bool {
	return e.getCls() == dbErrClsClient
}

type RoutingNotSupportedError struct {
	Server string
}

func (e *RoutingNotSupportedError) Error() string {
	return fmt.Sprintf("%s does not support routing", e.Server)
}
