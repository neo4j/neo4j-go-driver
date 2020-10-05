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

package bolt

import (
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/internal/packstream"
)

// Message struct tags
// Shared between bolt versions
const (
	msgReset      packstream.StructTag = 0x0f
	msgRun        packstream.StructTag = 0x10
	msgDiscardAll packstream.StructTag = 0x2f
	msgDiscardN                        = msgDiscardAll // Different name >= 4.0
	msgPullAll    packstream.StructTag = 0x3f
	msgPullN                           = msgPullAll // Different name >= 4.0
	msgRecord     packstream.StructTag = 0x71
	msgSuccess    packstream.StructTag = 0x70
	msgIgnored    packstream.StructTag = 0x7e
	msgFailure    packstream.StructTag = 0x7f
	msgHello      packstream.StructTag = 0x01
	msgGoodbye    packstream.StructTag = 0x02
	msgBegin      packstream.StructTag = 0x11
	msgCommit     packstream.StructTag = 0x12
	msgRollback   packstream.StructTag = 0x13
)
