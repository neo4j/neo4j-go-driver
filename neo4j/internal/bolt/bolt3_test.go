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

// bolt3.connect is tested through Connect, no need to test it herekk

// TODO: test RunAutoCommit
//       happy path
//       syntax error, RUN fails with client error
//       locking error, RUN fails with retryable error
//       transaction open
//       not alive
//       unconsumed result
//       no response
//       unexpected response
// TODO: test IsAlive
//       different error condtions that can make it not be alive
// TODO: test Close
//       unconsumed result
//       already closed
//       goodbye failure
