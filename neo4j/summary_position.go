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

// InputPosition contains information about a specific position in a statement
type InputPosition interface {
	// Offset returns the character offset referred to by this position; offset numbers start at 0.
	Offset() int
	// Line returns the line number referred to by this position; line numbers start at 1.
	Line() int
	// Column returns the column number referred to by this position; column numbers start at 1.
	Column() int
}

type neoInputPosition struct {
	offset int
	line   int
	column int
}

func (pos *neoInputPosition) Offset() int {
	return pos.offset
}

func (pos *neoInputPosition) Line() int {
	return pos.line
}

func (pos *neoInputPosition) Column() int {
	return pos.column
}
