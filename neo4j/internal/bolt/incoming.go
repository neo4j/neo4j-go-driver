/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package bolt

import (
	"context"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/racing"
	"time"
)

type incoming struct {
	buf             []byte // Reused buffer
	hyd             hydrator
	connReadTimeout time.Duration
}

func (i *incoming) next(ctx context.Context, reader *racing.RacingReader) (any, error) {
	// Get next message from transport layer
	var err error
	var msg []byte
	i.buf, msg, err = dechunkMessage(ctx, reader, i.buf, i.connReadTimeout)
	if err != nil {
		return nil, err
	}
	return i.hyd.hydrate(msg)
}
