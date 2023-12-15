/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package bolt

import (
	"encoding/json"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	"strings"
)

type loggableDictionary map[string]any

func copyAndSanitizeDictionary[T any | string](in map[string]T) map[string]T {
	out := make(map[string]T, len(in))
	for k, v := range in {
		if k == "credentials" {
			var redacted any = "<redacted>"
			out[k] = redacted.(T)
		} else {
			out[k] = v
		}
	}
	return out
}

func (d loggableDictionary) String() string {
	return serializeTrace(copyAndSanitizeDictionary(d))
}

type loggableStringDictionary map[string]string

func (sd loggableStringDictionary) String() string {
	return serializeTrace(copyAndSanitizeDictionary(sd))
}

type loggableList []any

func (l loggableList) String() string {
	return serializeTrace(l)
}

type loggableStringList []string

func (s loggableStringList) String() string {
	return serializeTrace(s)
}

type loggableSuccess success
type loggedSuccess struct {
	Server       string              `json:"server,omitempty"`
	ConnectionId string              `json:"connection_id,omitempty"`
	Fields       []string            `json:"fields,omitempty"`
	TFirst       int64               `json:"t_first,omitempty"`
	Bookmark     string              `json:"bookmark,omitempty"`
	TLast        int64               `json:"t_last,omitempty"`
	HasMore      bool                `json:"has_more,omitempty"`
	Db           string              `json:"db,omitempty"`
	Qid          int64               `json:"qid,omitempty"`
	ConfigHints  loggableDictionary  `json:"hints,omitempty"`
	RoutingTable *loggedRoutingTable `json:"routing_table,omitempty"`
}

func (s loggableSuccess) String() string {
	success := loggedSuccess{
		Server:       s.server,
		ConnectionId: s.connectionId,
		Fields:       s.fields,
		Bookmark:     s.bookmark,
		HasMore:      s.hasMore,
		Db:           s.db,
		ConfigHints:  s.configurationHints,
	}
	if s.tfirst > -1 {
		success.TFirst = s.tfirst
	}
	if s.tlast > -1 {
		success.TFirst = s.tlast
	}
	if s.qid > -1 {
		success.Qid = s.qid
	}
	routingTable := s.routingTable
	if routingTable != nil {
		success.RoutingTable = &loggedRoutingTable{
			TimeToLive:   routingTable.TimeToLive,
			DatabaseName: routingTable.DatabaseName,
			Routers:      routingTable.Routers,
			Readers:      routingTable.Readers,
			Writers:      routingTable.Writers,
		}
	}
	return serializeTrace(success)
}

type loggedRoutingTable struct {
	TimeToLive   int      `json:"ttl,omitempty"`
	DatabaseName string   `json:"db,omitempty"`
	Routers      []string `json:"routers,omitempty"`
	Readers      []string `json:"readers,omitempty"`
	Writers      []string `json:"writers,omitempty"`
}

type loggableFailure db.Neo4jError

func (f loggableFailure) String() string {
	return serializeTrace(map[string]any{
		"code":    f.Code,
		"message": f.Msg,
	})
}

func serializeTrace(v any) string {
	builder := strings.Builder{}
	encoder := json.NewEncoder(&builder)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(v)
	return strings.TrimSpace(builder.String())
}
