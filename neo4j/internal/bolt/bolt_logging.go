package bolt

import (
	"encoding/json"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	"strconv"
	"strings"
)

type loggableDictionary map[string]interface{}

func (d loggableDictionary) String() string {
	if credentials, ok := d["credentials"]; ok {
		d["credentials"] = "<redacted>"
		defer func() {
			d["credentials"] = credentials
		}()
	}
	return serializeTrace(d)
}

type loggableStringDictionary map[string]string

func (sd loggableStringDictionary) String() string {
	if credentials, ok := sd["credentials"]; ok {
		sd["credentials"] = "<redacted>"
		defer func() {
			sd["credentials"] = credentials
		}()
	}
	return serializeTrace(sd)
}

type loggableList []interface{}

func (l loggableList) String() string {
	return serializeTrace(l)
}

type loggableStringList []string

func (s loggableStringList) String() string {
	return serializeTrace(s)
}

type loggableSuccess success
type loggedSuccess struct {
	Server       string   `json:"server,omitempty"`
	ConnectionId string   `json:"connection_id,omitempty"`
	Fields       []string `json:"fields,omitempty"`
	TFirst       string   `json:"t_first,omitempty"`
	Bookmark     string   `json:"bookmark,omitempty"`
	TLast        string   `json:"t_last,omitempty"`
	HasMore      bool     `json:"has_more,omitempy"`
	Db           string   `json:"db,omitempty"`
	Qid          int64    `json:"qid,omitempty"`
}

func (s loggableSuccess) String() string {
	success := loggedSuccess{
		Server:       s.server,
		ConnectionId: s.connectionId,
		Fields:       s.fields,
		TFirst:       formatOmittingZero(s.tfirst),
		Bookmark:     s.bookmark,
		TLast:        formatOmittingZero(s.tlast),
		HasMore:      s.hasMore,
		Db:           s.db,
	}
	if s.qid > -1 {
		success.Qid = s.qid
	}
	return serializeTrace(success)

}

func formatOmittingZero(i int64) string {
	if i == 0 {
		return ""
	}
	return strconv.FormatInt(i, 10)
}

type loggableFailure db.Neo4jError

func (f loggableFailure) String() string {
	return serializeTrace(map[string]interface{}{
		"code":    f.Code,
		"message": f.Msg,
	})
}

func serializeTrace(v interface{}) string {
	builder := strings.Builder{}
	encoder := json.NewEncoder(&builder)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(v)
	return strings.TrimSpace(builder.String())
}
