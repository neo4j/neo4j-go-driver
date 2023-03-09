package bolt

import (
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
)

type responseHandler struct {
	onSuccess func(*success)
	onRecord  func(*db.Record)
	onFailure func(*db.Neo4jError)
	onUnknown func(any)
	onIgnored func(*ignored)
}

func onSuccessNoOp(*success) {}
func onIgnoredNoOp(*ignored) {}
