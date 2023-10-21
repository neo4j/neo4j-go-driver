package bolt

import (
	"context"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
)

type responseHandler struct {
	onSuccess func(*success)
	onRecord  func(*db.Record)
	onFailure func(context.Context, *db.Neo4jError)
	onIgnored func(*ignored)
}

func onSuccessNoOp(*success) {}
func onIgnoredNoOp(*ignored) {}
