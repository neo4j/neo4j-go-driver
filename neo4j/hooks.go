package neo4j

import "net/url"

type PreQueryHook = func(cypher string, params map[string]interface{}, target url.URL, config *TransactionConfig)

type PostQueryHook = func(err error, target url.URL, config *TransactionConfig)
