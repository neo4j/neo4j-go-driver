package main

import (
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

func cleanDb(driver neo4j.Driver) {
	session := driver.NewSession(neo4j.SessionConfig{})
	batch := 1000
	for {
		x, err := session.WriteTransaction(func(tx neo4j.Transaction) (interface{}, error) {
			result, err := tx.Run("MATCH (n) WITH n LIMIT $batch DETACH DELETE n RETURN count(n)",
				map[string]interface{}{"batch": batch})
			if err != nil {
				return nil, err
			}
			record, err := result.Single()
			if err != nil {
				return nil, err
			}
			return record.Values[0], nil
		})
		if err != nil {
			panic(err)
		}
		count := x.(int64)
		if count < int64(batch) {
			return
		}
	}
}
