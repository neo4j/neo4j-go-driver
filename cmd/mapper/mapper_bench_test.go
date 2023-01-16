package main

import (
	"context"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"testing"
)

//go:generate go run github.com/neo4j/neo4j-go-driver/v5/cmd/mapper "APerson"
type APerson struct {
	Labels   []string `neo4j:"mapping_type=labels"`
	Id       int64    `neo4j:"mapping_type=id"`
	BetterId string   `neo4j:"mapping_type=element_id"`
	Name     string   `neo4j:"mapping_type=property,name=name"`
}

func BenchmarkMapSingle(b *testing.B) {
	ctx := context.Background()
	auth := neo4j.BasicAuth("neo4j", "admin", "")
	driver, err := neo4j.NewDriverWithContext("neo4j://localhost", auth)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := driver.Close(ctx); err != nil {
			b.Error(err)
		}
	})
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	b.Cleanup(func() {
		if err := session.Close(ctx); err != nil {
			b.Error(err)
		}
	})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := session.Run(ctx, "MATCH (p:Person) RETURN p LIMIT 1", nil)
		if err != nil {
			b.Error(err)
			continue
		}
		record, err := result.Single(ctx)
		if err != nil {
			b.Error(err)
			continue
		}
		_, err = MapAPersonFromRecord(record)
		if err != nil {
			b.Error(err)
		}
	}
}
