package db_test

import (
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	. "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/testutil"
	"testing"
)

func TestRecord_String(outer *testing.T) {
	outer.Parallel()

	type testCase struct {
		description    string
		record         db.Record
		expectedResult string
	}

	tests := []testCase{
		{
			description:    "empty record",
			record:         db.Record{},
			expectedResult: `{}`,
		},
		{
			description: "non-empty record",
			record: db.Record{
				Values: []any{int64(42)},
				Keys:   []string{"42"},
			},
			expectedResult: `{"42": 42}`,
		},
	}

	for _, test := range tests {
		outer.Run(test.description, func(t *testing.T) {
			record := test.record
			AssertDeepEquals(outer, record.String(), test.expectedResult)
		})
	}
}
