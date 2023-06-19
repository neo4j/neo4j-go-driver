package boltagent

import (
	. "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/testutil"
	"testing"
)

func init() {
	os = "darwin"
	arch = "amd64"
	version = "go1.20.3"
}

func TestNew(t *testing.T) {
	actual := New()

	AssertStringEqual(t, actual.Product(), Product)
	AssertStringEqual(t, actual.Platform(), "darwin; amd64")
	AssertStringEqual(t, actual.Language(), "Go/go1.20.3")
}
