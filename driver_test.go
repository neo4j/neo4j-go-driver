package neo4j_go_driver

import (
	"testing"
)

func assertNil(t *testing.T, err error) {
	if err != nil {
		println(err.Error())
		t.Error("An error occurred")
	}
}

func TestDriver(t *testing.T) {
	uri := "bolt://localhost:7687"
	driver, err := NewDriver(uri, NoAuth(), nil)
	assertNil(t, err)

	if driver.Target().Scheme != "bolt" {
		t.Errorf("driver.uri.scheme = %q", driver.Target().Scheme)
	}

	if driver.Target().Host != "localhost:7687" {
		t.Errorf("driver.uri.host = %q", driver.Target().Host)
	}
}
