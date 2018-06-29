package neo4j_go_driver

import (
    "testing"
    connector "neo4j-go-connector/pkg"
)

func TestDirect(t *testing.T) {
    testCases := []struct {
        name     string
        script   string
        testFunc func(t *testing.T)
    }{
        {name: "should return error when server disconnects after RUN",
            script: "disconnect_on_run.script",
            testFunc: consumeShouldFailOnServerDisconnects},
        {name: "should return error when server disconnects after PULL_ALL",
            script: "disconnect_on_pull_all.script",
            testFunc: consumeShouldFailOnServerDisconnects},
        {name: "should execute simple query",
            script: "return_1.script",
            testFunc: shouldExecuteReturn1},
    }

    for _, testCase := range testCases {
        t.Run(testCase.name, func(t *testing.T) {
            stub := startStubServer(t, 9001, testCase.script)

            testCase.testFunc(t)

            assertTrue(t, stub.waitForExit())
        })
    }
}

func consumeShouldFailOnServerDisconnects(t *testing.T) {
    driver := createDirectDriver(t)
    defer driver.Close()

    session := createSession(t, driver)
    defer session.Close()

    result, err := session.RunWithParams("RETURN $x", &map[string]interface{}{"x": 1})
    if err != nil {
        t.Error(err)
    }

    summary, err := result.Consume()
    assertNil(t, summary)
    assertNonNil(t, err)
    assertTrue(t, connector.IsServiceUnavailable(err))
}

func shouldExecuteReturn1(t *testing.T) {
    driver := createDirectDriver(t)
    defer driver.Close()

    session := createSession(t, driver)
    defer session.Close()

    result, err := session.RunWithParams("RETURN $x", &map[string]interface{}{"x": 1})
    if err != nil {
        t.Error(err)
    }

    count := int64(0)
    for result.Next() {
        if x, ok := result.Record().Get("x"); ok {
            count += x.(int64)
        }
    }

    if err := result.Err(); err != nil {
        t.Error(err)
    }

    if count!=1 {
        t.Errorf("expected count to be 1, but found %d", count)
    }
}

func createDirectDriver(t *testing.T) Driver {
    driver, err := NewDriver("bolt://localhost:9001", NoAuth(), &Config{Encrypted: false})
    if err != nil {
        t.Fatal(err)
    }

    return driver
}

func createSession(t *testing.T, driver Driver) *Session {
    session, err := driver.Session()
    if err != nil {
        t.Fatal(err)
    }

    return session
}
