package connector_mocks

import "neo4j-go-connector/pkg"

type mockConnector struct {
	pool *mockPool
}

type mockPool struct {
	connection *MockConnection
}

func (connector *mockConnector) GetPool() (neo4j.Pool, error) {
	return connector.pool, nil
}

func (connector *mockConnector) Close() error {
	return connector.pool.Close()
}

func (pool *mockPool) Acquire() (neo4j.Connection, error) {
	return pool.connection, nil
}

func (pool *mockPool) Close() error {
	return pool.connection.Close()
}

func MockedConnector(connection *MockConnection) neo4j.Connector {
	return &mockConnector{
		pool: &mockPool{
			connection: connection,
		},
	}
}

