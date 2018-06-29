package neo4j_go_driver

import (
	"testing"
	"neo4j-go-driver/connector-mocks"
	"github.com/golang/mock/gomock"
)

func TestRunner(t *testing.T) {
	t.Run("runStatement should invoke run on connection", func(t *testing.T) {
		mockCtrl := gomock.NewController(t)
		defer mockCtrl.Finish()

		mockConnection := connector_mocks.NewMockConnection(mockCtrl)
		mockConnector := connector_mocks.MockedConnector(mockConnection)
		mockDriver := newDirectWithConnector("bolt://localhost", mockConnector)
		runner := newRunner(mockDriver, AccessModeWrite, true)

		gomock.InOrder(
			mockConnection.EXPECT().Run("RETURN 1", nil).Times(1),
			mockConnection.EXPECT().PullAll().Times(1),
			mockConnection.EXPECT().Flush().Times(1),
		)

		_, err := runner.runStatement(Statement{cypher: "RETURN 1"})

		assertNil(t, err)
	})
}
