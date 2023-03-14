package bolt

import (
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
	idb "github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/db"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/notifications"
)

func checkNotificationFiltering(
	notificationConfig idb.NotificationConfig,
	bolt idb.Connection,
) error {
	if notificationConfig.MinSev == notifications.DefaultLevel &&
		!notificationConfig.DisCats.DisablesNone() && len(notificationConfig.DisCats.DisabledCategories()) == 0 {
		return nil
	}
	version := bolt.Version()
	if version.Major < 5 || version.Major == 5 && version.Minor < 2 {
		return &db.FeatureNotSupportedError{
			Server:  bolt.ServerName(),
			Feature: "notification filtering",
			Reason:  "requires least server v5.7",
		}
	}
	return nil
}
