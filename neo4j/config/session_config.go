package config

import (
	bm "github.com/neo4j/neo4j-go-driver/v5/neo4j/bookmarks"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/auth"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/log"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/notifications"
)

// AccessMode defines modes that routing driver decides to which cluster member
// a connection should be opened.
type AuthToken = auth.Token
type AccessMode int

const (
	// AccessModeWrite tells the driver to use a connection to 'Leader'
	AccessModeWrite AccessMode = 0
	// AccessModeRead tells the driver to use a connection to one of the 'Follower' or 'Read Replica'.
	AccessModeRead AccessMode = 1
)

// SessionConfig is used to configure a new session, its zero value uses safe defaults.
type SessionConfig struct {
	// AccessMode used when using Session.Run and explicit transactions. Used to route query
	// to read or write servers when running in a cluster. Session.ReadTransaction and Session.WriteTransaction
	// does not rely on this mode.
	AccessMode AccessMode
	// Bookmarks are the initial bookmarks used to ensure that the executing server is at least up
	// to date to the point represented by the latest of the provided bookmarks. After running commands
	// on the session the bookmark can be retrieved with Session.LastBookmark. All commands executing
	// within the same session will automatically use the bookmark from the previous command in the
	// session.
	Bookmarks bm.Bookmarks
	// DatabaseName sets the target database name for the queries executed within the session created with this
	// configuration.
	// Usage of Cypher clauses like USE is not a replacement for this option.
	// Drive​r sends Cypher to the server for processing.
	// This option has no explicit value by default, but it is recommended to set one if the target database is known
	// in advance. This has the benefit of ensuring a consistent target database name throughout the session in a
	// straightforward way and potentially simplifies driver logic as well as reduces network communication resulting
	// in better performance.
	// When no explicit name is set, the driver behavior depends on the connection URI scheme supplied to the driver on
	// instantiation and Bolt protocol version.
	//
	// Specifically, the following applies:
	//
	// - for bolt schemes
	//		queries are dispatched to the server for execution without explicit database name supplied,
	// 		meaning that the target database name for query execution is determined by the server.
	//		It is important to note that the target database may change (even within the same session), for instance if the
	//		user's home database is changed on the server.
	//
	// - for neo4j schemes
	//		providing that Bolt protocol version 4.4, which was introduced with Neo4j server 4.4, or above
	//		is available, the driver fetches the user's home database name from the server on first query execution
	//		within the session and uses the fetched database name explicitly for all queries executed within the session.
	//		This ensures that the database name remains consistent within the given session. For instance, if the user's
	//		home database name is 'movies' and the server supplies it to the driver upon database name fetching for the
	//		session, all queries within that session are executed with the explicit database name 'movies' supplied.
	//		Any change to the user’s home database is reflected only in sessions created after such change takes effect.
	//		This behavior requires additional network communication.
	//		In clustered environments, it is strongly recommended to avoid a single point of failure.
	//		For instance, by ensuring that the connection URI resolves to multiple endpoints.
	//		For older Bolt protocol versions, the behavior is the same as described for the bolt schemes above.
	DatabaseName string
	// FetchSize defines how many records to pull from server in each batch.
	// From Bolt protocol v4 (Neo4j 4+) records can be fetched in batches as compared to fetching
	// all in previous versions.
	//
	// If FetchSize is set to FetchDefault, the driver decides the appropriate size. If set to a positive value
	// that size is used if the underlying protocol supports it otherwise it is ignored.
	//
	// To turn off fetching in batches and always fetch everything, set FetchSize to FetchAll.
	// If a single large result is to be retrieved this is the most performant setting.
	FetchSize int
	// Logging target the session will send its Bolt message traces
	//
	// Possible to use custom logger (implement log.BoltLogger interface) or
	// use neo4j.ConsoleBoltLogger.
	BoltLogger log.BoltLogger
	// ImpersonatedUser sets the Neo4j user that the session will be acting as.
	// If not set, the user configured for the driver will be used.
	//
	// If user impersonation is used, the default database for that impersonated
	// user will be used unless DatabaseName is set.
	//
	// In the former case, when routing is enabled, using impersonation
	// without DatabaseName will cause the driver to query the
	// cluster for the name of the default database of the impersonated user.
	// This is done at the beginning of the session so that queries are routed
	// to the correct cluster member (different databases may have different
	// leaders).
	ImpersonatedUser string
	// BookmarkManager defines a central point to externally supply bookmarks
	// and be notified of bookmark updates per database
	// Since 5.0
	// default: nil (no-op)
	BookmarkManager bm.BookmarkManager
	// NotificationsMinSeverity defines the minimum severity level of notifications the server should send.
	// By default, the driver's settings are used.
	// Else, this option overrides the driver's settings.
	// Disabling severities allows the server to skip analysis for those, which can speed up query execution.
	NotificationsMinSeverity notifications.NotificationMinimumSeverityLevel
	// NotificationsDisabledCategories defines the categories of notifications the server should not send.
	// By default, the driver's settings are used.
	// Else, this option overrides the driver's settings.
	// Disabling categories allows the server to skip analysis for those, which can speed up query execution.
	NotificationsDisabledCategories notifications.NotificationDisabledCategories
	// Auth is used to overwrite the authentication information for the session.
	// This requires the server to support re-authentication on the protocol level.
	// `nil` will make the driver use the authentication information from the driver configuration.
	// The `neo4j` package provides factory functions for common authentication schemes:
	//   - `neo4j.NoAuth`
	//   - `neo4j.BasicAuth`
	//   - `neo4j.KerberosAuth`
	//   - `neo4j.BearerAuth`
	//   - `neo4j.CustomAuth`
	Auth *AuthToken

	ForceReAuth bool
}
