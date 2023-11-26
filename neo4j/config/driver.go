/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package config

import (
	"crypto/tls"
	"crypto/x509"
	"time"

	bm "github.com/neo4j/neo4j-go-driver/v5/neo4j/bookmarks"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/auth"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/log"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/notifications"
)

// AccessMode defines modes that routing driver decides to which cluster member
// a connection should be opened.
type AccessMode int
type AuthToken = auth.Token

const (
	// AccessModeWrite tells the driver to use a connection to 'Leader'
	AccessModeWrite AccessMode = 0
	// AccessModeRead tells the driver to use a connection to one of the 'Follower' or 'Read Replica'.
	AccessModeRead AccessMode = 1
)

// A Config contains options that can be used to customize certain
// aspects of the driver
type Config struct {
	// RootCAs defines the set of certificate authorities that the driver trusts. If set
	// to nil, the driver uses hosts system certificates.
	//
	// The trusted certificates are used to validate connections for URI schemes 'bolt+s'
	// and 'neo4j+s'.
	//
	// Deprecated: RootCAs will be removed in 6.0.
	// Please rely on TlsConfig's RootCAs attribute instead.
	// RootCAs is ignored if TlsConfig is set.
	RootCAs *x509.CertPool
	// TlsConfig defines the TLS configuration of the driver.
	//
	// The configuration is only used for URI schemes 'bolt+s', 'bolt+ssc',
	// 'neo4j+s' and 'neo4j+ssc'.
	//
	// The default MinVersion attribute is tls.VersionTLS12. This is overridable.
	// The InsecureSkipVerify attribute of TlsConfig is always derived from the initial URI scheme.
	// The ServerName attribute of TlsConfig is always derived from the initial URI host.
	//
	// This is considered an advanced setting, use it at your own risk.
	// Introduced in 5.0.
	TlsConfig *tls.Config

	// Logging target the driver will send its log outputs
	//
	// Possible to use custom logger (implement log.Logger interface) or
	// use neo4j.ConsoleLogger.
	//
	// default: No Op Logger (log.Void)
	Log log.Logger
	// Resolver that would be used to resolve initial router address. This may
	// be useful if you want to provide more than one URL for initial router.
	// If not specified, the URL provided to NewDriver or NewDriverWithContext
	// is used as the initial router.
	//
	// default: nil
	AddressResolver ServerAddressResolver
	// Maximum amount of time a retryable operation would continue retrying. It
	// cannot be specified as a negative value.
	//
	// default: 30 * time.Second
	MaxTransactionRetryTime time.Duration
	// Maximum number of connections per URL to allow on this driver. It
	// cannot be specified as 0 and negative values are interpreted as
	// math.MaxInt32.
	//
	// default: 100
	MaxConnectionPoolSize int
	// Maximum connection lifetime on pooled connections. Values less than
	// or equal to 0 disables the lifetime check.
	//
	// default: 1 * time.Hour
	MaxConnectionLifetime time.Duration
	// Maximum amount of time to either acquire an idle connection from the pool
	// or create a new connection (when the pool is not full). Negative values
	// result in an infinite wait time, whereas a 0 value results in no timeout.
	// If no timeout is set, an immediate failure follows when there are no
	// available connections.
	//
	// Since 4.3, this setting competes with connection read timeout hints, if
	// the server-side option called
	// "dbms.connector.bolt.connection_keep_alive_for_requests" is enabled.
	// The read timeout is automatically applied and may result in connections
	// being dropped if they are idle beyond the corresponding period.
	//
	// Since 5.0, this setting competes with the context-aware APIs. These APIs
	// are discoverable through NewDriverWithContext.
	// When a connection needs to be acquired from the internal driver
	// connection pool and the user-provided context.Context carries a deadline
	// (through context.WithTimeout or context.WithDeadline), the earliest
	// deadline wins. Connections are still subject to early terminations if a read timeout
	// hint is received.
	//
	// default: 1 * time.Minute
	ConnectionAcquisitionTimeout time.Duration
	// Connect timeout that will be set on underlying sockets. Values less than
	// or equal to 0 results in no timeout being applied.
	//
	// Since 5.0, this setting competes with the context-aware APIs. These APIs
	// are discoverable through NewDriverWithContext.
	// If a connection needs to be created when one of these APIs is called
	// and the user-provided context.Context carries a deadline (through
	// context.WithTimeout or context.WithDeadline), the TCP dialer will pick
	// the earliest between this setting and the context deadline.
	//
	// default: 5 * time.Second
	SocketConnectTimeout time.Duration
	// Whether to enable TCP keep alive on underlying sockets.
	//
	// default: true
	SocketKeepalive bool
	// Optionally override the user agent string sent to Neo4j server.
	//
	// default: neo4j.UserAgent
	UserAgent string
	// FetchSize defines how many records to pull from server in each batch.
	// From Bolt protocol v4 (Neo4j 4+) records can be fetched in batches as
	// compared to fetching all in previous versions.
	//
	// If FetchSize is set to FetchDefault, the driver decides the appropriate
	// size. If set to a positive value that size is used if the underlying
	// protocol supports it otherwise it is ignored.
	//
	// To turn off fetching in batches and always fetch everything, set
	// FetchSize to FetchAll.
	// If a single large result is to be retrieved, this is the most performant
	// setting.
	FetchSize int
	// NotificationsMinSeverity defines the minimum severity level of notifications the server should send.
	// By default, the server's settings are used.
	// Disabling severities allows the server to skip analysis for those, which can speed up query execution.
	NotificationsMinSeverity notifications.NotificationMinimumSeverityLevel
	// NotificationsDisabledCategories defines the categories of notifications the server should not send.
	// By default, the server's settings are used.
	// Disabling categories allows the server to skip analysis for those, which can speed up query execution.
	NotificationsDisabledCategories notifications.NotificationDisabledCategories
	// By default, if the server requests it, the driver will automatically transmit anonymous usage
	// statistics to the server it is connected to.
	//
	// By configuring TelemetryDisabled=True, the driver will refrain from transmitting any telemetry data.
	//
	// Each time one of the specified APIs is utilized to execute a query for the first time, the driver
	// informs the server of this action without providing additional details such as arguments or client identifiers:
	//
	//   DriverWithContext.ExecuteQuery
	//   SessionWithContext.Run
	//   SessionWithContext.BeginTransaction
	//   SessionWithContext.ExecuteRead
	//   SessionWithContext.ExecuteWrite
	//
	// default: true
	TelemetryDisabled bool
}

// ServerAddressResolver is a function type that defines the resolver function used by the routing driver to
// resolve the initial address used to create the driver.
type ServerAddressResolver func(address ServerAddress) []ServerAddress

// ServerAddress represents a host and port. Host can either be an IP address or a DNS name.
// Both IPv4 and IPv6 hosts are supported.
type ServerAddress interface {
	// Hostname returns the host portion of this ServerAddress.
	Hostname() string
	// Port returns the port portion of this ServerAddress.
	Port() string
}

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
