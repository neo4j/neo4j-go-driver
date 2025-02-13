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

package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/config"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/log"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/notifications"
	"io"
	"math"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/auth"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/db"
)

// Handles a testkit backend session.
// Tracks all objects (and errors) that is created by testkit frontend.
type backend struct {
	rd                              *bufio.Reader // Socket to read requests from
	wr                              io.Writer     // Socket to write responses (and logs) on, don't buffer (WriteString on bufio was weird...)
	drivers                         map[string]neo4j.DriverWithContext
	sessionStates                   map[string]*sessionState
	results                         map[string]neo4j.ResultWithContext
	managedTransactions             map[string]neo4j.ManagedTransaction
	explicitTransactions            map[string]neo4j.ExplicitTransaction
	recordedErrors                  map[string]error
	resolvedAddresses               map[string][]any
	authTokenManagers               map[string]auth.TokenManager
	resolvedGetAuthTokens           map[string]neo4j.AuthToken
	resolvedHandleSecurityException map[string]bool
	resolvedBasicTokens             map[string]AuthToken
	resolvedBearerTokens            map[string]AuthTokenAndExpiration
	id                              int // ID to use for next object created by frontend
	wrLock                          sync.Mutex
	suppliedBookmarks               map[string]neo4j.Bookmarks
	consumedBookmarks               map[string]struct{}
	bookmarkManagers                map[string]neo4j.BookmarkManager
	clientCertificateProviders      map[string]auth.ClientCertificateProvider
	resolvedClientCertificates      map[string]auth.ClientCertificate
}

// To implement transactional functions a bit of extra state is needed on the
// driver session.
type sessionState struct {
	session          neo4j.SessionWithContext
	retryableState   int
	retryableErrorId string
}

type GenericTokenManager struct {
	GetAuthTokenFunc            func() neo4j.AuthToken
	HandleSecurityExceptionFunc func(neo4j.AuthToken, *db.Neo4jError) bool
}

type AuthToken struct {
	token neo4j.AuthToken
}

type AuthTokenAndExpiration struct {
	token      neo4j.AuthToken
	expiration *time.Time
}

func (g GenericTokenManager) GetAuthToken(_ context.Context) (neo4j.AuthToken, error) {
	return g.GetAuthTokenFunc(), nil
}

func (g GenericTokenManager) HandleSecurityException(_ context.Context, token neo4j.AuthToken, securityException *db.Neo4jError) (bool, error) {
	handled := g.HandleSecurityExceptionFunc(token, securityException)
	return handled, nil
}

type TestKitClientCertificateProvider struct {
	id      string
	backend *backend
}

func NewTestKitClientCertificateProvider(id string, backend *backend) *TestKitClientCertificateProvider {
	return &TestKitClientCertificateProvider{
		id:      id,
		backend: backend,
	}
}

func (p TestKitClientCertificateProvider) GetCertificate() *tls.Certificate {
	requestId := p.backend.nextId()
	p.backend.writeResponse("ClientCertificateProviderRequest", map[string]any{
		"id":                          requestId,
		"clientCertificateProviderId": p.id,
	})
	for {
		p.backend.process()
		if clientCertificate, ok := p.backend.resolvedClientCertificates[requestId]; ok {
			delete(p.backend.resolvedClientCertificates, requestId)

			provider, err := auth.NewStaticClientCertificateProvider(clientCertificate)
			if err != nil {
				panic(fmt.Sprintf("Unable to create provider for client certificate: %v : %s", clientCertificate, err))
			}
			return provider.GetCertificate()
		}
	}
}

const (
	retryableNothing  = 0
	retryablePositive = 1
	retryableNegative = -1
)

var ctx = context.Background()

func newBackend(rd *bufio.Reader, wr io.Writer) *backend {
	return &backend{
		rd:                              rd,
		wr:                              wr,
		drivers:                         make(map[string]neo4j.DriverWithContext),
		sessionStates:                   make(map[string]*sessionState),
		results:                         make(map[string]neo4j.ResultWithContext),
		managedTransactions:             make(map[string]neo4j.ManagedTransaction),
		explicitTransactions:            make(map[string]neo4j.ExplicitTransaction),
		recordedErrors:                  make(map[string]error),
		resolvedAddresses:               make(map[string][]any),
		authTokenManagers:               make(map[string]auth.TokenManager),
		resolvedGetAuthTokens:           make(map[string]neo4j.AuthToken),
		resolvedHandleSecurityException: make(map[string]bool),
		resolvedBasicTokens:             make(map[string]AuthToken),
		resolvedBearerTokens:            make(map[string]AuthTokenAndExpiration),
		id:                              0,
		bookmarkManagers:                make(map[string]neo4j.BookmarkManager),
		suppliedBookmarks:               make(map[string]neo4j.Bookmarks),
		consumedBookmarks:               make(map[string]struct{}),
		clientCertificateProviders:      make(map[string]auth.ClientCertificateProvider),
		resolvedClientCertificates:      make(map[string]auth.ClientCertificate),
	}
}

type frontendError struct {
	msg string
}

func (e *frontendError) Error() string {
	return e.msg
}

func (b *backend) writeLine(s string) error {
	bs := []byte(s + "\n")
	_, err := b.wr.Write(bs)
	return err
}

func (b *backend) writeLineLocked(s string) error {
	b.wrLock.Lock()
	defer b.wrLock.Unlock()
	fmt.Println(s)
	return b.writeLine(s)
}

// Reads and writes to the socket until it is closed
func (b *backend) serve() {
	for b.process() {
	}
}

func (b *backend) setError(err error) string {
	id := b.nextId()
	b.recordedErrors[id] = err
	return id
}

func (b *backend) writeError(err error) {
	// Convert error if it is a known type of error.
	// This is very simple right now, no extra information is sent at all just keep
	// track of this error so that we can reuse the real thing within a retryable tx
	fmt.Printf("Error: %s (%T)\n", err.Error(), err)
	code := ""
	retriable := neo4j.IsRetryable(err)
	_, isHydrationError := err.(*db.ProtocolError)
	tokenErr, isTokenExpiredErr := err.(*neo4j.TokenExpiredError)
	if isTokenExpiredErr {
		code = tokenErr.Code
	}
	if neo4j.IsNeo4jError(err) {
		code = err.(*db.Neo4jError).Code
	}
	isDriverError := isHydrationError ||
		isTokenExpiredErr ||
		neo4j.IsNeo4jError(err) ||
		neo4j.IsUsageError(err) ||
		neo4j.IsConnectivityError(err) ||
		neo4j.IsTransactionExecutionLimit(err)

	if isDriverError {
		var msg, errorType, gqlStatus, gqlStatusDescription, gqlClassification, gqlRawClassification string
		var gqlDiagnosticRecord map[string]any
		var cause *db.Neo4jError
		if neo4jError, ok := err.(*neo4j.Neo4jError); ok {
			msg = neo4jError.Msg
			gqlStatus = neo4jError.GqlStatus
			gqlStatusDescription = neo4jError.GqlStatusDescription
			gqlClassification = string(neo4jError.GqlClassification)
			gqlRawClassification = neo4jError.GqlRawClassification
			gqlDiagnosticRecord = serializeParameters(neo4jError.GqlDiagnosticRecord)
			errorType = "Neo4jError"
			cause = neo4jError.GqlCause
		} else {
			msg = err.Error()
			errorType = strings.Split(err.Error(), ":")[0]
		}

		id := b.setError(err)
		b.writeResponse("DriverError", map[string]any{
			"id":                id,
			"errorType":         errorType,
			"msg":               msg,
			"code":              code,
			"gqlStatus":         gqlStatus,
			"statusDescription": gqlStatusDescription,
			"classification":    gqlClassification,
			"rawClassification": emptyStringToNil(gqlRawClassification),
			"diagnosticRecord":  gqlDiagnosticRecord,
			"cause":             b.serializeGqlErrorCause(cause),
			"retryable":         retriable,
		})
		return
	}

	// This is an error that originated in frontend
	frontendErr, isFrontendErr := err.(*frontendError)
	if isFrontendErr {
		b.writeResponse("FrontendError", map[string]any{"msg": frontendErr.msg})
		return
	}

	// TODO: Return the other kinds of errors as well...

	// Unknown error, interpret this as a backend error
	// Report this to frontend and close the connection
	// This simplifies debugging errors from the frontend perspective, it will also make sure
	// that the frontend doesn't hang when backend suddenly disappears.
	b.writeResponse("BackendError", map[string]any{"msg": err.Error()})
}

func (b *backend) serializeGqlErrorCause(cause *db.Neo4jError) map[string]any {
	if cause == nil {
		return nil
	}
	return map[string]any{"name": "GqlError", "data": map[string]any{
		"msg":               cause.Msg,
		"gqlStatus":         cause.GqlStatus,
		"statusDescription": cause.GqlStatusDescription,
		"classification":    string(cause.GqlClassification),
		"rawClassification": emptyStringToNil(cause.GqlRawClassification),
		"diagnosticRecord":  serializeParameters(cause.GqlDiagnosticRecord),
		"cause":             b.serializeGqlErrorCause(cause.GqlCause),
	}}
}

func (b *backend) nextId() string {
	b.id++
	return fmt.Sprintf("%d", b.id)
}

func (b *backend) process() bool {
	request := ""
	inRequest := false

	for {
		line, err := b.rd.ReadString('\n')
		if err != nil {
			return false
		}

		switch line {
		case "#request begin\n":
			if inRequest {
				panic("Already in request")
			}
			inRequest = true
		case "#request end\n":
			if !inRequest {
				panic("End while not in request")
			}
			b.handleRequest(b.toRequest(request))
			request = ""
			inRequest = false
			return true
		default:
			if !inRequest {
				panic("Line while not in request")
			}

			request = request + line
		}
	}
}

func (b *backend) writeResponse(name string, data any) {
	response := map[string]any{"name": name, "data": data}
	responseJson, err := json.Marshal(response)
	fmt.Printf("RES: %s %s\n", name, string(responseJson))
	if err != nil {
		panic(err.Error())
	}
	// Make sure that logging framework doesn't write anything inbetween here...
	b.wrLock.Lock()
	defer b.wrLock.Unlock()
	err = b.writeLine("#response begin")
	if err != nil {
		panic(err.Error())
	}
	err = b.writeLine(string(responseJson))
	if err != nil {
		panic(err.Error())
	}
	err = b.writeLine("#response end")
	if err != nil {
		panic(err.Error())
	}
}

func (b *backend) toRequest(s string) map[string]any {
	req := map[string]any{}
	decoder := json.NewDecoder(strings.NewReader(s))
	decoder.UseNumber()
	err := decoder.Decode(&req)
	if err != nil {
		panic(fmt.Sprintf("Unable to parse: '%s' as a request: %s", s, err))
	}
	return req
}

func (b *backend) toTransactionConfigApply(data map[string]any) func(*neo4j.TransactionConfig) {
	txConfig := neo4j.TransactionConfig{Timeout: math.MinInt}
	// Optional transaction meta data
	txConfig.Metadata = b.toTxMetadata(data)
	// Optional timeout in milliseconds
	txConfig.Timeout = b.toTimeout(data)
	return func(conf *neo4j.TransactionConfig) {
		if txConfig.Metadata != nil {
			conf.Metadata = txConfig.Metadata
		}
		if txConfig.Timeout != math.MinInt {
			conf.Timeout = txConfig.Timeout
		}
	}
}

func (b *backend) toTxMetadata(data map[string]any) map[string]any {
	if data["txMeta"] != nil {
		txMetadata, err := b.toParams(data["txMeta"].(map[string]any))
		if err != nil {
			panic(err)
		}
		return txMetadata
	}
	return nil
}

func (b *backend) toTimeout(data map[string]any) time.Duration {
	if data["timeout"] != nil {
		return time.Millisecond * time.Duration(asInt64(data["timeout"].(json.Number)))
	}
	return math.MinInt
}

func (b *backend) toCypherAndParams(data map[string]any) (string, map[string]any, error) {
	rawParameters, _ := data["params"].(map[string]any)
	parameters, err := b.toParams(rawParameters)
	if err != nil {
		return "", nil, err
	}
	query := data["cypher"].(string)
	return query, parameters, nil
}

func (b *backend) toParams(parameters map[string]any) (map[string]any, error) {
	result := make(map[string]any, len(parameters))
	for name, rawParam := range parameters {
		param, err := cypherToNative(rawParam)
		if err != nil {
			return nil, err
		}
		result[name] = param
	}
	return result, nil
}

func (b *backend) toClientCertificate(data map[string]any) auth.ClientCertificate {
	clientCertificateData := data["clientCertificate"].(map[string]any)["data"].(map[string]any)
	return auth.ClientCertificate{
		CertFile: clientCertificateData["certfile"].(string),
		KeyFile:  clientCertificateData["keyfile"].(string),
		Password: b.toStringPointer(clientCertificateData["password"]),
	}
}

func (b *backend) toStringPointer(v any) *string {
	if v == nil {
		return nil
	}
	strVal, ok := v.(string)
	if !ok {
		return nil
	}
	return &strVal
}

func (b *backend) handleTransactionFunc(isRead bool, data map[string]any) {
	sid := data["sessionId"].(string)
	sessionState := b.sessionStates[sid]
	blockingRetry := func(tx neo4j.ManagedTransaction) (any, error) {
		sessionState.retryableState = retryableNothing
		// Instruct client to start doing its work
		txId := b.nextId()
		b.managedTransactions[txId] = tx
		b.writeResponse("RetryableTry", map[string]any{"id": txId})
		// Process all things that the client might do within the transaction
		for {
			b.process()
			switch sessionState.retryableState {
			case retryablePositive:
				// Client succeeded and wants to commit
				return nil, nil
			case retryableNegative:
				// Client failed in some way
				if sessionState.retryableErrorId != "" {
					return nil, b.recordedErrors[sessionState.retryableErrorId]
				} else {
					return nil, &frontendError{msg: "Error from client"}
				}
			case retryableNothing:
				// Client did something not related to the retryable state
			}
		}
	}
	var err error
	if isRead {
		_, err = sessionState.session.ExecuteRead(ctx, blockingRetry, b.toTransactionConfigApply(data))
	} else {
		_, err = sessionState.session.ExecuteWrite(ctx, blockingRetry, b.toTransactionConfigApply(data))
	}

	if err != nil {
		b.writeError(err)
	} else {
		b.writeResponse("RetryableDone", map[string]any{})
	}
}

func (b *backend) customAddressResolverFunction() config.ServerAddressResolver {
	return func(address config.ServerAddress) []config.ServerAddress {
		id := b.nextId()
		b.writeResponse("ResolverResolutionRequired", map[string]string{
			"id":      id,
			"address": fmt.Sprintf("%s:%s", address.Hostname(), address.Port()),
		})
		for {
			b.process()
			if addresses, ok := b.resolvedAddresses[id]; ok {
				delete(b.resolvedAddresses, id)
				result := make([]config.ServerAddress, len(addresses))
				for i, address := range addresses {
					result[i] = NewServerAddress(address.(string))
				}
				return result
			}
		}
	}
}

type serverAddress struct {
	hostname string
	port     string
}

func NewServerAddress(address string) config.ServerAddress {
	parsedAddress, err := url.Parse("//" + address)
	if err != nil {
		panic(err)
	}
	return serverAddress{
		hostname: parsedAddress.Hostname(),
		port:     parsedAddress.Port(),
	}
}

func (s serverAddress) Hostname() string {
	return s.hostname
}

func (s serverAddress) Port() string {
	return s.port
}

func (b *backend) handleRequest(req map[string]any) {
	name := req["name"].(string)
	data := req["data"].(map[string]any)

	dataJson, err := json.Marshal(data)
	if err != nil {
		// This data comes from a json decoder. So it better be serializable.
		panic(err)
	}

	fmt.Printf("REQ: %s %s\n", name, dataJson)
	switch name {

	case "ResolverResolutionCompleted":
		requestId := data["requestId"].(string)
		addresses := data["addresses"].([]any)
		b.resolvedAddresses[requestId] = addresses

	case "BookmarksSupplierCompleted":
		requestId := data["requestId"].(string)
		rawBookmarks := data["bookmarks"].([]any)
		bookmarks := make(neo4j.Bookmarks, len(rawBookmarks))
		for i, bookmark := range rawBookmarks {
			bookmarks[i] = bookmark.(string)
		}
		b.suppliedBookmarks[requestId] = bookmarks

	case "BookmarksConsumerCompleted":
		requestId := data["requestId"].(string)
		b.consumedBookmarks[requestId] = struct{}{}

	case "NewDriver":
		rawAuth := data["authorizationToken"]
		var err error
		var authToken auth.TokenManager
		if rawAuth == nil {
			managerId := data["authTokenManagerId"].(string)
			authToken = b.authTokenManagers[managerId]
		} else {
			authToken, err = getAuth(rawAuth.(map[string]any)["data"].(map[string]any))
			if err != nil {
				b.writeError(err)
				return
			}
		}
		// Parse URI (or rather type cast)
		uri := data["uri"].(string)
		driver, err := neo4j.NewDriverWithContext(uri, authToken, func(c *config.Config) {
			// Setup custom logger that redirects log entries back to frontend
			c.Log = &streamLog{writeLine: b.writeLineLocked}
			// Optional custom user agent from frontend
			userAgentX := data["userAgent"]
			if userAgentX != nil {
				c.UserAgent = userAgentX.(string)
			}
			if data["resolverRegistered"].(bool) {
				c.AddressResolver = b.customAddressResolverFunction()
			}
			if data["connectionAcquisitionTimeoutMs"] != nil {
				c.ConnectionAcquisitionTimeout = time.Millisecond * time.Duration(asInt64(data["connectionAcquisitionTimeoutMs"].(json.Number)))
			}
			if data["livenessCheckTimeoutMs"] != nil {
				c.ConnectionLivenessCheckTimeout = time.Millisecond * time.Duration(asInt64(data["livenessCheckTimeoutMs"].(json.Number)))
			}
			if data["maxConnectionLifetimeMs"] != nil {
				c.MaxConnectionLifetime = time.Millisecond * time.Duration(asInt64(data["maxConnectionLifetimeMs"].(json.Number)))
			}
			if data["maxConnectionPoolSize"] != nil {
				c.MaxConnectionPoolSize = asInt(data["maxConnectionPoolSize"].(json.Number))
			}
			if data["fetchSize"] != nil {
				c.FetchSize = asInt(data["fetchSize"].(json.Number))
			}
			if data["maxTxRetryTimeMs"] != nil {
				c.MaxTransactionRetryTime = time.Millisecond * time.Duration(asInt64(data["maxTxRetryTimeMs"].(json.Number)))
			}
			if data["connectionTimeoutMs"] != nil {
				c.SocketConnectTimeout = time.Millisecond * time.Duration(asInt64(data["connectionTimeoutMs"].(json.Number)))
			}
			if data["notificationsMinSeverity"] != nil {
				minSeverity, err := mapNotificationMinSeverityLevel(data["notificationsMinSeverity"].(string))
				if err != nil {
					b.writeError(err)
					return
				}
				c.NotificationsMinSeverity = minSeverity
			}
			if data["notificationsDisabledCategories"] != nil {
				notiDisCats := data["notificationsDisabledCategories"].([]any)
				if len(notiDisCats) == 0 {
					c.NotificationsDisabledCategories = notifications.DisableNoCategories()
				} else {
					cats := convertSlice(notiDisCats, anyToNotificationCategory)
					c.NotificationsDisabledCategories = notifications.DisableCategories(cats...)
				}
			}
			if data["telemetryDisabled"] != nil {
				c.TelemetryDisabled = data["telemetryDisabled"].(bool)
			}

			clientCertificateProviderId := data["clientCertificateProviderId"]
			if clientCertificateProviderId != nil {
				provider := b.clientCertificateProviders[clientCertificateProviderId.(string)]
				c.ClientCertificateProvider = provider
			} else {
				if data["clientCertificate"] != nil {
					clientCertificate := b.toClientCertificate(data)
					provider, err := auth.NewStaticClientCertificateProvider(clientCertificate)
					if err != nil {
						b.writeError(err)
						return
					}
					c.ClientCertificateProvider = provider
				}
			}
		})
		if err != nil {
			b.writeError(err)
			return
		}
		idKey := b.nextId()
		b.drivers[idKey] = driver
		b.writeResponse("Driver", map[string]any{"id": idKey})

	case "NewClientCertificateProvider":
		provider := NewTestKitClientCertificateProvider(b.nextId(), b)
		b.clientCertificateProviders[provider.id] = TestKitClientCertificateProvider{id: provider.id, backend: b}
		b.writeResponse("ClientCertificateProvider", map[string]any{"id": provider.id})

	case "ClientCertificateProviderClose":
		providerId := data["id"].(string)
		delete(b.clientCertificateProviders, providerId)
		b.writeResponse("ClientCertificateProvider", map[string]any{"id": providerId})

	case "ClientCertificateProviderCompleted":
		requestId := data["requestId"].(string)
		if data["clientCertificate"] != nil {
			clientCertificate := b.toClientCertificate(data)
			b.resolvedClientCertificates[requestId] = clientCertificate
		} else {
			b.resolvedClientCertificates[requestId] = auth.ClientCertificate{}
		}

	case "DriverClose":
		driverId := data["driverId"].(string)
		driver := b.drivers[driverId]
		err := driver.Close(ctx)
		if err != nil {
			b.writeError(err)
			return
		}
		b.writeResponse("Driver", map[string]any{"id": driverId})

	case "GetServerInfo":
		driverId := data["driverId"].(string)
		driver := b.drivers[driverId]
		serverInfo, err := driver.GetServerInfo(context.Background())
		if err != nil {
			b.writeError(err)
			return
		}
		protocolVersion := serverInfo.ProtocolVersion()
		b.writeResponse("ServerInfo", map[string]any{
			"address":         serverInfo.Address(),
			"agent":           serverInfo.Agent(),
			"protocolVersion": fmt.Sprintf("%d.%d", protocolVersion.Major, protocolVersion.Minor),
		})

	case "ExecuteQuery":
		driver := b.drivers[data["driverId"].(string)]
		var configurers []neo4j.ExecuteQueryConfigurationOption
		if rawConfig := data["config"]; rawConfig != nil {
			executeQueryConfig := rawConfig.(map[string]any)
			configurers = append(configurers, func(config *neo4j.ExecuteQueryConfiguration) {
				config.BoltLogger = &streamLog{writeLine: b.writeLineLocked}

				routing := executeQueryConfig["routing"]
				if routing != nil {
					switch routing {
					case "r":
						config.Routing = neo4j.Read
					case "w":
						config.Routing = neo4j.Write
					default:
						b.writeError(fmt.Errorf("unexpected executequery routing value: %v", routing))
						return
					}
				}
				impersonatedUser := executeQueryConfig["impersonatedUser"]
				if impersonatedUser != nil {
					config.ImpersonatedUser = impersonatedUser.(string)
				}
				database := executeQueryConfig["database"]
				if database != nil {
					config.Database = database.(string)
				}
				bookmarkManagerId := executeQueryConfig["bookmarkManagerId"]
				if bookmarkManagerId != nil {
					if number, ok := bookmarkManagerId.(json.Number); ok {
						id := number.String()
						if id != "-1" {
							b.writeError(fmt.Errorf("unexpected bookmark manager id: %s", id))
							return
						}
						config.BookmarkManager = nil
					} else {
						config.BookmarkManager = b.bookmarkManagers[bookmarkManagerId.(string)]
					}
				}
				// Append configurers to config if they exist.
				if executeQueryConfig["timeout"] != nil {
					config.TransactionConfigurers = append(config.TransactionConfigurers, neo4j.WithTxTimeout(b.toTimeout(executeQueryConfig)))
				}
				if executeQueryConfig["txMeta"] != nil {
					config.TransactionConfigurers = append(config.TransactionConfigurers, neo4j.WithTxMetadata(b.toTxMetadata(executeQueryConfig)))
				}
				// Append Auth configuration if it exists
				if executeQueryConfig["authorizationToken"] != nil {
					token, err := getAuth(executeQueryConfig["authorizationToken"].(map[string]any)["data"].(map[string]any))
					if err != nil {
						b.writeError(err)
						return
					}
					config.Auth = &token
				}
			})
		}

		cypher, params, err := b.toCypherAndParams(data)
		if err != nil {
			b.writeError(err)
			return
		}
		eagerResult, err := neo4j.ExecuteQuery[*neo4j.EagerResult](
			ctx, driver, cypher, params, neo4j.EagerResultTransformer, configurers...)
		if err != nil {
			b.writeError(err)
			return
		}
		b.writeResponse("EagerResult", map[string]any{
			"keys":    eagerResult.Keys,
			"records": serializeRecords(eagerResult.Records),
			"summary": serializeSummary(eagerResult.Summary),
		})

	case "NewSession":
		driver := b.drivers[data["driverId"].(string)]
		sessionConfig := neo4j.SessionConfig{
			BoltLogger: &streamLog{writeLine: b.writeLineLocked},
		}
		if data["accessMode"] != nil {
			switch data["accessMode"].(string) {
			case "r":
				sessionConfig.AccessMode = neo4j.AccessModeRead
			case "w":
				sessionConfig.AccessMode = neo4j.AccessModeWrite
			default:
				b.writeError(errors.New("Unknown access mode: " + data["accessMode"].(string)))
				return
			}
		}
		if data["bookmarks"] != nil {
			rawBookmarks := data["bookmarks"].([]any)
			bookmarks := make([]string, len(rawBookmarks))
			for i, x := range rawBookmarks {
				bookmarks[i] = x.(string)
			}
			sessionConfig.Bookmarks = neo4j.BookmarksFromRawValues(bookmarks...)
		}
		if data["database"] != nil {
			sessionConfig.DatabaseName = data["database"].(string)
		}
		if data["fetchSize"] != nil {
			sessionConfig.FetchSize = asInt(data["fetchSize"].(json.Number))
		}
		if data["impersonatedUser"] != nil {
			sessionConfig.ImpersonatedUser = data["impersonatedUser"].(string)
		}
		if data["bookmarkManagerId"] != nil {
			bmmId := data["bookmarkManagerId"].(string)
			bookmarkManager := b.bookmarkManagers[bmmId]
			if bookmarkManager == nil {
				b.writeError(fmt.Errorf("could not find bookmark manager with ID %s", bmmId))
				return
			}
			sessionConfig.BookmarkManager = bookmarkManager
		}

		if data["notificationsMinSeverity"] != nil {
			minSeverity, err := mapNotificationMinSeverityLevel(data["notificationsMinSeverity"].(string))
			if err != nil {
				b.writeError(err)
				return
			}
			sessionConfig.NotificationsMinSeverity = minSeverity
		}
		if data["notificationsDisabledCategories"] != nil {
			notiDisCats := data["notificationsDisabledCategories"].([]any)
			if len(notiDisCats) == 0 {
				sessionConfig.NotificationsDisabledCategories = notifications.DisableNoCategories()
			} else {
				cats := convertSlice(notiDisCats, anyToNotificationCategory)
				sessionConfig.NotificationsDisabledCategories = notifications.DisableCategories(cats...)
			}
		}
		if data["authorizationToken"] != nil {
			authToken, err := getAuth(data["authorizationToken"].(map[string]any)["data"].(map[string]any))
			if err != nil {
				b.writeError(err)
				return
			}
			sessionConfig.Auth = &authToken
		}
		session := driver.NewSession(ctx, sessionConfig)
		idKey := b.nextId()
		b.sessionStates[idKey] = &sessionState{session: session}
		b.writeResponse("Session", map[string]any{"id": idKey})

	case "NewBookmarkManager":
		bookmarkManagerId := b.nextId()
		b.bookmarkManagers[bookmarkManagerId] = neo4j.NewBookmarkManager(
			b.bookmarkManagerConfig(bookmarkManagerId, data))
		b.writeResponse("BookmarkManager", map[string]any{
			"id": bookmarkManagerId,
		})

	case "BookmarkManagerClose":
		bookmarkManagerId := data["id"].(string)
		delete(b.bookmarkManagers, bookmarkManagerId)
		b.writeResponse("BookmarkManager", map[string]any{
			"id": bookmarkManagerId,
		})

	case "SessionClose":
		sessionId := data["sessionId"].(string)
		sessionState := b.sessionStates[sessionId]
		err := sessionState.session.Close(ctx)
		if err != nil {
			b.writeError(err)
			return
		}
		b.writeResponse("Session", map[string]any{"id": sessionId})

	case "SessionRun":
		sessionState := b.sessionStates[data["sessionId"].(string)]
		cypher, params, err := b.toCypherAndParams(data)
		if err != nil {
			b.writeError(err)
			return
		}
		result, err := sessionState.session.Run(ctx, cypher, params, b.toTransactionConfigApply(data))
		if err != nil {
			b.writeError(err)
			return
		}
		keys, err := result.Keys()
		if err != nil {
			b.writeError(err)
			return
		}
		idKey := b.nextId()
		b.results[idKey] = result
		b.writeResponse("Result", map[string]any{"id": idKey, "keys": keys})

	case "SessionBeginTransaction":
		sessionState := b.sessionStates[data["sessionId"].(string)]
		tx, err := sessionState.session.BeginTransaction(ctx, b.toTransactionConfigApply(data))
		if err != nil {
			b.writeError(err)
			return
		}
		idKey := b.nextId()
		b.explicitTransactions[idKey] = tx
		b.writeResponse("Transaction", map[string]any{"id": idKey})

	case "SessionLastBookmarks":
		sessionState := b.sessionStates[data["sessionId"].(string)]
		bookmarks := neo4j.BookmarksToRawValues(sessionState.session.LastBookmarks())
		if bookmarks == nil {
			bookmarks = []string{}
		}
		b.writeResponse("Bookmarks", map[string]any{"bookmarks": bookmarks})

	case "TransactionRun":
		// ManagedTransaction is compatible with ExplicitTransaction
		// and is all that is needed for TransactionRun
		var tx neo4j.ManagedTransaction
		var found bool
		transactionId := data["txId"].(string)
		if tx, found = b.explicitTransactions[transactionId]; !found {
			tx = b.managedTransactions[transactionId]
		}
		cypher, params, err := b.toCypherAndParams(data)
		if err != nil {
			b.writeError(err)
			return
		}
		result, err := tx.Run(ctx, cypher, params)
		if err != nil {
			b.writeError(err)
			return
		}
		keys, err := result.Keys()
		if err != nil {
			b.writeError(err)
			return
		}
		idKey := b.nextId()
		b.results[idKey] = result
		b.writeResponse("Result", map[string]any{"id": idKey, "keys": keys})

	case "TransactionCommit":
		txId := data["txId"].(string)
		tx := b.explicitTransactions[txId]
		err := tx.Commit(ctx)
		if err != nil {
			b.writeError(err)
			return
		}
		b.writeResponse("Transaction", map[string]any{"id": txId})

	case "TransactionRollback":
		txId := data["txId"].(string)
		tx := b.explicitTransactions[txId]
		err := tx.Rollback(ctx)
		if err != nil {
			b.writeError(err)
			return
		}
		b.writeResponse("Transaction", map[string]any{"id": txId})

	case "TransactionClose":
		txId := data["txId"].(string)
		tx := b.explicitTransactions[txId]
		err := tx.Close(ctx)
		if err != nil {
			b.writeError(err)
			return
		}
		b.writeResponse("Transaction", map[string]any{"id": txId})

	case "SessionReadTransaction":
		b.handleTransactionFunc(true, data)

	case "SessionWriteTransaction":
		b.handleTransactionFunc(false, data)

	case "RetryablePositive":
		sessionState := b.sessionStates[data["sessionId"].(string)]
		sessionState.retryableState = retryablePositive

	case "RetryableNegative":
		sessionState := b.sessionStates[data["sessionId"].(string)]
		sessionState.retryableState = retryableNegative
		sessionState.retryableErrorId = data["errorId"].(string)

	case "ResultNext":
		result := b.results[data["resultId"].(string)]
		more := result.Next(ctx)
		b.writeRecord(result, result.Record(), more)
	case "ResultPeek":
		result := b.results[data["resultId"].(string)]
		var record *db.Record = nil
		more := result.PeekRecord(ctx, &record)
		b.writeRecord(result, record, more)
	case "ResultList":
		result := b.results[data["resultId"].(string)]
		records, err := result.Collect(ctx)
		if err != nil {
			b.writeError(err)
			return
		}
		b.writeResponse("RecordList", map[string]any{
			"records": serializeRecords(records),
		})
	case "ResultConsume":
		result := b.results[data["resultId"].(string)]
		summary, err := result.Consume(ctx)
		if err != nil {
			b.writeError(err)
			return
		}
		b.writeResponse("Summary", serializeSummary(summary))

	case "ForcedRoutingTableUpdate":
		databaseRaw := data["database"]
		var database string
		if databaseRaw != nil {
			database = databaseRaw.(string)
		}
		var bookmarks []string
		bookmarksRaw := data["bookmarks"]
		if bookmarksRaw != nil {
			bookmarksSlice := bookmarksRaw.([]any)
			bookmarks = make([]string, len(bookmarksSlice))
			for i, bookmark := range bookmarksSlice {
				bookmarks[i] = bookmark.(string)
			}
		}
		driverId := data["driverId"].(string)
		driver := b.drivers[driverId]
		err := neo4j.ForceRoutingTableUpdate(driver, database, bookmarks, &streamLog{writeLine: b.writeLineLocked})
		if err != nil {
			b.writeError(err)
			return
		}
		b.writeResponse("Driver", map[string]any{"id": driverId})

	case "GetRoutingTable":
		driver := b.drivers[data["driverId"].(string)]
		databaseRaw := data["database"]
		var database string
		if databaseRaw != nil {
			database = databaseRaw.(string)
		}
		table, err := neo4j.GetRoutingTable(driver, database)
		if err != nil {
			b.writeError(err)
			return
		}
		var databaseName any = table.DatabaseName
		if databaseName == "" {
			databaseName = nil
		}
		b.writeResponse("RoutingTable", map[string]any{
			"database": databaseName,
			"ttl":      table.TimeToLive,
			"routers":  table.Routers,
			"readers":  table.Readers,
			"writers":  table.Writers,
		})

	case "CheckMultiDBSupport":
		driver := b.drivers[data["driverId"].(string)]
		session := driver.NewSession(ctx, neo4j.SessionConfig{
			BoltLogger: log.BoltToConsole(),
		})
		result, err := session.Run(ctx, "RETURN 42", nil)
		defer func() {
			err = session.Close(ctx)
			if err != nil {
				b.writeError(fmt.Errorf("could not check multi DB support: %w", err))
			}
		}()
		if err != nil {
			b.writeError(fmt.Errorf("could not check multi DB support: %w", err))
			return
		}
		summary, err := result.Consume(ctx)
		if err != nil {
			b.writeError(fmt.Errorf("could not check multi DB support: %w", err))
			return
		}

		server := summary.Server()
		isMultiTenant := server.ProtocolVersion().Major >= 4
		b.writeResponse("MultiDBSupport", map[string]any{
			"id":        b.nextId(),
			"available": isMultiTenant,
		})

	case "CheckDriverIsEncrypted":
		driver := b.drivers[data["driverId"].(string)]
		b.writeResponse("DriverIsEncrypted", map[string]any{
			"encrypted": driver.IsEncrypted(),
		})

	case "VerifyConnectivity":
		driverId := data["driverId"].(string)
		if err := b.drivers[driverId].VerifyConnectivity(ctx); err != nil {
			b.writeError(err)
			return
		}
		b.writeResponse("Driver", map[string]any{"id": driverId})

	case "FakeTimeInstall":
		if err := neo4j.FreezeTime(); err != nil {
			b.writeError(err)
			return
		}
		b.writeResponse("FakeTimeAck", nil)

	case "FakeTimeUninstall":
		if err := neo4j.UnfreezeTime(); err != nil {
			b.writeError(err)
			return
		}
		b.writeResponse("FakeTimeAck", nil)

	case "FakeTimeTick":
		milliseconds := asInt64(data["incrementMs"].(json.Number))
		if err := neo4j.TickTime(time.Duration(milliseconds) * time.Millisecond); err != nil {
			b.writeError(err)
			return
		}
		b.writeResponse("FakeTimeAck", nil)

	case "VerifyAuthentication":
		driverId := data["driverId"].(string)
		var token *neo4j.AuthToken
		if data["authorizationToken"] != nil {
			authToken, err := getAuth(data["authorizationToken"].(map[string]any)["data"].(map[string]any))
			if err != nil {
				b.writeError(err)
				return
			}
			token = &authToken
		}
		if err := b.drivers[driverId].VerifyAuthentication(ctx, token); err != nil {
			invalidAuthError := &neo4j.InvalidAuthenticationError{}
			if errors.As(err, &invalidAuthError) {
				b.writeResponse("DriverIsAuthenticated", map[string]any{"id": driverId, "authenticated": false})
			} else {
				b.writeError(err)
			}
		} else {
			b.writeResponse("DriverIsAuthenticated", map[string]any{"id": driverId, "authenticated": true})
		}

	case "NewAuthTokenManager":
		managerId := b.nextId()
		manager := GenericTokenManager{
			GetAuthTokenFunc: func() neo4j.AuthToken {
				id := b.nextId()
				b.writeResponse(
					"AuthTokenManagerGetAuthRequest",
					map[string]any{
						"id":                 id,
						"authTokenManagerId": managerId,
					})
				for {
					b.process()
					if token, ok := b.resolvedGetAuthTokens[id]; ok {
						delete(b.resolvedGetAuthTokens, id)
						return token
					}
				}
			},
			HandleSecurityExceptionFunc: func(token neo4j.AuthToken, error *db.Neo4jError) bool {
				id := b.nextId()
				b.writeResponse(
					"AuthTokenManagerHandleSecurityExceptionRequest",
					map[string]any{
						"id":                 id,
						"authTokenManagerId": managerId,
						"auth":               serializeAuth(token),
						"errorCode":          error.Code,
					})
				for {
					b.process()
					if handled, ok := b.resolvedHandleSecurityException[id]; ok {
						delete(b.resolvedHandleSecurityException, id)
						return handled
					}
				}
			},
		}
		b.authTokenManagers[managerId] = manager
		b.writeResponse("AuthTokenManager", map[string]any{"id": managerId})
	case "AuthTokenManagerGetAuthCompleted":
		id := data["requestId"].(string)
		token, err := getAuth(data["auth"].(map[string]any)["data"].(map[string]any))
		if err != nil {
			b.writeError(err)
			return
		}
		b.resolvedGetAuthTokens[id] = token
	case "AuthTokenManagerHandleSecurityExceptionCompleted":
		handled := data["handled"].(bool)
		id := data["requestId"].(string)
		b.resolvedHandleSecurityException[id] = handled
	case "NewBasicAuthTokenManager":
		managerId := b.nextId()

		manager := auth.BasicTokenManager(
			func(context.Context) (neo4j.AuthToken, error) {
				id := b.nextId()
				b.writeResponse(
					"BasicAuthTokenProviderRequest",
					map[string]any{
						"id":                      id,
						"basicAuthTokenManagerId": managerId,
					})
				for {
					b.process()
					if basicToken, ok := b.resolvedBasicTokens[id]; ok {
						delete(b.resolvedBasicTokens, id)
						return basicToken.token, nil
					}
				}
			})
		b.authTokenManagers[managerId] = manager
		b.writeResponse("BasicAuthTokenManager", map[string]any{"id": managerId})
	case "BasicAuthTokenProviderCompleted":
		id := data["requestId"].(string)
		token, _ := getAuth(data["auth"].(map[string]any)["data"].(map[string]any))
		b.resolvedBasicTokens[id] = AuthToken{token}
	case "NewBearerAuthTokenManager":
		managerId := b.nextId()

		manager := auth.BearerTokenManager(
			func(context.Context) (neo4j.AuthToken, *time.Time, error) {
				id := b.nextId()
				b.writeResponse(
					"BearerAuthTokenProviderRequest",
					map[string]any{
						"id":                       id,
						"bearerAuthTokenManagerId": managerId,
					})
				for {
					b.process()
					if bearerToken, ok := b.resolvedBearerTokens[id]; ok {
						delete(b.resolvedBearerTokens, id)
						return bearerToken.token, bearerToken.expiration, nil
					}
				}
			})
		b.authTokenManagers[managerId] = manager
		b.writeResponse("BearerAuthTokenManager", map[string]any{"id": managerId})
	case "BearerAuthTokenProviderCompleted":
		id := data["requestId"].(string)
		bearerToken := data["auth"].(map[string]any)["data"].(map[string]any)
		token, err := getAuth(bearerToken["auth"].(map[string]any)["data"].(map[string]any))
		if err != nil {
			b.writeError(err)
			return
		}
		var expiration *time.Time
		expiresInRaw := bearerToken["expiresInMs"]
		if expiresInRaw != nil {
			expiresIn := time.Millisecond * time.Duration(asInt64(bearerToken["expiresInMs"].(json.Number)))
			expirationTime := neo4j.Now().Add(expiresIn)
			expiration = &expirationTime
		}
		b.resolvedBearerTokens[id] = AuthTokenAndExpiration{token, expiration}
	case "AuthTokenManagerClose":
		id := data["id"].(string)
		delete(b.authTokenManagers, id)
		b.writeResponse("AuthTokenManager", map[string]any{"id": id})

	case "GetFeatures":
		b.writeResponse("FeatureList", map[string]any{
			"features": []string{
				// === FUNCTIONAL FEATURES ===
				"Feature:API:BookmarkManager",
				"Feature:API:ConnectionAcquisitionTimeout",
				"Feature:API:Driver.ExecuteQuery",
				"Feature:API:Driver.ExecuteQuery:WithAuth",
				"Feature:API:Driver:GetServerInfo",
				"Feature:API:Driver.IsEncrypted",
				"Feature:API:Driver:MaxConnectionLifetime",
				"Feature:API:Driver:NotificationsConfig",
				"Feature:API:Driver.VerifyAuthentication",
				"Feature:API:Driver.VerifyConnectivity",
				//"Feature:API:Driver.SupportsSessionAuth",
				"Feature:API:Liveness.Check",
				"Feature:API:Result.List",
				"Feature:API:Result.Peek",
				//"Feature:API:Result.Single",
				//"Feature:API:Result.SingleOptional",
				"Feature:API:RetryableExceptions",
				"Feature:API:Session:AuthConfig",
				"Feature:API:Session:NotificationsConfig",
				"Feature:API:SSLClientCertificate",
				//"Feature:API:SSLConfig",
				//"Feature:API:SSLSchemes",
				"Feature:API:Summary:GqlStatusObjects",
				"Feature:API:Type.Spatial",
				"Feature:API:Type.Temporal",
				"Feature:Auth:Bearer",
				"Feature:Auth:Custom",
				"Feature:Auth:Kerberos",
				"Feature:Auth:Managed",
				"Feature:Bolt:3.0",
				"Feature:Bolt:4.2",
				"Feature:Bolt:4.3",
				"Feature:Bolt:4.4",
				"Feature:Bolt:5.0",
				"Feature:Bolt:5.1",
				"Feature:Bolt:5.2",
				"Feature:Bolt:5.3",
				"Feature:Bolt:5.4",
				"Feature:Bolt:5.5",
				"Feature:Bolt:5.6",
				"Feature:Bolt:5.7",
				//"Feature:Bolt:5.8",
				//"Feature:Bolt:HandshakeManifestV1",
				"Feature:Bolt:Patch:UTC",
				"Feature:Bolt:HandshakeManifestV1",
				"Feature:Impersonation",
				//"Feature:TLS:1.1",
				"Feature:TLS:1.2",
				"Feature:TLS:1.3",

				// === OPTIMIZATIONS ===
				"AuthorizationExpiredTreatment",
				"Optimization:AuthPipelining",
				"Optimization:ConnectionReuse",
				"Optimization:EagerTransactionBegin",
				"Optimization:ExecuteQueryPipelining",
				//"Optimization:HomeDatabaseCache",
				//"Optimization:HomeDbCacheBasicPrincipalIsImpersonatedUser",
				"Optimization:ImplicitDefaultArguments",
				"Optimization:MinimalBookmarksSet",
				"Optimization:MinimalResets",
				//"Optimization:MinimalVerifyAuthentication",
				"Optimization:PullPipelining",
				//"Optimization:ResultListFetchAll",

				// === IMPLEMENTATION DETAILS ===
				"Detail:ClosedDriverIsEncrypted",
				"Detail:DefaultSecurityConfigValueEquality",
				//"Detail:NumberIsNumber",

				// === CONFIGURATION HINTS (BOLT 4.3+) ===
				"ConfHint:connection.recv_timeout_seconds",

				// === BACKEND FEATURES FOR TESTING ===
				"Backend:MockTime",
				"Backend:RTFetch",
				"Backend:RTForceUpdate",
			},
		})

	case "StartTest":
		testName := data["testName"].(string)
		if reason, ok := mustSkip(testName); ok {
			b.writeResponse("SkipTest", map[string]any{"reason": reason})
			return
		}
		if strings.Contains(testName, "test_should_echo_all_timezone_ids") ||
			strings.Contains(testName, "test_date_time_cypher_created_tz_id") {
			b.writeResponse("RunSubTests", nil)
			return
		}
		b.writeResponse("RunTest", nil)

	case "StartSubTest":
		testName := data["testName"].(string)
		arguments := data["subtestArguments"].(map[string]any)
		if reason, ok := mustSkipSubTest(testName, arguments); ok {
			b.writeResponse("SkipTest", map[string]any{"reason": reason})
			return
		}
		b.writeResponse("RunTest", nil)

	default:
		b.writeError(errors.New("Unknown request: " + name))
	}
}

func getAuth(authTokenMap map[string]any) (neo4j.AuthToken, error) {
	var authToken neo4j.AuthToken
	switch authTokenMap["scheme"] {
	case "basic":
		realm, ok := authTokenMap["realm"].(string)
		if !ok {
			realm = ""
		}
		authToken = neo4j.BasicAuth(
			authTokenMap["principal"].(string),
			authTokenMap["credentials"].(string),
			realm)
	case "kerberos":
		authToken = neo4j.KerberosAuth(authTokenMap["credentials"].(string))
	case "bearer":
		authToken = neo4j.BearerAuth(authTokenMap["credentials"].(string))
	default:
		parameters := authTokenMap["parameters"].(map[string]any)
		if err := patchNumbersInMap(parameters); err != nil {
			return neo4j.AuthToken{}, err
		}
		authToken = neo4j.CustomAuth(
			authTokenMap["scheme"].(string),
			authTokenMap["principal"].(string),
			authTokenMap["credentials"].(string),
			authTokenMap["realm"].(string),
			parameters)
	}
	return authToken, nil
}

func serializeAuth(token neo4j.AuthToken) map[string]any {
	return map[string]any{
		"name": "AuthorizationToken",
		"data": token.Tokens,
	}
}

func (b *backend) writeRecord(result neo4j.ResultWithContext, record *neo4j.Record, expectRecord bool) {
	if expectRecord && record == nil {
		b.writeResponse("BackendError", map[string]any{
			"msg": "Found no record where one was expected.",
		})
	} else if !expectRecord && record != nil {
		b.writeResponse("BackendError", map[string]any{
			"msg": "Found a record where none was expected.",
		})
	}

	if record != nil {
		if invalidValue := firstRecordInvalidValue(record); invalidValue != nil {
			b.writeError(&db.ProtocolError{
				MessageType: invalidValue.Message,
				Err:         invalidValue.Err.Error(),
			})
			return
		}
		b.writeResponse("Record", serializeRecord(record))
	} else {
		err := result.Err()
		if err != nil && err.Error() != "result cursor is not available anymore" {
			b.writeError(err)
			return
		}
		b.writeResponse("NullRecord", nil)
	}
}

func mustSkip(testName string) (string, bool) {
	skippedTests := testSkips()
	for testPattern, exclusionReason := range skippedTests {
		if matches(testPattern, testName) {
			return exclusionReason, true
		}
	}
	return "", false
}

func mustSkipSubTest(testName string, arguments map[string]any) (string, bool) {
	if strings.Contains(testName, "test_should_echo_all_timezone_ids") {
		return mustSkipTimeZoneSubTest(arguments)
	}
	return "", false
}

func matches(pattern, testName string) bool {
	if pattern == testName {
		return true
	}
	if !strings.Contains(pattern, "*") {
		return false
	}
	regex := asRegex(pattern)
	return regex.MatchString(testName)
}

func asRegex(rawPattern string) *regexp.Regexp {
	pattern := regexp.QuoteMeta(rawPattern)
	pattern = strings.ReplaceAll(pattern, `\*`, ".*")
	return regexp.MustCompile(pattern)
}

func serializeRecords(records []*neo4j.Record) []any {
	response := make([]any, len(records))
	for i, record := range records {
		response[i] = serializeRecord(record)
	}
	return response
}

func serializeRecord(record *neo4j.Record) map[string]any {
	values := record.Values
	cypherValues := make([]any, len(values))
	for i, v := range values {
		cypherValues[i] = nativeToCypher(v)
	}
	data := map[string]any{"values": cypherValues}
	return data
}

func serializeNotifications(slice []neo4j.Notification, version db.ProtocolVersion) []map[string]any {
	if slice == nil {
		if version.Major == 5 && version.Minor >= 5 {
			return []map[string]any{}
		}
		return nil
	}
	if len(slice) == 0 {
		return []map[string]any{}
	}
	var res []map[string]any
	for i, notification := range slice {
		res = append(res, map[string]any{
			"code":        notification.Code(),
			"title":       notification.Title(),
			"description": notification.Description(),
			//lint:ignore SA1019 Severity is supported at least until 6.0
			"severity":         notification.Severity(),
			"severityLevel":    string(notification.SeverityLevel()),
			"rawSeverityLevel": notification.RawSeverityLevel(),
			"category":         string(notification.Category()),
			"rawCategory":      notification.RawCategory(),
		})
		if notification.Position() != nil {
			res[i]["position"] = map[string]any{
				"offset": notification.Position().Offset(),
				"line":   notification.Position().Line(),
				"column": notification.Position().Column(),
			}
		}
	}
	return res
}

func serializeGqlStatusObjects(slice []neo4j.GqlStatusObject) []map[string]any {
	var res []map[string]any
	for i, status := range slice {
		res = append(res, map[string]any{
			"isNotification":    status.IsNotification(),
			"gqlStatus":         status.GqlStatus(),
			"statusDescription": status.StatusDescription(),
			"rawClassification": emptyStringToNil(status.RawClassification()),
			"classification":    string(status.Classification()),
			"rawSeverity":       emptyStringToNil(status.RawSeverity()),
			"severity":          string(status.Severity()),
			"diagnosticRecord":  serializeParameters(status.DiagnosticRecord()),
		})
		if status.Position() != nil {
			res[i]["position"] = map[string]any{
				"offset": status.Position().Offset(),
				"line":   status.Position().Line(),
				"column": status.Position().Column(),
			}
		} else {
			res[i]["position"] = nil
		}
	}
	return res
}

func emptyStringToNil(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func serializeSummary(summary neo4j.ResultSummary) map[string]any {
	serverInfo := summary.Server()
	counters := summary.Counters()
	protocolVersion := serverInfo.ProtocolVersion()
	response := map[string]any{
		"serverInfo": map[string]any{
			"protocolVersion": fmt.Sprintf("%d.%d", protocolVersion.Major, protocolVersion.Minor),
			"agent":           serverInfo.Agent(),
			"address":         serverInfo.Address(),
		},
		"counters": map[string]any{
			"constraintsAdded":      counters.ConstraintsAdded(),
			"constraintsRemoved":    counters.ConstraintsRemoved(),
			"containsSystemUpdates": counters.ContainsSystemUpdates(),
			"containsUpdates":       counters.ContainsUpdates(),
			"indexesAdded":          counters.IndexesAdded(),
			"indexesRemoved":        counters.IndexesRemoved(),
			"labelsAdded":           counters.LabelsAdded(),
			"labelsRemoved":         counters.LabelsRemoved(),
			"nodesCreated":          counters.NodesCreated(),
			"nodesDeleted":          counters.NodesDeleted(),
			"propertiesSet":         counters.PropertiesSet(),
			"relationshipsCreated":  counters.RelationshipsCreated(),
			"relationshipsDeleted":  counters.RelationshipsDeleted(),
			"systemUpdates":         counters.SystemUpdates(),
		},
		"query": map[string]any{
			"text":       summary.Query().Text(),
			"parameters": serializeParameters(summary.Query().Parameters()),
		},
		"notifications":    serializeNotifications(summary.Notifications(), protocolVersion),
		"gqlStatusObjects": serializeGqlStatusObjects(summary.GqlStatusObjects()),
		"plan":             serializePlan(summary.Plan()),
		"profile":          serializeProfile(summary.Profile()),
	}
	if summary.ResultAvailableAfter() >= 0 {
		response["resultAvailableAfter"] = summary.ResultAvailableAfter().Milliseconds()
	} else {
		response["resultAvailableAfter"] = nil
	}
	if summary.ResultConsumedAfter() >= 0 {
		response["resultConsumedAfter"] = summary.ResultConsumedAfter().Milliseconds()
	} else {
		response["resultConsumedAfter"] = nil
	}
	if summary.StatementType() != neo4j.StatementTypeUnknown {
		response["queryType"] = summary.StatementType().String()
	} else {
		response["queryType"] = nil
	}
	if summary.Database() != nil {
		response["database"] = summary.Database().Name()
	} else {
		response["database"] = nil
	}
	return response
}

func serializePlans(children []neo4j.Plan) []map[string]any {
	result := make([]map[string]any, len(children))
	for i, child := range children {
		result[i] = serializePlan(child)
	}
	return result
}

func serializePlan(plan neo4j.Plan) map[string]any {
	if plan == nil {
		return nil
	}
	return map[string]any{
		"args":         plan.Arguments(),
		"operatorType": plan.Operator(),
		"children":     serializePlans(plan.Children()),
		"identifiers":  plan.Identifiers(),
	}
}

func serializeProfile(profile neo4j.ProfiledPlan) map[string]any {
	if profile == nil {
		return nil
	}
	result := map[string]any{
		"args":         profile.Arguments(),
		"children":     serializeProfiles(profile.Children()),
		"dbHits":       profile.DbHits(),
		"identifiers":  profile.Identifiers(),
		"operatorType": profile.Operator(),
		"rows":         profile.Records(),
	}
	return result
}

func serializeProfiles(children []neo4j.ProfiledPlan) []map[string]any {
	result := make([]map[string]any, len(children))
	for i, child := range children {
		childProfile := serializeProfile(child)
		childProfile["pageCacheMisses"] = child.PageCacheMisses()
		childProfile["pageCacheHits"] = child.PageCacheHits()
		childProfile["pageCacheHitRatio"] = child.PageCacheHitRatio()
		childProfile["time"] = child.Time()
		result[i] = childProfile
	}
	return result
}

func serializeParameters(parameters map[string]any) map[string]any {
	result := make(map[string]any, len(parameters))
	for k, parameter := range parameters {
		result[k] = nativeToCypher(parameter)
	}
	return result
}

func firstRecordInvalidValue(record *db.Record) *neo4j.InvalidValue {
	if record == nil {
		return nil
	}
	for _, value := range record.Values {
		if result, ok := value.(*neo4j.InvalidValue); ok {
			return result
		}
	}
	return nil
}

// you can use '*' as wildcards anywhere in the qualified test name (useful to exclude a whole class e.g.)
func testSkips() map[string]string {
	return map[string]string{
		// Won't fix - accepted/idiomatic behavioral differences
		"stub.iteration.test_result_scope.TestResultScope.*":                                                                                       "Won't fix - Results are always valid but don't return records when out of scope",
		"stub.connectivity_check.test_get_server_info.TestGetServerInfo.test_routing_fail_when_no_reader_are_available":                            "Won't fix - Go driver retries routing table when no readers are available",
		"stub.connectivity_check.test_verify_connectivity.TestVerifyConnectivity.test_routing_fail_when_no_reader_are_available":                   "Won't fix - Go driver retries routing table when no readers are available",
		"stub.driver_parameters.test_connection_acquisition_timeout_ms.TestConnectionAcquisitionTimeoutMs.test_does_not_encompass_router_*":        "Won't fix - ConnectionAcquisitionTimeout spans the whole process including db resolution, RT updates, connection acquisition from the pool, and creation of new connections.",
		"stub.driver_parameters.test_connection_acquisition_timeout_ms.TestConnectionAcquisitionTimeoutMs.test_router_handshake_has_own_timeout_*": "Won't fix - ConnectionAcquisitionTimeout spans the whole process including db resolution, RT updates, connection acquisition from the pool, and creation of new connections.",
		"stub.routing.test_routing_v*.RoutingV*.test_should_successfully_check_if_support_for_multi_db_is_available":                               "Won't fix - driver.SupportsMultiDb() is not implemented",
		"stub.routing.test_no_routing_v*.NoRoutingV*.test_should_check_multi_db_support":                                                           "Won't fix - driver.SupportsMultiDb() is not implemented",
		"stub.routing.test_routing_v3.RoutingV3.test_should_fail_discovery_when_router_fails_with_procedure_not_found_code":                        "Won't fix - only Bolt 3 affected (not officially supported by this driver) + this is only a difference in how errors are surfaced",
		"stub.routing.test_routing_v3.RoutingV3.test_should_fail_when_writing_on_unexpectedly_interrupting_writer_on_pull_using_tx_run":            "Won't fix - only Bolt 3 affected (not officially supported by this driver): broken servers are not removed from routing table",
		"stub.routing.test_routing_v3.RoutingV3.test_should_fail_when_writing_on_unexpectedly_interrupting_writer_on_run_using_tx_run":             "Won't fix - only Bolt 3 affected (not officially supported by this driver): broken servers are not removed from routing table",
		"stub.routing.test_routing_v3.RoutingV3.test_should_fail_when_writing_on_unexpectedly_interrupting_writer_using_tx_run":                    "Won't fix - only Bolt 3 affected (not officially supported by this driver): broken servers are not removed from routing table",

		// Missing message support in testkit backend
		"stub.routing.*.*.test_should_request_rt_from_all_initial_routers_until_successful_on_unknown_failure":       "Add DNS resolver TestKit message and connection timeout support",
		"stub.routing.*.*.test_should_request_rt_from_all_initial_routers_until_successful_on_authorization_expired": "Add DNS resolver TestKit message and connection timeout support",

		// To fix/to decide whether to fix
		"stub.routing.test_routing_v*.RoutingV*.test_should_revert_to_initial_router_if_known_router_throws_protocol_errors": "Driver always uses configured URL first and custom resolver only if that fails",
		"stub.routing.test_routing_v*.RoutingV*.test_should_read_successfully_from_reachable_db_after_trying_unreachable_db": "Driver retries to fetch a routing table up to 100 times if it's empty",
		"stub.routing.test_routing_v*.RoutingV*.test_should_write_successfully_after_leader_switch_using_tx_run":             "Driver retries to fetch a routing table up to 100 times if it's empty",
		"stub.routing.test_routing_v*.RoutingV*.test_should_fail_when_writing_without_writers_using_session_run":             "Driver retries to fetch a routing table up to 100 times if it's empty",
		"stub.routing.test_routing_v*.RoutingV*.test_should_accept_routing_table_without_writers_and_then_rediscover":        "Driver retries to fetch a routing table up to 100 times if it's empty",
		"stub.routing.test_routing_v*.RoutingV*.test_should_fail_on_routing_table_with_no_reader":                            "Driver retries to fetch a routing table up to 100 times if it's empty",
		"stub.routing.test_routing_v*.RoutingV*.test_should_fail_discovery_when_router_fails_with_unknown_code":              "Unify: other drivers have a list of fast failing errors during discover: on anything else, the driver will try the next router",
		"stub.routing.test_routing_v*.RoutingV*.test_should_drop_connections_failing_liveness_check":                         "Liveness check error handling is not (yet) unified: https://github.com/neo-technology/drivers-adr/pull/83",
		"stub.*.test_0_timeout": "Fixme: driver omits 0 as tx timeout value",
		"stub.summary.test_summary.TestSummaryBasicInfo.test_server_info": "pending unification: should the server address be pre or post DNS resolution?",
	}
}

func mustSkipTimeZoneSubTest(arguments map[string]any) (string, bool) {
	rawDateTime := arguments["dt"].(map[string]any)
	dateTimeData := rawDateTime["data"].(map[string]any)
	timeZoneName := dateTimeData["timezone_id"].(string)
	location, err := time.LoadLocation(timeZoneName)
	if err != nil {
		return fmt.Sprintf("time zone not supported: %s", err), true
	}
	dateTime := time.Date(
		asInt(dateTimeData["year"].(json.Number)),
		time.Month(asInt(dateTimeData["month"].(json.Number))),
		asInt(dateTimeData["day"].(json.Number)),
		asInt(dateTimeData["hour"].(json.Number)),
		asInt(dateTimeData["minute"].(json.Number)),
		asInt(dateTimeData["second"].(json.Number)),
		asInt(dateTimeData["nanosecond"].(json.Number)),
		location,
	)
	expectedOffset := asInt(dateTimeData["utc_offset_s"].(json.Number))
	if _, actualOffset := dateTime.Zone(); actualOffset != expectedOffset {
		return fmt.Sprintf("Expected offset %d for timezone %s and time %s, got offset %d instead",
				expectedOffset, timeZoneName, dateTime.String(), actualOffset),
			true
	}
	return "", false
}

// some TestKit tests send large integer values which require to configure
// the JSON deserializer to use json.Number instead of float64 (lossy conversions
// would happen otherwise)
// however, some specific dictionaries (like transaction metadata and custom
// auth parameters) are better off relying on numbers being treated as float64
func patchNumbersInMap(dictionary map[string]any) error {
	for key, value := range dictionary {
		if number, ok := value.(json.Number); ok {
			floatingPointValue, err := number.Float64()
			if err != nil {
				return fmt.Errorf("could not deserialize number %v in map %v: %w", number, dictionary, err)
			}
			dictionary[key] = floatingPointValue
		}
	}
	return nil
}

func (b *backend) bookmarkManagerConfig(bookmarkManagerId string,
	config map[string]any) neo4j.BookmarkManagerConfig {

	var initialBookmarks neo4j.Bookmarks
	if config["initialBookmarks"] != nil {
		initialBookmarks = convertInitialBookmarks(config["initialBookmarks"].([]any))
	}
	result := neo4j.BookmarkManagerConfig{InitialBookmarks: initialBookmarks}
	supplierRegistered := config["bookmarksSupplierRegistered"]
	if supplierRegistered != nil && supplierRegistered.(bool) {
		result.BookmarkSupplier = b.supplyBookmarks(bookmarkManagerId)
	}
	consumerRegistered := config["bookmarksConsumerRegistered"]
	if consumerRegistered != nil && consumerRegistered.(bool) {
		result.BookmarkConsumer = b.consumeBookmarks(bookmarkManagerId)
	}
	return result
}

func (b *backend) supplyBookmarks(bookmarkManagerId string) func(context.Context) (neo4j.Bookmarks, error) {
	return func(ctx context.Context) (neo4j.Bookmarks, error) {
		id := b.nextId()
		msg := map[string]any{"id": id, "bookmarkManagerId": bookmarkManagerId}
		b.writeResponse("BookmarksSupplierRequest", msg)
		b.process()
		return b.suppliedBookmarks[id], nil
	}
}

func (b *backend) consumeBookmarks(bookmarkManagerId string) func(context.Context, neo4j.Bookmarks) error {
	return func(_ context.Context, bookmarks neo4j.Bookmarks) error {
		id := b.nextId()
		b.writeResponse("BookmarksConsumerRequest", map[string]any{
			"id":                id,
			"bookmarkManagerId": bookmarkManagerId,
			"bookmarks":         bookmarks,
		})
		for {
			b.process()
			if _, found := b.consumedBookmarks[id]; found {
				delete(b.consumedBookmarks, id)
				return nil
			}
		}
	}
}

func convertInitialBookmarks(bookmarks []any) neo4j.Bookmarks {
	result := make(neo4j.Bookmarks, len(bookmarks))
	for i, bookmark := range bookmarks {
		result[i] = bookmark.(string)
	}
	return result
}

func anyToNotificationCategory(v any) notifications.NotificationCategory {
	return notifications.NotificationCategory(v.(string))
}

func convertSlice[T any](slice []any, transform func(any) T) []T {
	res := make([]T, len(slice))
	for i, cat := range slice {
		res[i] = transform(cat)
	}
	return res
}

func mapNotificationMinSeverityLevel(rawMinSeverityLevel string) (notifications.NotificationMinimumSeverityLevel, error) {
	switch rawMinSeverityLevel {
	case "OFF":
		return notifications.DisabledLevel, nil
	case "WARNING":
		return notifications.WarningLevel, nil
	case "INFORMATION":
		return notifications.InformationLevel, nil
	}
	return "", fmt.Errorf("unknown min severity level %s", rawMinSeverityLevel)
}
