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

var testSkips map[string]string

func init() {
	// you can use '*' as wildcards anywhere in the qualified test name (useful to exclude a whole class e.g.)
	testSkips = map[string]string{
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

		// To fix/to decide whether to fix
		"stub.routing.*.*.test_should_successfully_acquire_rt_when_router_ip_changes":                                                      "Backend lacks custom DNS resolution and Go driver RT discovery differs.",
		"stub.routing.test_routing_v*.RoutingV*.test_should_revert_to_initial_router_if_known_router_throws_protocol_errors":               "Driver always uses configured URL first and custom resolver only if that fails",
		"stub.routing.test_routing_v*.RoutingV*.test_should_request_rt_from_all_initial_routers_until_successful_on_authorization_expired": "Driver always uses configured URL first and custom resolver only if that fails",
		"stub.routing.test_routing_v*.RoutingV*test_should_request_rt_from_all_initial_routers_until_successful_on_unknown_failure":        "Driver always uses configured URL first and custom resolver only if that fails",
		"stub.routing.test_routing_v*.RoutingV*.test_should_read_successfully_from_reachable_db_after_trying_unreachable_db":               "Driver retries to fetch a routing table up to 100 times if it's empty",
		"stub.routing.test_routing_v*.RoutingV*.test_should_write_successfully_after_leader_switch_using_tx_run":                           "Driver retries to fetch a routing table up to 100 times if it's empty",
		"stub.routing.test_routing_v*.RoutingV*.test_should_fail_when_writing_without_writers_using_session_run":                           "Driver retries to fetch a routing table up to 100 times if it's empty",
		"stub.routing.test_routing_v*.RoutingV*.test_should_accept_routing_table_without_writers_and_then_rediscover":                      "Driver retries to fetch a routing table up to 100 times if it's empty",
		"stub.routing.test_routing_v*.RoutingV*.test_should_fail_on_routing_table_with_no_reader":                                          "Driver retries to fetch a routing table up to 100 times if it's empty",
		"stub.routing.test_routing_v*.RoutingV*.test_should_fail_discovery_when_router_fails_with_unknown_code":                            "Unify: other drivers have a list of fast failing errors during discover: on anything else, the driver will try the next router",
		"stub.routing.test_routing_v*.RoutingV*.test_should_drop_connections_failing_liveness_check":                                       "Liveness check error handling is not (yet) unified: https://github.com/neo-technology/drivers-adr/pull/83",
		"stub.*.test_0_timeout": "Fixme: driver omits 0 as tx timeout value",
		"stub.summary.test_summary.TestSummaryBasicInfo*.test_server_info": "pending unification: should the server address be pre or post DNS resolution?",
	}
	for testPattern, reason := range extrasTestSkips {
		if _, ok := extrasTestSkips[testPattern]; ok {
			panic("Fixed test skip colliding with extra skip: '" + testPattern + "'")
		}
		testSkips[testPattern] = reason
	}
}

var features []string

func init() {
	allFeatures := []string{
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
		"Feature:API:Type.Vector",
		"Feature:API:Type.UnsupportedType",
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
		"Feature:Bolt:5.8",
		"Feature:Bolt:6.0",
		"Feature:Bolt:Patch:UTC",
		"Feature:Bolt:HandshakeManifestV1",
		"Feature:IdempotentRetries",
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
		"Optimization:HomeDatabaseCache",
		"Optimization:HomeDbCacheBasicPrincipalIsImpersonatedUser",
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
	}
	features = make([]string, 0, len(allFeatures))
	for _, feature := range allFeatures {
		if _, ok := extrasBlockedTestKitFeatures[feature]; !ok {
			features = append(features, feature)
		}
	}
	for blockedFeature := range extrasBlockedTestKitFeatures {
		if !contains(allFeatures, blockedFeature) {
			panic("Extra is trying to block an unsupported feature: '" + blockedFeature + "'")
		}
	}
}
