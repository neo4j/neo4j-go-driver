/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package control

import (
	"bufio"
	"fmt"
	"math/rand"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/neo4j/neo4j-go-driver/neo4j"
)

// ClusterMemberRole is the type of the server that's part of the causal cluster
type ClusterMemberRole string

const (
	// Leader role
	Leader ClusterMemberRole = "LEADER"
	// Follower role
	Follower ClusterMemberRole = "FOLLOWER"
	// ReadReplica role
	ReadReplica ClusterMemberRole = "READ_REPLICA"
)

type configFunc func(config *neo4j.Config)

// ClusterMember holds information about a single server in the cluster
type ClusterMember struct {
	path            string
	hostnameAndPort string
	boltURI         string
	routingURI      string
}

// Cluster holds information about the cluster
type Cluster struct {
	path           string
	username       string
	password       string
	authToken      neo4j.AuthToken
	config         configFunc
	members        []*ClusterMember
	offlineMembers []*ClusterMember
	drivers        clusterDrivers
}

type clusterDrivers struct {
	driverMap sync.Map
	authToken neo4j.AuthToken
	config    configFunc
}

var cluster *Cluster
var clusterErr error
var clusterLock sync.Mutex

// EnsureCluster either returns an existing cluster instance or starts up a new one
func EnsureCluster() (*Cluster, error) {
	if cluster == nil && clusterErr == nil {
		clusterLock.Lock()
		defer clusterLock.Unlock()
		if cluster == nil {
			cluster, clusterErr = newCluster(resolveServerPath(true))
		}
	}

	return cluster, clusterErr
}

// StopCluster stops the cluster
func StopCluster() {
	if cluster != nil {
		clusterLock.Lock()
		defer clusterLock.Unlock()

		if cluster != nil {
			stopCluster(cluster.path)
		}
	}
}

func newCluster(path string) (*Cluster, error) {
	var output string
	var err error
	var clusterMember *ClusterMember
	var clusterMembers []*ClusterMember

	if !isClusterInstalled(path) {
		// there isn't any cluster installation
		if err = installCluster(versionToTestAgainst(), clusterCoreCount, clusterReadReplicaCount, password, clusterPort, path); err != nil {
			return nil, err
		}
	}

	if output, err = startCluster(path); err != nil {
		return nil, err
	}

	scanner := bufio.NewScanner(strings.NewReader(output))
	for scanner.Scan() {
		paramsScanner := bufio.NewScanner(strings.NewReader(scanner.Text()))
		paramsScanner.Split(bufio.ScanWords)

		index := 0
		memberBoltURI := ""
		memberPath := ""
		for paramsScanner.Scan() {
			switch index {
			case 0:
				break
			case 1:
				memberBoltURI = paramsScanner.Text()
			case 2:
				memberPath = paramsScanner.Text()
			default:
				memberPath = memberPath + " " + paramsScanner.Text()
			}

			index++
		}
		if err = paramsScanner.Err(); err != nil {
			return nil, err
		}

		if clusterMember, err = newClusterMember(memberBoltURI, memberPath); err != nil {
			return nil, err
		}

		clusterMembers = append(clusterMembers, clusterMember)
	}
	if err = scanner.Err(); err != nil {
		return nil, err
	}

	authToken := neo4j.BasicAuth(username, password, "")
	config := func(config *neo4j.Config) {
		config.Log = neo4j.ConsoleLogger(logLevel())
	}

	result := &Cluster{
		path:           path,
		authToken:      authToken,
		username:       username,
		password:       password,
		config:         config,
		members:        clusterMembers,
		offlineMembers: nil,
		drivers: clusterDrivers{
			driverMap: sync.Map{},
			authToken: authToken,
			config:    config,
		},
	}

	if err = result.waitMembersToBeOnline(); err != nil {
		return result, err
	}

	if err = result.deleteData(); err != nil {
		return result, err
	}

	return result, nil
}

// Username returns the configured username
func (cluster *Cluster) Username() string {
	return cluster.username
}

// Password returns the configured password
func (cluster *Cluster) Password() string {
	return cluster.password
}

// AuthToken returns the configured authentication token
func (cluster *Cluster) AuthToken() neo4j.AuthToken {
	return cluster.authToken
}

// Config returns the configured configurer function
func (cluster *Cluster) Config() func(config *neo4j.Config) {
	return cluster.config
}

// Leader returns the current leader in the cluster
func (cluster *Cluster) Leader() *ClusterMember {
	var leaders, _ = cluster.membersWithRole(Leader)
	if len(leaders) > 0 {
		return leaders[0]
	}
	return nil
}

// LeaderAddress returns the current leader's address
func (cluster *Cluster) LeaderAddress() neo4j.ServerAddress {
	return &url.URL{Host: cluster.Leader().hostnameAndPort}
}

// Cores returns the current core members in the cluster
func (cluster *Cluster) Cores() []*ClusterMember {
	var cores []*ClusterMember

	cores = append(cores, cluster.Leader())
	cores = append(cores, cluster.Followers()...)

	return cores
}

// CoreAddresses returns the current core members' addresses
func (cluster *Cluster) CoreAddresses() []neo4j.ServerAddress {
	var urls []neo4j.ServerAddress

	for _, core := range cluster.Cores() {
		urls = append(urls, &url.URL{Host: core.hostnameAndPort})
	}

	return urls
}

// Followers returns the current follower members in the cluster
func (cluster *Cluster) Followers() []*ClusterMember {
	filtered, _ := cluster.membersWithRole(Follower)
	return filtered
}

// AnyFollower returns a follower from the list of current followers
func (cluster *Cluster) AnyFollower() *ClusterMember {
	followers := cluster.Followers()
	if len(followers) > 0 {
		followerSelected := rand.Intn(len(followers))
		return followers[followerSelected]
	}
	return nil
}

// ReadReplicas returns the current read replica members in the cluster
func (cluster *Cluster) ReadReplicas() []*ClusterMember {
	filtered, _ := cluster.membersWithRole(ReadReplica)
	return filtered
}

// ReadReplicaAddresses returns the current read replica members' addresses
func (cluster *Cluster) ReadReplicaAddresses() []neo4j.ServerAddress {
	var urls []neo4j.ServerAddress

	for _, replica := range cluster.ReadReplicas() {
		urls = append(urls, &url.URL{Host: replica.hostnameAndPort})
	}

	return urls
}

// AnyReadReplica returns a read replica from the list of current read replicas
func (cluster *Cluster) AnyReadReplica() *ClusterMember {
	readReplicas := cluster.ReadReplicas()
	if len(readReplicas) > 0 {
		readReplicaSelected := rand.Intn(len(readReplicas))
		return readReplicas[readReplicaSelected]
	}
	return nil
}

func (cluster *Cluster) memberWithAddress(address string) *ClusterMember {
	for _, member := range cluster.members {
		if member.BoltURI() == address {
			return member
		}
	}

	return nil
}

func (cluster *Cluster) deleteData() error {
	driver, err := cluster.driverToLeader()
	if err != nil {
		return err
	}

	return deleteData(driver)
}

func (cluster *Cluster) waitMembersToBeOnline() error {
	var err, lastErr error
	var allMembers map[string]bool
	var onlineMembers map[string]ClusterMemberRole

	lastErr = nil
	startTime := time.Now()
	for time.Since(startTime) < clusterStartupTimeout {
		allMembers = make(map[string]bool, len(cluster.members))
		for _, member := range cluster.members {
			allMembers[member.boltURI] = false
		}

		if onlineMembers, err = cluster.discoverOnlineMembers(); err != nil {
			lastErr = err
		} else {
			for memberBoltAddress := range onlineMembers {
				delete(allMembers, memberBoltAddress)
			}

			if len(allMembers) == 0 {
				return nil
			}
		}

		time.Sleep(clusterCheckInterval)
	}

	return fmt.Errorf("timed out waiting for the cluster members to be online: %v", lastErr)
}

func (cluster *Cluster) membersWithRole(role ClusterMemberRole) ([]*ClusterMember, error) {
	var err error
	var onlineMembers map[string]ClusterMemberRole

	if onlineMembers, err = cluster.discoverOnlineMembers(); err != nil {
		return nil, err
	}

	var discoveredMembers []*ClusterMember
	for memberBoltAddress, memberRole := range onlineMembers {
		if memberRole == role {
			member := cluster.memberWithAddress(memberBoltAddress)
			if member != nil {
				discoveredMembers = append(discoveredMembers, member)
			} else {
				return nil, fmt.Errorf("unable to locate member with address %s", memberBoltAddress)
			}
		}
	}
	return discoveredMembers, nil
}

func (cluster *Cluster) discoverOnlineMembers() (map[string]ClusterMemberRole, error) {
	var discoveredMembers = make(map[string]ClusterMemberRole, 0)

	driver, err := cluster.driverToAnyCore()
	if err != nil {
		return nil, err
	}

	session, err := driver.Session(neo4j.AccessModeRead)
	if err != nil {
		return nil, err
	}
	defer session.Close()

	result, err := session.Run("CALL dbms.cluster.overview()", nil)
	if err != nil {
		return nil, err
	}

	for result.Next() {
		memberRole, _ := result.Record().Get("role")
		memberAddresses, _ := result.Record().Get("addresses")
		memberBoltAddress := memberAddresses.([]interface{})[0].(string)

		discoveredMembers[memberBoltAddress] = ClusterMemberRole(memberRole.(string))
	}

	return discoveredMembers, nil
}

func (cluster *Cluster) driverToLeader() (neo4j.Driver, error) {
	return cluster.drivers.driverFor(cluster.Leader())
}

func (cluster *Cluster) driverToAnyCore() (neo4j.Driver, error) {
	if len(cluster.members) == 0 {
		return nil, errors.New("there are no members present in the cluster")
	}

	for _, member := range cluster.members {
		driver, err := cluster.drivers.driverFor(member)
		if err != nil {
			continue
		}

		session, err := driver.Session(neo4j.AccessModeRead)
		if err != nil {
			continue
		}

		result, err := session.Run("CALL dbms.cluster.role()", nil)
		if err != nil {
			continue
		}
		defer session.Close()

		if result.Next() {
			role, _ := result.Record().Get("role")
			if role.(string) != string(ReadReplica) {
				return driver, nil
			}
		}
	}

	return nil, errors.New("unable to get a driver to a core")
}

func (drivers *clusterDrivers) driverFor(member *ClusterMember) (neo4j.Driver, error) {
	if stored, ok := drivers.driverMap.Load(member.hostnameAndPort); ok {
		return stored.(neo4j.Driver), nil
	}

	driver, err := neo4j.NewDriver(member.BoltURI(), drivers.authToken, drivers.config)
	if err != nil {
		return nil, err
	}

	if stored, loaded := drivers.driverMap.LoadOrStore(member.hostnameAndPort, driver); loaded {
		driver.Close()

		return stored.(neo4j.Driver), nil
	}

	return driver, nil
}

// Path returns the folder where the member is installed
func (member *ClusterMember) Path() string {
	return member.path
}

// BoltURI returns the bolt:// uri used to connect to the member
func (member *ClusterMember) BoltURI() string {
	return member.boltURI
}

// RoutingURI returns the bolt+routing:// uri used to connect to the cluster with this member
// as the initial router
func (member *ClusterMember) RoutingURI() string {
	return member.routingURI
}

// Address returns hostname:port identifier for the member
func (member *ClusterMember) Address() neo4j.ServerAddress {
	var uri *url.URL
	var err error

	if uri, err = url.Parse(member.boltURI); err != nil {
		return nil
	}

	return uri
}

func (member *ClusterMember) String() string {
	return fmt.Sprintf("ClusterMember{boltURI=%s, boltAddress=%s, path=%s}", member.boltURI, member.hostnameAndPort, member.path)
}

func newClusterMember(uri string, path string) (*ClusterMember, error) {
	parsedURI, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}

	port := parsedURI.Port()
	if port == "" {
		port = "7687"
	}

	return &ClusterMember{
		path:            path,
		hostnameAndPort: fmt.Sprintf("%s:%s", parsedURI.Hostname(), port),
		boltURI:         fmt.Sprintf("bolt://%s:%s", parsedURI.Hostname(), port),
		routingURI:      fmt.Sprintf("bolt+routing://%s:%s", parsedURI.Hostname(), port),
	}, nil
}
