/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/neo4j/neo4j-go-driver"
)

type ClusterMemberRole string

const (
	LEADER       ClusterMemberRole = "LEADER"
	FOLLOWER     ClusterMemberRole = "FOLLOWER"
	READ_REPLICA ClusterMemberRole = "READ_REPLICA"
)

type configFunc func(config *neo4j.Config)

type ClusterMember struct {
	path            string
	hostnameAndPort string
	boltUri         string
	routingUri      string
}

type Cluster struct {
	path           string
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

const (
	clusterStartupTimeout = 120 * time.Second
	clusterCheckInterval  = 500 * time.Millisecond
)

var lock sync.Mutex
var cluster *Cluster
var clusterErr error

func EnsureCluster() (*Cluster, error) {
	if cluster == nil && clusterErr == nil {
		lock.Lock()
		defer lock.Unlock()
		if cluster == nil {
			var clusterPath string = os.TempDir()

			if _, file, _, ok := runtime.Caller(1); ok {
				clusterPath = path.Join(path.Dir(file), "build")
			}

			cluster, clusterErr = newCluster(clusterPath)
		}
	}

	return cluster, clusterErr
}

func StopCluster() {
	if cluster != nil {
		lock.Lock()
		defer lock.Unlock()

		stopCluster(cluster.path)
	}
}

func newCluster(path string) (*Cluster, error) {
	var output string
	var err error
	var clusterMember *ClusterMember
	var clusterMembers []*ClusterMember

	if _, err = os.Stat(path); os.IsNotExist(err) {
		// there isn't any cluster installation
		if err = installCluster(neo4jVersionToTestAgainst(), 3, 2, "password", 20000, path); err != nil {
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
		memberBoltUri := ""
		memberPath := ""
		for paramsScanner.Scan() {
			switch index {
			case 0:
				break
			case 1:
				memberBoltUri = paramsScanner.Text()
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

		if clusterMember, err = newClusterMember(memberBoltUri, memberPath); err != nil {
			return nil, err
		}

		clusterMembers = append(clusterMembers, clusterMember)
	}
	if err = scanner.Err(); err != nil {
		return nil, err
	}

	authToken := neo4j.BasicAuth("neo4j", "password", "")
	config := func(config *neo4j.Config) {

	}

	result := &Cluster{
		path:           path,
		authToken:      authToken,
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

func (cluster *Cluster) AuthToken() neo4j.AuthToken {
	return cluster.authToken
}

func (cluster *Cluster) Config() func(config *neo4j.Config) {
	return func(config *neo4j.Config) {
		config.Log = neo4j.ConsoleLogger(neo4j.DEBUG)
	}
}

func (cluster *Cluster) Leader() *ClusterMember {
	var leaders, _ = cluster.membersWithRole(LEADER)
	if len(leaders) > 0 {
		return leaders[0]
	}
	return nil
}

func (cluster *Cluster) Cores() []*ClusterMember {
	var cores []*ClusterMember

	cores = append(cores, cluster.Leader())
	cores = append(cores, cluster.Followers()...)

	return cores
}

func (cluster *Cluster) Followers() []*ClusterMember {
	filtered, _ := cluster.membersWithRole(FOLLOWER)
	return filtered
}

func (cluster *Cluster) AnyFollower() *ClusterMember {
	followers := cluster.Followers()
	if len(followers) > 0 {
		followerSelected := rand.Intn(len(followers))
		return followers[followerSelected]
	}
	return nil
}

func (cluster *Cluster) ReadReplicas() []*ClusterMember {
	filtered, _ := cluster.membersWithRole(READ_REPLICA)
	return filtered
}

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
		if member.BoltUri() == address {
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

	session, err := driver.Session(neo4j.AccessModeWrite)
	if err != nil {
		return err
	}
	defer session.Close()

	for {
		result, err := session.Run("MATCH (n) WITH n LIMIT 10000 DETACH DELETE n RETURN count(n)", nil)
		if err != nil {
			return err
		}

		if result.Next() {
			deleted := result.Record().GetByIndex(0).(int64)
			if deleted == 0 {
				break
			}
		}

		if err := result.Err(); err != nil {
			return err
		}
	}

	return nil
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
			allMembers[member.boltUri] = false
		}

		if onlineMembers, err = cluster.discoverOnlineMembers(); err != nil {
			lastErr = err
		} else {
			for memberBoltAddress, _ := range onlineMembers {
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
		return nil, fmt.Errorf("there are no members present in the cluster")
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
			if role.(string) != string(READ_REPLICA) {
				return driver, nil
			}
		}
	}

	return nil, fmt.Errorf("unable to get a driver to a core")
}

func (drivers *clusterDrivers) driverFor(member *ClusterMember) (neo4j.Driver, error) {
	if stored, ok := drivers.driverMap.Load(member.hostnameAndPort); ok {
		return stored.(neo4j.Driver), nil
	}

	driver, err := neo4j.NewDriver(member.BoltUri(), drivers.authToken, drivers.config)
	if err != nil {
		return nil, err
	}

	if stored, loaded := drivers.driverMap.LoadOrStore(member.hostnameAndPort, driver); loaded {
		driver.Close()

		return stored.(neo4j.Driver), nil
	}

	return driver, nil
}

func (member *ClusterMember) Path() string {
	return member.path
}

func (member *ClusterMember) BoltUri() string {
	return member.boltUri
}

func (member *ClusterMember) RoutingUri() string {
	return member.routingUri
}

func (member *ClusterMember) String() string {
	return fmt.Sprintf("ClusterMember{boltUri=%s, boltAddress=%s, path=%s}", member.boltUri, member.hostnameAndPort, member.path)
}

func newClusterMember(uri string, path string) (*ClusterMember, error) {
	parsedUri, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}

	port := parsedUri.Port()
	if port == "" {
		port = "7687"
	}

	return &ClusterMember{
		path:            path,
		hostnameAndPort: fmt.Sprintf("%s:%s", parsedUri.Hostname(), port),
		boltUri:         fmt.Sprintf("bolt://%s:%s", parsedUri.Hostname(), port),
		routingUri:      fmt.Sprintf("bolt+routing://%s:%s", parsedUri.Hostname(), port),
	}, nil
}
