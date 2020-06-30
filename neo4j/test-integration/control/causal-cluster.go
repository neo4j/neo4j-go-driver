/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
	"net/url"
	"strings"
	"sync"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

type configFunc func(config *neo4j.Config)

// ClusterMember holds information about a single server in the cluster
type ClusterMember struct {
	HostnameAndPort string
	RoutingURI      string
}

// Cluster holds information about the cluster
type Cluster struct {
	path       string
	username   string
	password   string
	authToken  neo4j.AuthToken
	config     configFunc
	Members    []*ClusterMember
	RoutingURI string
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
		config.Encrypted = useEncryption()
		config.Log = neo4j.ConsoleLogger(logLevel())
		config.AddressResolver = func(address neo4j.ServerAddress) []neo4j.ServerAddress {
			servers := make([]neo4j.ServerAddress, len(cluster.Members))
			for i, m := range cluster.Members {
				servers[i] = &url.URL{Host: m.HostnameAndPort}
			}
			return servers
		}
	}

	result := &Cluster{
		path:       path,
		authToken:  authToken,
		username:   username,
		password:   password,
		config:     config,
		Members:    clusterMembers,
		RoutingURI: clusterMembers[0].RoutingURI,
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

func (cluster *Cluster) deleteData() error {
	driver, err := neo4j.NewDriver(cluster.RoutingURI, cluster.AuthToken(), cluster.Config())
	if err != nil {
		return err
	}

	return deleteData(driver)
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
		HostnameAndPort: fmt.Sprintf("%s:%s", parsedURI.Hostname(), port),
		RoutingURI:      fmt.Sprintf("bolt+routing://%s:%s", parsedURI.Hostname(), port),
	}, nil
}
