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

package neo4j

import (
	"math"
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	config := defaultConfig()

	if config.MaxTransactionRetryTime != 30*time.Second {
		t.Errorf("should have max transaction retry duration set to 30 sconds by default")
	}

	if config.MaxConnectionPoolSize != 100 {
		t.Errorf("should have max connection pool size set to 100 by default")
	}

	if config.MaxConnectionLifetime != 1*time.Hour {
		t.Errorf("should have max connection lifetime set to 1 hour by default")
	}

	if config.ConnectionAcquisitionTimeout != 1*time.Minute {
		t.Errorf("should have connection acquisition timeout set to 1 minute by default")
	}

	if config.SocketConnectTimeout != 5*time.Second {
		t.Errorf("should have socket connect timeout set to 5 seconds by default")
	}

	if config.SocketKeepalive != true {
		t.Errorf("should have socket keep alive enabled by default")
	}
}

func TestValidateAndNormaliseConfig(rt *testing.T) {

	rt.Run("MaxTransactionRetryTime less than zero", func(t *testing.T) {
		config := defaultConfig()

		config.MaxTransactionRetryTime = -1 * time.Second
		err := validateAndNormaliseConfig(config)
		if err == nil {
			t.Errorf("MaxTransactionRetryTime is less than 0 but never returned an error")
		}
	})

	rt.Run("MaxConnectionPoolSize equals zero", func(t *testing.T) {
		config := defaultConfig()

		config.MaxConnectionPoolSize = 0
		err := validateAndNormaliseConfig(config)
		if err == nil {
			t.Errorf("MaxConnectionPoolSize is 0 but never returned an error")
		}
	})

	rt.Run("MaxConnectionPoolSize less than zero", func(t *testing.T) {
		config := defaultConfig()

		config.MaxConnectionPoolSize = -1
		err := validateAndNormaliseConfig(config)
		if err != nil {
			t.Errorf("MaxConnectionPoolSize is negative but returned an error")
		}
		if config.MaxConnectionPoolSize != math.MaxInt32 {
			t.Errorf("MaxConnectionPoolSize should be set to math.MaxInt32 when negative")
		}
	})

	rt.Run("ConnectionAcquisitionTimeout less than zero", func(t *testing.T) {
		config := defaultConfig()

		config.ConnectionAcquisitionTimeout = -1 * time.Second
		err := validateAndNormaliseConfig(config)
		if err != nil {
			t.Errorf("ConnectionAcquisitionTimeout is negative but returned an error")
		}
		if config.ConnectionAcquisitionTimeout != -1*time.Nanosecond {
			t.Errorf("MaxConnectionPoolSize should be set to (-1 * time.Nanosecond) when negative")
		}
	})

	rt.Run("SocketConnectTimeout less than zero", func(t *testing.T) {
		config := defaultConfig()

		config.SocketConnectTimeout = -1 * time.Second
		err := validateAndNormaliseConfig(config)
		if err != nil {
			t.Errorf("SocketConnectTimeout is negative but returned an error")
		}
		if config.SocketConnectTimeout != 0*time.Nanosecond {
			t.Errorf("SocketConnectTimeout should be set to (0 * time.Nanosecond) when negative")
		}
	})
}
