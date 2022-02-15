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

package neo4j

import (
	"github.com/neo4j/neo4j-go-driver/v4/neo4j/db"
	"reflect"
	"testing"
)

func TestProfiledPlan(st *testing.T) {
	leaf1 := db.ProfiledPlan{Operator: "bar"}
	leaf2 := db.ProfiledPlan{Operator: "fighters"}
	root := &profile{profile: &db.ProfiledPlan{Operator: "foo", Children: []db.ProfiledPlan{leaf1, leaf2}}}

	st.Run("Child plans are correctly populated", func(t *testing.T) {
		expected := []ProfiledPlan{
			&profile{profile: &leaf1},
			&profile{profile: &leaf2},
		}

		children := root.Children()

		if !reflect.DeepEqual(children, expected) {
			t.Errorf("Expected %v to equal %v", children, expected)
		}
	})
}

func TestCounters(st *testing.T) {

	emptySummary := resultSummary{sum: &db.Summary{}}
	summary := resultSummary{
		sum: &db.Summary{
			Counters: map[string]int{
				"system-updates": 42,
			},
		},
	}

	st.Run("Returns empty system update count by default", func(t *testing.T) {
		actual := emptySummary.Counters().SystemUpdates()
		if actual != 0 {
			t.Errorf("Expected 0 system update, got %d", actual)
		}
	})

	st.Run("Returns populated system update count", func(t *testing.T) {
		actual := summary.Counters().SystemUpdates()
		if actual != 42 {
			t.Errorf("Expected 42 system updates, got %d", actual)
		}
	})
}

func TestNotifications(st *testing.T) {
	pos1 := db.InputPosition{
		Offset: 1,
		Line:   2,
		Column: 3,
	}
	notif1 := db.Notification{
		Code:        "code1",
		Title:       "title1",
		Description: "desc1",
		Severity:    "sev1",
		Position:    &pos1,
	}
	notif2 := db.Notification{
		Code:        "code2",
		Title:       "title2",
		Description: "desc2",
		Severity:    "sev2",
		Position:    nil,
	}

	summary := resultSummary{
		sum: &db.Summary{
			Notifications: []db.Notification{notif1, notif2},
		},
	}

	st.Run("Notifications are returned correctly", func(t *testing.T) {
		expected := []Notification{
			&notification{notification: &notif1},
			&notification{notification: &notif2},
		}
		received := summary.Notifications()
		if !reflect.DeepEqual(received, expected) {
			t.Errorf("Expected %v to equal %v", received, expected)
		}
	})
}
