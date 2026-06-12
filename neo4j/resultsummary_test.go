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

package neo4j

import (
	"reflect"
	"testing"

	idb "github.com/neo4j/neo4j-go-driver/v6/neo4j/internal/db"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j/internal/testutil"
	"github.com/neo4j/neo4j-go-driver/v6/neo4j/internal/util"
)

func TestQueryProfile(st *testing.T) {
	st.Parallel()

	leaf1 := idb.Profile{Operator: "bar",
		DbHits:            util.Ptr(int64(1)),
		Rows:              util.Ptr(int64(2)),
		PageCacheMisses:   util.Ptr(int64(3)),
		PageCacheHits:     util.Ptr(int64(4)),
		PageCacheHitRatio: util.Ptr(1.2),
		Time:              util.Ptr(int64(5)),
	}
	leaf2 := idb.Profile{Operator: "fighters"}
	root := idb.Profile{Operator: "foo", Children: []idb.Profile{leaf1, leaf2}}

	st.Run("ProfiledPlan", func(st *testing.T) {
		st.Parallel()
		root := &profiledPlan{profile: &root}

		st.Run("Child plans are correctly populated", func(t *testing.T) {
			t.Parallel()

			expected := []ProfiledPlan{
				&profiledPlan{profile: &leaf1},
				&profiledPlan{profile: &leaf2},
			}

			children := root.Children()

			testutil.AssertDeepEquals(t, children, expected)
		})

		st.Run("Test DbHits", func(t *testing.T) {
			t.Parallel()
			testutil.AssertIntEqual(t, root.Children()[0].DbHits(), 1)
			testutil.AssertIntEqual(t, root.Children()[1].DbHits(), 0)
		})
		st.Run("Test Records", func(t *testing.T) {
			t.Parallel()
			testutil.AssertIntEqual(t, root.Children()[0].Records(), 2)
			testutil.AssertIntEqual(t, root.Children()[1].Records(), 0)
		})
		st.Run("Test PageCacheMisses", func(t *testing.T) {
			t.Parallel()
			testutil.AssertIntEqual(t, root.Children()[0].PageCacheMisses(), 3)
			testutil.AssertIntEqual(t, root.Children()[1].PageCacheMisses(), 0)
		})
		st.Run("Test PageCacheHits", func(t *testing.T) {
			t.Parallel()
			testutil.AssertIntEqual(t, root.Children()[0].PageCacheHits(), 4)
			testutil.AssertIntEqual(t, root.Children()[1].PageCacheHits(), 0)
		})
		st.Run("Test PageCacheHitRatio", func(t *testing.T) {
			t.Parallel()
			testutil.AssertFloatEqual(t, root.Children()[0].PageCacheHitRatio(), 1.2)
			testutil.AssertFloatEqual(t, root.Children()[1].PageCacheHitRatio(), 0.0)
		})
		st.Run("Test Time", func(t *testing.T) {
			t.Parallel()
			testutil.AssertIntEqual(t, root.Children()[0].Time(), 5)
			testutil.AssertIntEqual(t, root.Children()[1].Time(), 0)
		})
	})

	st.Run("QueryProfile", func(st *testing.T) {
		st.Parallel()
		root := &profile{profile: &root}

		st.Run("Child plans are correctly populated", func(t *testing.T) {
			t.Parallel()

			expected := []QueryProfile{
				&profile{profile: &leaf1},
				&profile{profile: &leaf2},
			}

			children := root.Children()

			testutil.AssertDeepEquals(t, children, expected)
		})

		st.Run("Test DbHits", func(t *testing.T) {
			t.Parallel()
			dbHits, ok := root.Children()[0].DbHits()
			testutil.AssertTrue(t, ok)
			testutil.AssertIntEqual(t, dbHits, 1)
			_, ok = root.Children()[1].DbHits()
			testutil.AssertFalse(t, ok)
		})
		st.Run("Test Records", func(t *testing.T) {
			t.Parallel()
			rows, ok := root.Children()[0].Rows()
			testutil.AssertTrue(t, ok)
			testutil.AssertIntEqual(t, rows, 2)
			_, ok = root.Children()[1].Rows()
			testutil.AssertFalse(t, ok)
		})
		st.Run("Test PageCacheMisses", func(t *testing.T) {
			t.Parallel()
			pageCacheMisses, ok := root.Children()[0].PageCacheMisses()
			testutil.AssertTrue(t, ok)
			testutil.AssertIntEqual(t, pageCacheMisses, 3)
			_, ok = root.Children()[1].PageCacheMisses()
			testutil.AssertFalse(t, ok)
		})
		st.Run("Test PageCacheHits", func(t *testing.T) {
			t.Parallel()
			pageCacheHits, ok := root.Children()[0].PageCacheHits()
			testutil.AssertTrue(t, ok)
			testutil.AssertIntEqual(t, pageCacheHits, 4)
			_, ok = root.Children()[1].PageCacheHits()
			testutil.AssertFalse(t, ok)
		})
		st.Run("Test PageCacheHitRatio", func(t *testing.T) {
			t.Parallel()
			pageCacheHitRatio, ok := root.Children()[0].PageCacheHitRatio()
			testutil.AssertTrue(t, ok)
			testutil.AssertFloatEqual(t, pageCacheHitRatio, 1.2)
			_, ok = root.Children()[1].PageCacheHitRatio()
			testutil.AssertFalse(t, ok)
		})
		st.Run("Test Time", func(t *testing.T) {
			t.Parallel()
			time, ok := root.Children()[0].Time()
			testutil.AssertTrue(t, ok)
			testutil.AssertIntEqual(t, time, 5)
			_, ok = root.Children()[1].Time()
			testutil.AssertFalse(t, ok)
		})
	})

}

func TestNotifications(st *testing.T) {
	st.Parallel()

	pos1 := idb.InputPosition{
		Offset: 1,
		Line:   2,
		Column: 3,
	}
	notif1 := idb.Notification{
		Code:        "code1",
		Title:       "title1",
		Description: "desc1",
		Severity:    "sev1",
		Position:    &pos1,
	}
	notif2 := idb.Notification{
		Code:        "code2",
		Title:       "title2",
		Description: "desc2",
		Severity:    "sev2",
		Position:    nil,
	}

	summary := resultSummary{
		sum: &idb.Summary{
			Notifications: []idb.Notification{notif1, notif2},
		},
	}

	st.Run("Notifications are returned correctly", func(t *testing.T) {
		t.Parallel()

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

func TestCounters(st *testing.T) {
	st.Parallel()

	emptySummary := resultSummary{sum: &idb.Summary{}}
	summary := resultSummary{
		sum: &idb.Summary{
			Counters: map[string]int{
				"system-updates": 42,
			},
		},
	}

	st.Run("Returns empty system update count by default", func(t *testing.T) {
		t.Parallel()

		actual := emptySummary.Counters().SystemUpdates()
		if actual != 0 {
			t.Errorf("Expected 0 system update, got %d", actual)
		}
	})

	st.Run("Returns populated system update count", func(t *testing.T) {
		t.Parallel()

		actual := summary.Counters().SystemUpdates()
		if actual != 42 {
			t.Errorf("Expected 42 system updates, got %d", actual)
		}
	})
}
