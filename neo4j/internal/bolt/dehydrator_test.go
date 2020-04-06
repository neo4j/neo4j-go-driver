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
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package bolt

import (
	"bytes"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/neo4j/internal/packstream"
	"github.com/neo4j/neo4j-go-driver/neo4j/internal/types"
)

func TestDehydrator(ot *testing.T) {
	assertNoErr := func(t *testing.T, err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}

	cases := []struct {
		name string
		x    interface{}
		s    *packstream.Struct
	}{
		{
			name: "Point2D",
			x:    &types.Point2D{SpatialRefId: 7, X: 8.0, Y: 9.0},
			s:    &packstream.Struct{Tag: 'X', Fields: []interface{}{uint32(7), 8.0, 9.0}},
		},
		{
			name: "Point3D",
			x:    &types.Point3D{SpatialRefId: 7, X: 8.0, Y: 9.0, Z: 10},
			s:    &packstream.Struct{Tag: 'Y', Fields: []interface{}{uint32(7), 8.0, 9.0, 10.0}},
		},
	}
	for _, c := range cases {
		ot.Run(c.name, func(t *testing.T) {
			s, err := dehydrate(c.x)
			assertNoErr(t, err)
			// Compare Structs
			if s.Tag != c.s.Tag {
				t.Errorf("Wrong tags, expected %d but was %d", c.s.Tag, s.Tag)
			}
			if len(c.s.Fields) != len(s.Fields) {
				t.Errorf("Wrong number of fields, expected %d but was %d", len(c.s.Fields), len(s.Fields))
			}
			for i, v := range c.s.Fields {
				if s.Fields[i] != v {
					t.Errorf("Field %d differs, expected %+v but was %+v", i, v, s.Fields[i])
				}
			}
		})
	}

	assertDateTimeSame := func(t *testing.T, t1, t2 time.Time) {
		t.Helper()
		if t1.UnixNano() != t2.UnixNano() {
			t.Errorf("Times differ")
		}
	}

	assertDateSame := func(t *testing.T, t1, t2 time.Time) {
		t.Helper()
		if t1.Year() != t2.Year() {
			t.Errorf("Wrong year")
		}
		if t1.Month() != t2.Month() {
			t.Errorf("Wrong Month")
		}
		if t1.Day() != t2.Day() {
			t.Errorf("Wrong Day")
		}
	}

	assertTimeSame := func(t *testing.T, t1, t2 time.Time) {
		t.Helper()
		if t1.Hour() != t2.Hour() {
			t.Errorf("Wrong Hour, %d vs %d", t1.Hour(), t2.Hour())
		}
		if t1.Minute() != t2.Minute() {
			t.Errorf("Wrong Minute")
		}
		if t1.Second() != t2.Second() {
			t.Errorf("Wrong Second")
		}
		if t1.Nanosecond() != t2.Nanosecond() {
			t.Errorf("Wrong.Nanosecond")
		}
	}

	assertTimeLocationSame := func(t *testing.T, t1, t2 time.Time) {
		t.Helper()
		if t1.Location().String() != t2.Location().String() {
			t.Errorf("Locations differ, %s vs %s", t1.Location(), t2.Location())
		}
	}

	assertOffsetSame := func(t *testing.T, t1, t2 time.Time) {
		t.Helper()
		_, off1 := t1.Zone()
		_, off2 := t2.Zone()
		if off1 != off2 {
			t.Errorf("Offsets differ, %d vs %d", off1, off2)
		}
	}

	assertDurationSame := func(t *testing.T, d1, d2 types.Duration) {
		t.Helper()
		if d1.Months != d2.Months {
			t.Errorf("Months differ, %d vs %d", d1.Months, d2.Months)
		}
		if d1.Days != d2.Days {
			t.Errorf("Days differ, %d vs %d", d1.Days, d2.Days)
		}
		if d1.Seconds != d2.Seconds {
			t.Errorf("Seconds differ, %d vs %d", d1.Seconds, d2.Seconds)
		}
		if d1.Nanos != d2.Nanos {
			t.Errorf("Nanos differ, %d vs %d", d1.Nanos, d2.Nanos)
		}
	}

	dehydrateAndHydrate := func(t *testing.T, xi interface{}) interface{} {
		t.Helper()
		// Run through packstream to be certain of type assumptions
		buf := bytes.NewBuffer([]byte{})
		packer := packstream.NewPacker(buf, dehydrate)
		err := packer.Pack(xi)
		assertNoErr(t, err)
		unpacker := packstream.NewUnpacker(buf)
		xo, err := unpacker.UnpackStruct(hydrate)
		assertNoErr(t, err)
		return xo
	}

	ot.Run("time.Time", func(t *testing.T) {
		ni := time.Now()
		l, _ := time.LoadLocation("America/New_York")
		ni = ni.In(l)
		no := dehydrateAndHydrate(t, ni).(time.Time)
		assertDateTimeSame(t, ni, no)
		assertTimeLocationSame(t, ni, no)
	})

	ot.Run("time.Time offset", func(t *testing.T) {
		ni := time.Now()
		l := time.FixedZone("Offset", 60*60)
		ni = ni.In(l)
		no := dehydrateAndHydrate(t, ni).(time.Time)
		assertDateTimeSame(t, ni, no)
		assertTimeLocationSame(t, ni, no)
	})

	ot.Run("LocalDateTime", func(t *testing.T) {
		ni := time.Now()
		l, _ := time.LoadLocation("America/New_York")
		ni = ni.In(l)
		no := dehydrateAndHydrate(t, types.LocalDateTime(ni)).(types.LocalDateTime)
		assertDateTimeSame(t, ni, time.Time(no))
		// Received time should be in Local time even if sent as something else
		if time.Time(no).Location().String() != "Local" {
			t.Errorf("Should be local")
		}
	})

	ot.Run("Date", func(t *testing.T) {
		ni := time.Now()
		l, _ := time.LoadLocation("America/New_York")
		ni = ni.In(l)
		no := dehydrateAndHydrate(t, types.Date(ni)).(types.Date)
		assertDateSame(t, ni, time.Time(no))
	})

	ot.Run("Time", func(t *testing.T) {
		ni := time.Now()
		l, _ := time.LoadLocation("America/New_York")
		ni = ni.In(l)
		no := dehydrateAndHydrate(t, types.Time(ni)).(types.Time)
		assertOffsetSame(t, ni, time.Time(no))
		assertTimeSame(t, ni, time.Time(no))
	})

	ot.Run("LocalTime", func(t *testing.T) {
		ni := time.Now()
		l, _ := time.LoadLocation("America/New_York")
		ni = ni.In(l)
		no := dehydrateAndHydrate(t, types.LocalTime(ni)).(types.LocalTime)
		assertTimeSame(t, ni, time.Time(no))
	})

	ot.Run("time.Duration", func(t *testing.T) {
		di := 7*time.Minute + 28*time.Hour
		do := dehydrateAndHydrate(t, di).(types.Duration)
		assertDurationSame(t, types.Duration{Days: 1, Seconds: (4 * 60 * 60) + 7*60}, do)
	})

	ot.Run("Duration", func(t *testing.T) {
		di := types.Duration{Months: 3, Days: 3, Seconds: 9000, Nanos: 13}
		do := dehydrateAndHydrate(t, di).(types.Duration)
		assertDurationSame(t, di, do)
	})
}
