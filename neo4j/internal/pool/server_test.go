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

package pool

import (
	"testing"
)

func TestServer(ot *testing.T) {
	ot.Run("reg/unreg/size", func(t *testing.T) {
		s := &server{}
		if s.size != 0 {
			t.Error("not 0")
		}

		// Register should increase size
		c1 := &fakeConn{}
		s.reg(c1)
		if s.size != 1 {
			t.Error("not 1")
		}
		c2 := &fakeConn{}
		s.reg(c2)
		if s.size != 2 {
			t.Error("not 2")
		}

		// Unregister should decrease size
		s.unreg(c2)
		if s.size != 1 {
			t.Error("not 1")
		}
		s.unreg(c1)
		if s.size != 0 {
			t.Error("not 0")
		}
	})

	ot.Run("get/ret", func(t *testing.T) {
		s := &server{}
		c1 := &fakeConn{}
		s.reg(c1)
		s.ret(c1)

		c2 := s.get()
		if c2 == nil {
			t.Error("did not get")
		}
		c3 := s.get()
		if c3 != nil {
			t.Error("did get")
		}
		s.ret(c2)
		c3 = s.get()
		if c3 == nil {
			t.Error("did not get")
		}
	})

	ot.Run("prune", func(t *testing.T) {
		s := &server{}
		// Register and return three connections
		conns := make([]*fakeConn, 3)
		for i := range conns {
			c := &fakeConn{}
			conns[i] = c
			s.reg(c)
			s.ret(c)
		}

		// Let the conn in the middle be dead
		s.prune(func(c Connection) bool {
			return c != conns[1]
		})
		if s.size != 2 {
			t.Error("Should be pruned")
		}

		// Should be able to borrow twice
		b1 := s.get()
		b2 := s.get()
		b3 := s.get()
		if b1 == nil || b2 == nil {
			t.Error("Should be two conns to borrow")
		}
		if b3 != nil {
			t.Error("Shouldn't be able to borrow third")
		}

		// Return the connections and prune everything
		s.ret(b1)
		s.ret(b2)
		s.prune(func(c Connection) bool {
			return false
		})

		// Shouldn't be abble to borrow anything and size should be zero
		b1 = s.get()
		if b1 != nil {
			t.Error("Shouldn't be able to borrow")
		}
		if s.size != 0 {
			t.Error("Shouldn't be anything left")
		}
	})
}
