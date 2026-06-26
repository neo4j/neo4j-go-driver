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

package bolt

// ringQueue is a FIFO ring-buffer deque that reuses its backing array across
// pushes and pops. Once it has grown to the working-set size, steady-state use
// (the request/response handler flow and per-record result buffering) allocates
// nothing, unlike a linked list which allocates an element per push. It is not
// safe for concurrent use.
type ringQueue[T any] struct {
	buf  []T
	head int
	size int
}

func (q *ringQueue[T]) length() int { return q.size }

// pushBack appends to the tail of the queue.
func (q *ringQueue[T]) pushBack(v T) {
	q.grow(q.size + 1)
	q.buf[(q.head+q.size)%len(q.buf)] = v
	q.size++
}

// pushFront prepends to the head of the queue.
func (q *ringQueue[T]) pushFront(v T) {
	q.grow(q.size + 1)
	q.head = (q.head - 1 + len(q.buf)) % len(q.buf)
	q.buf[q.head] = v
	q.size++
}

// popFront removes and returns the element at the head of the queue. It must not
// be called on an empty queue.
func (q *ringQueue[T]) popFront() T {
	v := q.buf[q.head]
	var zero T
	q.buf[q.head] = zero // release the reference so it can be garbage collected
	q.head = (q.head + 1) % len(q.buf)
	q.size--
	return v
}

// back returns the element at the tail of the queue without removing it. It must
// not be called on an empty queue.
func (q *ringQueue[T]) back() T {
	return q.buf[(q.head+q.size-1)%len(q.buf)]
}

// clear drops all elements while retaining the backing array for reuse.
func (q *ringQueue[T]) clear() {
	var zero T
	for i := 0; i < q.size; i++ {
		q.buf[(q.head+i)%len(q.buf)] = zero
	}
	q.head = 0
	q.size = 0
}

func (q *ringQueue[T]) grow(n int) {
	if n <= len(q.buf) {
		return
	}
	newCap := len(q.buf) * 2
	if newCap < 4 {
		newCap = 4
	}
	for newCap < n {
		newCap *= 2
	}
	nb := make([]T, newCap)
	for i := 0; i < q.size; i++ {
		nb[i] = q.buf[(q.head+i)%len(q.buf)]
	}
	q.buf = nb
	q.head = 0
}
