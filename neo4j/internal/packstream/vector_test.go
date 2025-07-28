/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package packstream

import (
	"encoding/hex"
	"reflect"
	"testing"
)

func TestVectorPacking(t *testing.T) {
	tests := []struct {
		name     string
		packFunc func(*Packer)
		expected []byte
	}{
		{
			name: "Float64 Vector",
			packFunc: func(p *Packer) {
				p.VectorFloat64([]float64{0.1, 0.2})
			},
			expected: []byte{
				0xb2,             // Struct of length 2
				0x56,             // Vector tag 'V'
				0xcc, 0x01, 0xc1, // Binary of length 1, FLOAT_64 marker
				0xcc, 0x10, // Binary of length 16
				0x3f, 0xb9, 0x99, 0x99, 0x99, 0x99, 0x99, 0x9a, // 0.1
				0x3f, 0xc9, 0x99, 0x99, 0x99, 0x99, 0x99, 0x9a, // 0.2
			},
		},
		{
			name: "Float32 Vector",
			packFunc: func(p *Packer) {
				p.VectorFloat32([]float32{0.1, 0.2})
			},
			expected: []byte{
				0xb2,             // Struct of length 2
				0x56,             // Vector tag 'V'
				0xcc, 0x01, 0xc6, // Binary of length 1, FLOAT_32 marker
				0xcc, 0x08, // Binary of length 8
				0x3d, 0xcc, 0xcc, 0xcd, // 0.1
				0x3e, 0x4c, 0xcc, 0xcd, // 0.2
			},
		},
		{
			name: "Int8 Vector",
			packFunc: func(p *Packer) {
				p.VectorInt8([]int8{1, 2, 3})
			},
			expected: []byte{
				0xb2,             // Struct of length 2
				0x56,             // Vector tag 'V'
				0xcc, 0x01, 0xc8, // Binary of length 1, INT_8 marker
				0xcc, 0x03, // Binary of length 3
				0x01, 0x02, 0x03, // Values
			},
		},
		{
			name: "Int16 Vector",
			packFunc: func(p *Packer) {
				p.VectorInt16([]int16{1, 2, 3})
			},
			expected: []byte{
				0xb2,             // Struct of length 2
				0x56,             // Vector tag 'V'
				0xcc, 0x01, 0xc9, // Binary of length 1, INT_16 marker
				0xcc, 0x06, // Binary of length 6
				0x00, 0x01, 0x00, 0x02, 0x00, 0x03, // Values
			},
		},
		{
			name: "Int32 Vector",
			packFunc: func(p *Packer) {
				p.VectorInt32([]int32{1, 2, 3})
			},
			expected: []byte{
				0xb2,             // Struct of length 2
				0x56,             // Vector tag 'V'
				0xcc, 0x01, 0xca, // Binary of length 1, INT_32 marker
				0xcc, 0x0c, // Binary of length 12
				0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03, // Values
			},
		},
		{
			name: "Int64 Vector",
			packFunc: func(p *Packer) {
				p.VectorInt64([]int64{1, 2, 3})
			},
			expected: []byte{
				0xb2,             // Struct of length 2
				0x56,             // Vector tag 'V'
				0xcc, 0x01, 0xcb, // Binary of length 1, INT_64 marker
				0xcc, 0x18, // Binary of length 24
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // 1
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, // 2
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, // 3
			},
		},
		{
			name: "Empty Vector",
			packFunc: func(p *Packer) {
				p.VectorFloat64([]float64{})
			},
			expected: []byte{
				0xb2,             // Struct of length 2
				0x56,             // Vector tag 'V'
				0xcc, 0x01, 0xc1, // Binary of length 1, FLOAT_64 marker
				0xcc, 0x00, // Binary of length 0 (empty values)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Packer{}
			p.Begin([]byte{})
			tt.packFunc(p)
			result, err := p.End()
			if err != nil {
				t.Fatalf("Packing failed: %v", err)
			}

			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("Packed result differs from expected")
				t.Errorf("Expected: %s", hex.EncodeToString(tt.expected))
				t.Errorf("Got:      %s", hex.EncodeToString(result))
			}
		})
	}
}

func TestVectorUnpacking(t *testing.T) {
	tests := []struct {
		name       string
		data       []byte
		unpackFunc func(*Unpacker) any
		expected   any
	}{
		{
			name: "Float64 Vector",
			data: []byte{
				0xb2,             // Struct of length 2
				0x56,             // Vector tag 'V'
				0xcc, 0x01, 0xc1, // Binary of length 1, FLOAT_64 marker
				0xcc, 0x10, // Binary of length 16
				0x3f, 0xb9, 0x99, 0x99, 0x99, 0x99, 0x99, 0x9a, // 0.1
				0x3f, 0xc9, 0x99, 0x99, 0x99, 0x99, 0x99, 0x9a, // 0.2
			},
			unpackFunc: func(u *Unpacker) any {
				return u.VectorFloat64()
			},
			expected: []float64{0.1, 0.2},
		},
		{
			name: "Float32 Vector",
			data: []byte{
				0xb2,             // Struct of length 2
				0x56,             // Vector tag 'V'
				0xcc, 0x01, 0xc6, // Binary of length 1, FLOAT_32 marker
				0xcc, 0x08, // Binary of length 8
				0x3d, 0xcc, 0xcc, 0xcd, // 0.1
				0x3e, 0x4c, 0xcc, 0xcd, // 0.2
			},
			unpackFunc: func(u *Unpacker) any {
				return u.VectorFloat32()
			},
			expected: []float32{0.1, 0.2},
		},
		{
			name: "Int8 Vector",
			data: []byte{
				0xb2,             // Struct of length 2
				0x56,             // Vector tag 'V'
				0xcc, 0x01, 0xc8, // Binary of length 1, INT_8 marker
				0xcc, 0x03, // Binary of length 3
				0x01, 0x02, 0x03, // Values
			},
			unpackFunc: func(u *Unpacker) any {
				return u.VectorInt8()
			},
			expected: []int8{1, 2, 3},
		},
		{
			name: "Int16 Vector",
			data: []byte{
				0xb2,             // Struct of length 2
				0x56,             // Vector tag 'V'
				0xcc, 0x01, 0xc9, // Binary of length 1, INT_16 marker
				0xcc, 0x06, // Binary of length 6
				0x00, 0x01, 0x00, 0x02, 0x00, 0x03, // Values
			},
			unpackFunc: func(u *Unpacker) any {
				return u.VectorInt16()
			},
			expected: []int16{1, 2, 3},
		},
		{
			name: "Int32 Vector",
			data: []byte{
				0xb2,             // Struct of length 2
				0x56,             // Vector tag 'V'
				0xcc, 0x01, 0xca, // Binary of length 1, INT_32 marker
				0xcc, 0x0c, // Binary of length 12
				0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03, // Values
			},
			unpackFunc: func(u *Unpacker) any {
				return u.VectorInt32()
			},
			expected: []int32{1, 2, 3},
		},
		{
			name: "Int64 Vector",
			data: []byte{
				0xb2,             // Struct of length 2
				0x56,             // Vector tag 'V'
				0xcc, 0x01, 0xcb, // Binary of length 1, INT_64 marker
				0xcc, 0x18, // Binary of length 24
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // 1
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, // 2
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, // 3
			},
			unpackFunc: func(u *Unpacker) any {
				return u.VectorInt64()
			},
			expected: []int64{1, 2, 3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u := &Unpacker{}
			u.Reset(tt.data)
			u.Next() // Move to the struct

			result := tt.unpackFunc(u)
			if u.Err != nil {
				t.Fatalf("Unpacking failed: %v", u.Err)
			}

			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("Unpacked result differs from expected")
				t.Errorf("Expected: %v", tt.expected)
				t.Errorf("Got:      %v", result)
			}
		})
	}
}

func TestVectorRoundTrip(t *testing.T) {
	tests := []struct {
		name       string
		packFunc   func(*Packer)
		unpackFunc func(*Unpacker) any
		input      any
	}{
		{
			name: "Float64 Vector Round Trip",
			packFunc: func(p *Packer) {
				p.VectorFloat64([]float64{1.0, 2.0, 3.0})
			},
			unpackFunc: func(u *Unpacker) any {
				return u.VectorFloat64()
			},
			input: []float64{1.0, 2.0, 3.0},
		},
		{
			name: "Float32 Vector Round Trip",
			packFunc: func(p *Packer) {
				p.VectorFloat32([]float32{0.1, 0.2, 0.3})
			},
			unpackFunc: func(u *Unpacker) any {
				return u.VectorFloat32()
			},
			input: []float32{0.1, 0.2, 0.3},
		},
		{
			name: "Int8 Vector Round Trip",
			packFunc: func(p *Packer) {
				p.VectorInt8([]int8{1, 2, 3, 4, 5})
			},
			unpackFunc: func(u *Unpacker) any {
				return u.VectorInt8()
			},
			input: []int8{1, 2, 3, 4, 5},
		},
		{
			name: "Int16 Vector Round Trip",
			packFunc: func(p *Packer) {
				p.VectorInt16([]int16{10, 20, 30, 40, 50})
			},
			unpackFunc: func(u *Unpacker) any {
				return u.VectorInt16()
			},
			input: []int16{10, 20, 30, 40, 50},
		},
		{
			name: "Int32 Vector Round Trip",
			packFunc: func(p *Packer) {
				p.VectorInt32([]int32{100, 200, 300, 400, 500})
			},
			unpackFunc: func(u *Unpacker) any {
				return u.VectorInt32()
			},
			input: []int32{100, 200, 300, 400, 500},
		},
		{
			name: "Int64 Vector Round Trip",
			packFunc: func(p *Packer) {
				p.VectorInt64([]int64{1000, 2000, 3000, 4000, 5000})
			},
			unpackFunc: func(u *Unpacker) any {
				return u.VectorInt64()
			},
			input: []int64{1000, 2000, 3000, 4000, 5000},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Pack
			p := &Packer{}
			p.Begin([]byte{})
			tt.packFunc(p)
			packed, err := p.End()
			if err != nil {
				t.Fatalf("Packing failed: %v", err)
			}

			// Unpack
			u := &Unpacker{}
			u.Reset(packed)
			u.Next() // Move to the struct
			result := tt.unpackFunc(u)
			if u.Err != nil {
				t.Fatalf("Unpacking failed: %v", u.Err)
			}

			// Compare
			if !reflect.DeepEqual(result, tt.input) {
				t.Errorf("Round trip failed")
				t.Errorf("Input:  %v", tt.input)
				t.Errorf("Output: %v", result)
			}
		})
	}
}
