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

package test

import (
	"fmt"
	"math"

	"github.com/onsi/gomega/types"
)

func BeNaN() types.GomegaMatcher {
	return &floatNaNMatcher{}
}

type floatNaNMatcher struct{}

func (matcher *floatNaNMatcher) Match(actual interface{}) (success bool, err error) {
	var f32 float32
	var f64 float64
	var ok bool

	if f32, ok = actual.(float32); ok {
		f64 = float64(f32)
	} else if f64, ok = actual.(float64); !ok {
		return false, fmt.Errorf("expected float32 or float64 values, found: %v", actual)
	}

	return math.IsNaN(f64), nil
}

func (matcher *floatNaNMatcher) FailureMessage(actual interface{}) (message string) {
	return fmt.Sprintf("Expected\n\t%#v\nto be NaN", actual)
}

func (matcher *floatNaNMatcher) NegatedFailureMessage(actual interface{}) (message string) {
	return fmt.Sprintf("Expected\n\t%#v\nnot to be NaN", actual)
}
