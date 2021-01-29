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
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package test

import (
	"github.com/golang/mock/gomock"
	"github.com/onsi/gomega"
)

type omegaMatcherWrapper struct {
	matcher gomega.OmegaMatcher
}

func WrapMatcher(matcher gomega.OmegaMatcher) gomock.Matcher {
	return &omegaMatcherWrapper{matcher: matcher}
}

func (wrapper *omegaMatcherWrapper) Matches(actual interface{}) bool {
	result, err := wrapper.matcher.Match(actual)
	if err != nil {
		panic(err)
	}
	return result
}

func (wrapper *omegaMatcherWrapper) String() string {
	return wrapper.matcher.FailureMessage(nil)
}
