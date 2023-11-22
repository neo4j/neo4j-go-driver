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

package neo4j

import (
	"context"
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/notifications"
	"os"
)

func ExampleConfig_disableNoCategories() {
	ctx := context.Background()
	driver, err := NewDriverWithContext(getUrl(), getAuth(), func(config *Config) {
		// makes the server return all notification categories
		config.NotificationsDisabledCategories = notifications.DisableNoCategories()
	})
	handleError(err)
	defer handleClose(ctx, driver)

	result, err := ExecuteQuery(ctx, driver, someDeprecatedQuery(), nil, EagerResultTransformer)
	handleError(err)

	for _, notification := range result.Summary.Notifications() {
		fmt.Println("description:", notification.Description())
		fmt.Println("category: ", notification.RawCategory())
		fmt.Println("severity: ", notification.RawSeverityLevel())
	}
}

func ExampleSessionConfig_disableNoCategories() {
	ctx := context.Background()
	driver, err := NewDriverWithContext(getUrl(), getAuth())
	handleError(err)
	defer handleClose(ctx, driver)

	session := driver.NewSession(ctx, SessionConfig{
		// makes the server return all notification categories
		// this overrides the driver level configuration of the same name
		NotificationsDisabledCategories: notifications.DisableNoCategories(),
	})

	result, err := session.Run(ctx, someDeprecatedQuery(), nil)
	handleError(err)

	summary, err := result.Consume(ctx)
	handleError(err)

	for _, notification := range summary.Notifications() {
		fmt.Println("description:", notification.Description())
		fmt.Println("category: ", notification.RawCategory())
		fmt.Println("severity: ", notification.RawSeverityLevel())
	}
}

func ExampleConfig_disableSomeCategories() {
	ctx := context.Background()
	driver, err := NewDriverWithContext(getUrl(), getAuth(), func(config *Config) {
		// makes the server return all notification categories but deprecations
		config.NotificationsDisabledCategories = notifications.DisableCategories(notifications.Deprecation)
	})
	handleError(err)
	defer handleClose(ctx, driver)

	result, err := ExecuteQuery(ctx, driver, someDeprecatedQuery(), nil, EagerResultTransformer)
	handleError(err)

	// will not see the deprecation notification
	for _, notification := range result.Summary.Notifications() {
		fmt.Println("description:", notification.Description())
		fmt.Println("category: ", notification.RawCategory())
		fmt.Println("severity: ", notification.RawSeverityLevel())
	}
}

func ExampleSessionConfig_disableSomeCategories() {
	ctx := context.Background()
	driver, err := NewDriverWithContext(getUrl(), getAuth())
	handleError(err)
	defer handleClose(ctx, driver)

	session := driver.NewSession(ctx, SessionConfig{
		// makes the server return all notification categories but deprecations
		// this overrides the driver level configuration of the same name
		NotificationsDisabledCategories: notifications.DisableCategories(notifications.Deprecation),
	})

	result, err := session.Run(ctx, someDeprecatedQuery(), nil)
	handleError(err)

	summary, err := result.Consume(ctx)
	handleError(err)

	// will not see the deprecation notification
	for _, notification := range summary.Notifications() {
		fmt.Println("description:", notification.Description())
		fmt.Println("category: ", notification.RawCategory())
		fmt.Println("severity: ", notification.RawSeverityLevel())
	}
}

func ExampleConfig_minimumSeverityLevel() {
	ctx := context.Background()
	driver, err := NewDriverWithContext(getUrl(), getAuth(), func(config *Config) {
		// makes the server return only notifications with severity level warning or higher
		config.NotificationsMinSeverity = notifications.WarningLevel
	})
	handleError(err)
	defer handleClose(ctx, driver)

	result, err := ExecuteQuery(ctx, driver, someQueryWithInformationNotification(), nil, EagerResultTransformer)
	handleError(err)

	// will only see notifications with severity level warning or higher
	for _, notification := range result.Summary.Notifications() {
		fmt.Println("description:", notification.Description())
		fmt.Println("category: ", notification.RawCategory())
		fmt.Println("severity: ", notification.RawSeverityLevel())
	}
}

func ExampleSessionConfig_minimumSeverityLevel() {
	ctx := context.Background()
	driver, err := NewDriverWithContext(getUrl(), getAuth())
	handleError(err)
	defer handleClose(ctx, driver)

	session := driver.NewSession(ctx, SessionConfig{
		// makes the server return only notifications with severity level warning or higher
		// this overrides the driver level configuration of the same name
		NotificationsMinSeverity: notifications.WarningLevel,
	})

	result, err := session.Run(ctx, someDeprecatedQuery(), nil)
	handleError(err)

	summary, err := result.Consume(ctx)
	handleError(err)

	// will only see notifications with severity level warning or higher
	for _, notification := range summary.Notifications() {
		fmt.Println("description:", notification.Description())
		fmt.Println("category: ", notification.RawCategory())
		fmt.Println("severity: ", notification.RawSeverityLevel())
	}
}

func getUrl() string {
	return fmt.Sprintf("%s://%s:%s", os.Getenv("TEST_NEO4J_SCHEME"), os.Getenv("TEST_NEO4J_HOST"), os.Getenv("TEST_NEO4J_PORT"))
}
func getAuth() AuthToken {
	return BasicAuth(os.Getenv("TEST_NEO4J_USER"), os.Getenv("TEST_NEO4J_PASS"), "")
}

func handleClose(ctx context.Context, closer interface{ Close(context.Context) error }) {
	if closer != nil {
		handleError(closer.Close(ctx))
	}
}

func someDeprecatedQuery() string {
	return "CREATE TEXT INDEX FOR (n:Foo) ON n.bar OPTIONS {indexProvider: 'text-1.0'}"
}

func someQueryWithInformationNotification() string {
	return "MATCH (n), (m) RETURN n, m"
}
