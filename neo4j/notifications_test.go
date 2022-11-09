package neo4j_test

import (
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/testutil"
	"testing"
)

func TestNotificationSeverity_Valid(outer *testing.T) {
	outer.Parallel()
	type testCase struct {
		severity neo4j.NotificationSeverity
		isValid  bool
	}

	testCases := []testCase{
		{severity: neo4j.SeverityAll, isValid: true},
		{severity: neo4j.SeverityWarning, isValid: true},
		{severity: neo4j.SeverityInformation, isValid: true},
		{severity: "MADE_UP", isValid: false},
	}

	for _, testCase := range testCases {
		outer.Run(string(testCase.severity), func(t *testing.T) {
			testutil.AssertDeepEquals(t, testCase.severity.Valid(), testCase.isValid)
		})
	}
}

func TestNotificationCategory_Valid(outer *testing.T) {
	outer.Parallel()
	type testCase struct {
		severity neo4j.NotificationCategory
		isValid  bool
	}

	testCases := []testCase{
		{severity: neo4j.CategoryAll, isValid: true},
		{severity: neo4j.CategoryDeprecation, isValid: true},
		{severity: neo4j.CategoryHint, isValid: true},
		{severity: neo4j.CategoryUnrecognized, isValid: true},
		{severity: neo4j.CategoryUnsupported, isValid: true},
		{severity: neo4j.CategoryGeneric, isValid: true},
		{severity: neo4j.CategoryPerformance, isValid: true},
		{severity: "MADE_UP", isValid: false},
	}

	for _, testCase := range testCases {
		outer.Run(string(testCase.severity), func(t *testing.T) {
			testutil.AssertDeepEquals(t, testCase.severity.Valid(), testCase.isValid)
		})
	}
}

func TestNotificationFilter_Valid(outer *testing.T) {
	outer.Parallel()
	validSeverities := []neo4j.NotificationSeverity{
		neo4j.SeverityAll,
		neo4j.SeverityWarning,
		neo4j.SeverityInformation,
	}
	validCategories := []neo4j.NotificationCategory{
		neo4j.CategoryAll,
		neo4j.CategoryDeprecation,
		neo4j.CategoryHint,
		neo4j.CategoryUnrecognized,
		neo4j.CategoryUnsupported,
		neo4j.CategoryGeneric,
		neo4j.CategoryPerformance,
	}
	for _, severity := range validSeverities {
		for _, category := range validCategories {
			outer.Run(fmt.Sprintf("%s.%s", string(severity), string(category)), func(t *testing.T) {
				filter := neo4j.NotificationFilter{
					Severity: severity,
					Category: category,
				}
				if !filter.Valid() {
					t.Errorf("expected filter to be valid but was not")
				}
			})
		}
	}

	outer.Run("invalid severity in filter", func(t *testing.T) {
		filter := neo4j.NotificationFilter{
			Severity: neo4j.NotificationSeverity("Sever...Us.Snape"),
			Category: neo4j.CategoryUnrecognized,
		}
		if filter.Valid() {
			t.Errorf("expected filter to be invalid but was not")
		}
	})

	outer.Run("invalid category in filter", func(t *testing.T) {
		filter := neo4j.NotificationFilter{
			Severity: neo4j.SeverityWarning,
			Category: neo4j.NotificationCategory("😸egory"),
		}
		if filter.Valid() {
			t.Errorf("expected filter to be invalid but was not")
		}
	})

	outer.Run("invalid severity&category in filter", func(t *testing.T) {
		filter := neo4j.NotificationFilter{
			Severity: neo4j.NotificationSeverity("Sever...Us.Snape"),
			Category: neo4j.NotificationCategory("😸egory"),
		}
		if filter.Valid() {
			t.Errorf("expected filter to be invalid but was not")
		}
	})
}

func TestNewNotificationFilters(outer *testing.T) {
	outer.Parallel()

	outer.Run("deduplicates filters", func(t *testing.T) {
		filters, err := neo4j.NewNotificationFilters(
			neo4j.NotificationFilter{
				Severity: neo4j.SeverityInformation,
				Category: neo4j.CategoryGeneric,
			},
			neo4j.NotificationFilter{
				Severity: neo4j.SeverityWarning,
				Category: neo4j.CategoryDeprecation,
			},
			neo4j.NotificationFilter{
				Severity: neo4j.SeverityInformation,
				Category: neo4j.CategoryGeneric,
			},
		)

		testutil.AssertNoError(t, err)
		testutil.AssertDeepEquals(t, filters, []neo4j.NotificationFilter{
			{
				Severity: neo4j.SeverityInformation,
				Category: neo4j.CategoryGeneric,
			},
			{
				Severity: neo4j.SeverityWarning,
				Category: neo4j.CategoryDeprecation,
			},
		})
	})

	outer.Run("rejects invalid filters", func(t *testing.T) {
		_, err := neo4j.NewNotificationFilters(neo4j.NotificationFilter{
			// oopsie: severity and category are mixed
			Severity: neo4j.CategoryUnrecognized,
			Category: neo4j.SeverityInformation,
		})

		testutil.AssertErrorMessageContains(t, err, "filter UNRECOGNIZED.INFORMATION is not supported")
	})
}
