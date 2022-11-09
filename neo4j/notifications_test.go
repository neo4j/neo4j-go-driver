package neo4j

import (
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/testutil"
	"testing"
)

func TestNotificationSeverity_Valid(outer *testing.T) {
	outer.Parallel()
	type testCase struct {
		severity NotificationSeverity
		isValid  bool
	}

	testCases := []testCase{
		{severity: SeverityAll, isValid: true},
		{severity: SeverityWarning, isValid: true},
		{severity: SeverityInformation, isValid: true},
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
		category NotificationCategory
		isValid  bool
	}

	testCases := []testCase{
		{category: CategoryAll, isValid: true},
		{category: CategoryDeprecation, isValid: true},
		{category: CategoryHint, isValid: true},
		{category: CategoryUnrecognized, isValid: true},
		{category: CategoryUnsupported, isValid: true},
		{category: CategoryGeneric, isValid: true},
		{category: CategoryPerformance, isValid: true},
		{category: "MADE_UP", isValid: false},
	}

	for _, testCase := range testCases {
		outer.Run(string(testCase.category), func(t *testing.T) {
			testutil.AssertDeepEquals(t, testCase.category.Valid(), testCase.isValid)
		})
	}
}

func TestNotificationFilter_Valid(outer *testing.T) {
	outer.Parallel()
	validSeverities := []NotificationSeverity{
		SeverityAll,
		SeverityWarning,
		SeverityInformation,
	}
	validCategories := []NotificationCategory{
		CategoryAll,
		CategoryDeprecation,
		CategoryHint,
		CategoryUnrecognized,
		CategoryUnsupported,
		CategoryGeneric,
		CategoryPerformance,
	}
	for _, severity := range validSeverities {
		for _, category := range validCategories {
			outer.Run(fmt.Sprintf("%s.%s", string(severity), string(category)), func(t *testing.T) {
				filter := NotificationFilter{
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
		filter := NotificationFilter{
			Severity: NotificationSeverity("Sever...Us.Snape"),
			Category: CategoryUnrecognized,
		}
		if filter.Valid() {
			t.Errorf("expected filter to be invalid but was not")
		}
	})

	outer.Run("invalid category in filter", func(t *testing.T) {
		filter := NotificationFilter{
			Severity: SeverityWarning,
			Category: NotificationCategory("😸egory"),
		}
		if filter.Valid() {
			t.Errorf("expected filter to be invalid but was not")
		}
	})

	outer.Run("invalid severity&category in filter", func(t *testing.T) {
		filter := NotificationFilter{
			Severity: NotificationSeverity("Sever...Us.Snape"),
			Category: NotificationCategory("😸egory"),
		}
		if filter.Valid() {
			t.Errorf("expected filter to be invalid but was not")
		}
	})
}

func TestNewNotificationFilters(outer *testing.T) {
	outer.Parallel()

	outer.Run("deduplicates filters", func(t *testing.T) {
		filters, err := NewNotificationFilters(
			NotificationFilter{
				Severity: SeverityInformation,
				Category: CategoryGeneric,
			},
			NotificationFilter{
				Severity: SeverityWarning,
				Category: CategoryDeprecation,
			},
			NotificationFilter{
				Severity: SeverityInformation,
				Category: CategoryGeneric,
			},
		)

		testutil.AssertNoError(t, err)
		testutil.AssertDeepEquals(t, []NotificationFilter(filters), []NotificationFilter{
			{
				Severity: SeverityInformation,
				Category: CategoryGeneric,
			},
			{
				Severity: SeverityWarning,
				Category: CategoryDeprecation,
			},
		})
	})

	outer.Run("rejects invalid filters", func(t *testing.T) {
		_, err := NewNotificationFilters(NotificationFilter{
			// invalid because "oopsie: severity and category are mixed"
			Severity: CategoryUnrecognized,
			Category: SeverityInformation,
		})

		testutil.AssertErrorMessageContains(t, err, "filter UNRECOGNIZED.INFORMATION is not supported")
	})
}

func TestNotificationFiltersToStringSlice(outer *testing.T) {
	outer.Parallel()

	type testCase struct {
		name   string
		input  any
		output []string
		err    error
	}

	filters, _ := NewNotificationFilters(
		NotificationFilter{Severity: SeverityAll, Category: CategoryDeprecation},
		NotificationFilter{Severity: SeverityInformation, Category: CategoryGeneric},
	)
	testCases := []testCase{
		{
			name:   "filter set",
			input:  filters,
			output: []string{"*.DEPRECATION", "INFORMATION.GENERIC"},
		},
		{
			name:   "nil filters",
			input:  nil,
			output: nil,
		},
		{
			name:   "no filters", // punk is not dead
			input:  NoNotificationFilters(),
			output: nil,
		},
		{
			name:   "server-side default filters",
			input:  ServerDefaultNotificationFilters(),
			output: []string{"SERVER_DEFAULT"},
		},
		{
			name:  "invalid default filters",
			input: "wut? nope",
			err:   fmt.Errorf("unsupported notification filters type: string"),
		},
	}
	for _, test := range testCases {
		outer.Run(test.name, func(t *testing.T) {
			output, err := notificationFilterRawValuesOf(test.input)

			testutil.AssertDeepEquals(t, output, test.output)
			testutil.AssertDeepEquals(t, err, test.err)
		})
	}
}
