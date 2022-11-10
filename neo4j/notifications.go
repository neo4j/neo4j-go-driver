package neo4j

import (
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/bolt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/collection"
)

func processNotificationFilters(filters ...NotificationFilter) (notificationFilters, error) {
	dedupedFilters := collection.NewSet(filters)
	result := dedupedFilters.Values()
	for _, filter := range result {
		if !filter.Valid() {
			return nil, fmt.Errorf("notification filter %s is not supported", &filter)
		}
	}
	return result, nil
}

func notificationFilterRawValuesOf(rawFilters any) ([]string, error) {
	if rawFilters == nil {
		return nil, nil
	}
	switch filters := rawFilters.(type) {
	case notificationFilters:
		return filters.rawValues(), nil
	case *noNotification:
		return filters.rawValues(), nil
	case *serverDefaultNotifications:
		return filters.rawValues(), nil
	default:
		return nil, fmt.Errorf("unsupported notification filters type: %T", rawFilters)
	}
}

type NotificationSeverity string

const (
	// SeverityAll includes notifications of any NotificationSeverity.
	// Since 5.3
	SeverityAll NotificationSeverity = "*"
	// SeverityWarning includes notifications with warnings the user should act on.
	// Since 5.3
	SeverityWarning = "WARNING"
	// SeverityInformation includes notifications with information about the query.
	// Since 5.3
	SeverityInformation = "INFORMATION"
)

func (sev NotificationSeverity) Valid() bool {
	switch sev {
	case SeverityAll:
		fallthrough
	case SeverityWarning:
		fallthrough
	case SeverityInformation:
		return true
	}
	return false
}

type NotificationCategory string

const (
	// CategoryAll includes notifications of any NotificationCategory.
	// Since 5.3
	CategoryAll NotificationCategory = "*"
	// CategoryDeprecation includes notifications of queries using deprecated features.
	// Since 5.3
	CategoryDeprecation = "DEPRECATION"
	// CategoryHint includes notifications of queries specifying hints that cannot be satisfied.
	// Since 5.3
	CategoryHint = "HINT"
	// CategoryUnrecognized includes notifications of queries mentioning unknown entities.
	// Since 5.3
	CategoryUnrecognized = "UNRECOGNIZED"
	// CategoryUnsupported includes notifications of queries using unsupported or experimental features.
	// Since 5.3
	CategoryUnsupported = "UNSUPPORTED"
	// CategoryGeneric includes notifications of queries with potential issues
	// Since 5.3
	CategoryGeneric = "GENERIC"
	// CategoryPerformance includes notifications of queries using costly/slow operations.
	// Since 5.3
	CategoryPerformance = "PERFORMANCE"
)

func (cat NotificationCategory) Valid() bool {
	switch cat {
	case CategoryAll:
		fallthrough
	case CategoryDeprecation:
		fallthrough
	case CategoryHint:
		fallthrough
	case CategoryUnrecognized:
		fallthrough
	case CategoryUnsupported:
		fallthrough
	case CategoryGeneric:
		fallthrough
	case CategoryPerformance:
		return true
	}
	return false
}

type NotificationFilterType interface {
	notificationFilters | *noNotification | *serverDefaultNotifications
}

type notificationFilters []NotificationFilter

func (filters notificationFilters) rawValues() []string {
	result := make([]string, len(filters))
	for i, filter := range filters {
		result[i] = filter.String()
	}
	return result
}

type NotificationFilter struct {
	Severity NotificationSeverity
	Category NotificationCategory
}

func (filter *NotificationFilter) Valid() bool {
	return filter.Severity.Valid() && filter.Category.Valid()
}

func (filter *NotificationFilter) String() string {
	return fmt.Sprintf("%s.%s", filter.Severity, filter.Category)
}

type noNotification struct{}

func (n *noNotification) rawValues() []string {
	return noNoticationRawValues()
}

type serverDefaultNotifications struct{}

func (s *serverDefaultNotifications) rawValues() []string {
	return serverDefaultNoticationRawValues()
}

func NoNotificationFilters() *noNotification {
	return &noNotification{}
}

func ServerDefaultNotificationFilters() *serverDefaultNotifications {
	return &serverDefaultNotifications{}
}

func noNoticationRawValues() []string {
	return []string{bolt.NoNotifications}
}

func serverDefaultNoticationRawValues() []string {
	return []string{bolt.DefaultServerNotifications}
}
