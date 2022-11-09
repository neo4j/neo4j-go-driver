package neo4j

import (
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/internal/collection"
)

type NotificationSeverity string

const (
	SeverityAll         NotificationSeverity = "*"
	SeverityWarning                          = "WARNING"
	SeverityInformation                      = "INFORMATION"
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
	CategoryAll          NotificationCategory = "*"
	CategoryDeprecation                       = "DEPRECATION"
	CategoryHint                              = "HINT"
	CategoryUnrecognized                      = "UNRECOGNIZED"
	CategoryUnsupported                       = "UNSUPPORTED"
	CategoryGeneric                           = "GENERIC"
	CategoryPerformance                       = "PERFORMANCE"
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

type NoNotification struct{}
type ServerDefaultNotifications struct{}

type NotificationFilterType interface {
	[]NotificationFilter | *NoNotification | *ServerDefaultNotifications
}

func NewNotificationFilters(filters ...NotificationFilter) ([]NotificationFilter, error) {
	dedupedFilters := collection.NewSet(filters)
	result := dedupedFilters.Values()
	for _, filter := range result {
		if !filter.Valid() {
			return nil, fmt.Errorf("notification filter %s is not supported", &filter)
		}
	}
	return result, nil
}

func NoNotificationFilters() *NoNotification {
	return &NoNotification{}
}

func ServerDefaultNotificationFilters() *ServerDefaultNotifications {
	return &ServerDefaultNotifications{}
}
