package notifications

type NotificationCategory string

const (
	Hint         NotificationCategory = "HINT"
	Unrecognized NotificationCategory = "UNRECOGNIZED"
	Unsupported  NotificationCategory = "UNSUPPORTED"
	Performance  NotificationCategory = "PERFORMANCE"
	Deprecation  NotificationCategory = "DEPRECATION"
	Generic      NotificationCategory = "GENERIC"
)

type NotificationDisabledCategories struct {
	categories []NotificationCategory
	none       bool
}

func NotificationDisableCategories(value ...NotificationCategory) NotificationDisabledCategories {
	return NotificationDisabledCategories{value, false}
}
func NotificationAllCategories() NotificationDisabledCategories {
	return NotificationDisabledCategories{nil, true}
}

func (n *NotificationDisabledCategories) DisablesNone() bool {
	return n.none
}

func (n *NotificationDisabledCategories) DisabledCategories() []NotificationCategory {
	return n.categories
}

type NotificationMinimumSeverityLevel string

const (
	DefaultLevel     NotificationMinimumSeverityLevel = ""
	DisabledLevel    NotificationMinimumSeverityLevel = "DISABLED"
	WarningLevel     NotificationMinimumSeverityLevel = "WARNING"
	InformationLevel NotificationMinimumSeverityLevel = "INFORMATION"
)
