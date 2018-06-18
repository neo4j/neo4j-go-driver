package test_utils

import (
	"testing"
	"reflect"
)

func AssertLen(t *testing.T, value interface{}, expected int) {
	if value == nil {
		t.Errorf("Expected length of %d, but nil found", expected)
	}

	valueType := reflect.TypeOf(value)
	if valueType.Kind() == reflect.Ptr {
		AssertLen(t, reflect.ValueOf(value).Elem().Interface(), expected)
	} else {
		length := -1
		switch valueType.Kind() {
		case reflect.String:
			fallthrough
		case reflect.Slice:
			fallthrough
		case reflect.Map:
			length = reflect.ValueOf(value).Len()
		}

		if length == -1 {
			t.Errorf("Value %q does not satisfy Len() query.", value)
		}

		if length != expected {
			t.Errorf("Expected value %q to be of length %d but got %d.", value, expected, length)
		}
	}
}

func AssertMapKey(t *testing.T, dict *map[string]interface{}, key string) {
	if _, ok := (*dict)[key]; !ok {
		t.Errorf("Expected map %q to contain key '%q'", dict, key)
	}
}

func AssertMapKeyValue(t *testing.T, dict *map[string]interface{}, key string, value interface{}) {
	if val, ok := (*dict)[key]; ok {
		if val != value {
			t.Errorf("Expected map value '%q' for key '%q' to be equal to '%q'", val, key, value)
		}
	} else {
		t.Errorf("Expected map %q to contain key '%q'", dict, key)
	}
}
