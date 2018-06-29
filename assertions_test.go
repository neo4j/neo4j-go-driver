package neo4j_go_driver

import (
	"testing"
	"reflect"
)

func assertTrue(t *testing.T, value interface{}) {
	assertBoolean(t, value, true)
}

func assertFalse(t *testing.T, value interface{}) {
	assertBoolean(t, value, false)
}

func assertBoolean(t *testing.T, value interface{}, expected bool) {
	valueAsBool, ok := value.(bool)
	if !ok {
		t.Errorf("Expected value to be a boolean, but found '%v'", value)
	}

	if valueAsBool != expected {
		t.Errorf("Expected value to be '%v', but found '%v'", expected, valueAsBool)
	}
}


func assertNil(t *testing.T, value interface{}) {
	if value != nil && (reflect.ValueOf(value).Kind() == reflect.Ptr && !reflect.ValueOf(value).IsNil()) {
		t.Errorf("Expected value to be nil, but found '%v'", value)
	}
}

func assertNonNil(t *testing.T, value interface{}) {
	if value == nil || (reflect.ValueOf(value).Kind() == reflect.Ptr && reflect.ValueOf(value).IsNil()) {
		t.Errorf("Expected value to be non-nil, but found nil")
	}
}

func assertLen(t *testing.T, value interface{}, expected int) {
	if value == nil {
		t.Errorf("Expected length of %d, but nil found", expected)
	}

	valueType := reflect.TypeOf(value)
	if valueType.Kind() == reflect.Ptr {
		assertLen(t, reflect.ValueOf(value).Elem().Interface(), expected)
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
			t.Errorf("Value '%v' does not satisfy Len() query.", value)
		}

		if length != expected {
			t.Errorf("Expected value '%v' to be of length %d but got %d.", value, expected, length)
		}
	}
}

func assertMapContainsKey(t *testing.T, dict *map[string]interface{}, key string) {
	if _, ok := (*dict)[key]; !ok {
		t.Errorf("Expected map '%v' to contain key '%q'", dict, key)
	}
}

func assertMapContainsKeyValue(t *testing.T, dict *map[string]interface{}, key string, value interface{}) {
	if val, ok := (*dict)[key]; ok {
		if val != value {
			t.Errorf("Expected map value '%v' for key '%v' to be equal to '%v'", val, key, value)
		}
	} else {
		t.Errorf("Expected map '%v' to contain key '%v'", dict, key)
	}
}
