package neo4j_go_driver

import "errors"

type Record struct {
	keys   []string
	values []interface{}
}

func (record *Record) Keys() []string {
	return record.keys
}

func (record *Record) Values() []interface{} {
	return record.values
}

func (record *Record) Scan(dest ...interface{}) error {
	return nil
}

func (record *Record) Get(key string) (interface{}, bool) {
	for i := range record.keys {
		if record.keys[i] == key {
			return record.values[i], true
		}
	}

	return nil, false
}

func (record *Record) GetByIndex(index int) interface{} {
	if len(record.values) <= index {
		panic(errors.New("index out of bounds"))
	}

	return record.values[index]
}
