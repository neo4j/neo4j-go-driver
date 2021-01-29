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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package neo4j

import (
	"reflect"
	"time"

	"github.com/neo4j-drivers/gobolt"
)

const (
	dateSignature               int16 = 'D'
	dateSize                          = 1
	localTimeSignature          int16 = 't'
	localTimeSize                     = 1
	offsetTimeSignature         int16 = 'T'
	offsetTimeSize                    = 2
	durationSignature           int16 = 'E'
	durationSize                      = 4
	localDateTimeSignature      int16 = 'd'
	localDateTimeSize           int   = 2
	dateTimeWithOffsetSignature int16 = 'F'
	dateTimeWithZoneIdSignature int16 = 'f'
	dateTimeSize                int   = 3
)

type dateTimeValueHandler struct {
}

type dateValueHandler struct {
}

type localTimeValueHandler struct {
}

type offsetTimeValueHandler struct {
}

type localDateTimeValueHandler struct {
}

type durationValueHandler struct {
}

func (handler *dateValueHandler) ReadableStructs() []int16 {
	return []int16{dateSignature}
}

func (handler *dateValueHandler) WritableTypes() []reflect.Type {
	return []reflect.Type{reflect.TypeOf(Date{})}
}

func (handler *dateValueHandler) Read(signature int16, values []interface{}) (interface{}, error) {
	switch signature {
	case dateSignature:
		if len(values) != dateSize {
			return nil, gobolt.NewValueHandlerError("expected date struct to have %d fields but received %d", dateSize, len(values))
		}
		epochDays := values[0].(int64)
		return Date{epochDays}, nil
	}

	return nil, gobolt.NewValueHandlerError("unexpected struct signature provided to dateTimeValueHandler: %#x", signature)
}

func (handler *dateValueHandler) Write(value interface{}) (int16, []interface{}, error) {
	var date Date
	var ok bool

	if date, ok = value.(Date); !ok {
		return 0, nil, gobolt.NewValueHandlerError("passed in value %v is not supported by dateValueHandler", value)
	}

	return dateSignature, []interface{}{date.epochDays}, nil
}

func (handler *localTimeValueHandler) ReadableStructs() []int16 {
	return []int16{localTimeSignature}
}

func (handler *localTimeValueHandler) WritableTypes() []reflect.Type {
	return []reflect.Type{reflect.TypeOf(LocalTime{})}
}

func (handler *localTimeValueHandler) Read(signature int16, values []interface{}) (interface{}, error) {
	switch signature {
	case localTimeSignature:
		if len(values) != localTimeSize {
			return nil, gobolt.NewValueHandlerError("expected local time struct to have %d fields but received %d", localTimeSize, len(values))
		}
		nanosOfDay := values[0].(int64)
		return LocalTime{time.Duration(nanosOfDay)}, nil
	}

	return nil, gobolt.NewValueHandlerError("unexpected struct signature provided to localTimeValueHandler: %#x", signature)
}

func (handler *localTimeValueHandler) Write(value interface{}) (int16, []interface{}, error) {
	var localTime LocalTime
	var ok bool

	if localTime, ok = value.(LocalTime); !ok {
		return 0, nil, gobolt.NewValueHandlerError("passed in value %v is not supported by localTimeValueHandler", value)
	}

	return localTimeSignature, []interface{}{int64(localTime.nanosOfDay)}, nil
}

func (handler *offsetTimeValueHandler) ReadableStructs() []int16 {
	return []int16{offsetTimeSignature}
}

func (handler *offsetTimeValueHandler) WritableTypes() []reflect.Type {
	return []reflect.Type{reflect.TypeOf(OffsetTime{})}
}

func (handler *offsetTimeValueHandler) Read(signature int16, values []interface{}) (interface{}, error) {
	switch signature {
	case offsetTimeSignature:
		if len(values) != offsetTimeSize {
			return nil, gobolt.NewValueHandlerError("expected offset time struct to have %d fields but received %d", offsetTimeSize, len(values))
		}
		nanosOfDay := values[0].(int64)
		offset := values[1].(int64)
		return OffsetTime{time.Duration(nanosOfDay), int(offset)}, nil
	}

	return nil, gobolt.NewValueHandlerError("unexpected struct signature provided to offsetTimeValueHandler: %#x", signature)
}

func (handler *offsetTimeValueHandler) Write(value interface{}) (int16, []interface{}, error) {
	var offsetTime OffsetTime
	var ok bool

	if offsetTime, ok = value.(OffsetTime); !ok {
		return 0, nil, gobolt.NewValueHandlerError("passed in value %v is not supported by offsetTimeValueHandler", value)
	}

	return offsetTimeSignature, []interface{}{int64(offsetTime.nanosOfDay), offsetTime.offset}, nil
}

func (handler *durationValueHandler) ReadableStructs() []int16 {
	return []int16{durationSignature}
}

func (handler *durationValueHandler) WritableTypes() []reflect.Type {
	return []reflect.Type{reflect.TypeOf(Duration{})}
}

func (handler *durationValueHandler) Read(signature int16, values []interface{}) (interface{}, error) {
	switch signature {
	case durationSignature:
		if len(values) != durationSize {
			return nil, gobolt.NewValueHandlerError("expected duration struct to have %d fields but received %d", durationSize, len(values))
		}
		months := values[0].(int64)
		days := values[1].(int64)
		seconds := values[2].(int64)
		nanos := values[3].(int64)
		return Duration{months, days, seconds, int(nanos)}, nil
	}

	return nil, gobolt.NewValueHandlerError("unexpected struct signature provided to durationValueHandler: %#x", signature)
}

func (handler *durationValueHandler) Write(value interface{}) (int16, []interface{}, error) {
	var duration Duration
	var ok bool

	if duration, ok = value.(Duration); !ok {
		return 0, nil, gobolt.NewValueHandlerError("passed in value %v is not supported by durationValueHandler", value)
	}

	return durationSignature, []interface{}{duration.months, duration.days, duration.seconds, duration.nanos}, nil
}

func (handler *localDateTimeValueHandler) ReadableStructs() []int16 {
	return []int16{localDateTimeSignature}
}

func (handler *localDateTimeValueHandler) WritableTypes() []reflect.Type {
	return []reflect.Type{reflect.TypeOf(LocalDateTime{})}
}

func (handler *localDateTimeValueHandler) Read(signature int16, values []interface{}) (interface{}, error) {
	switch signature {
	case localDateTimeSignature:
		if len(values) != localDateTimeSize {
			return nil, gobolt.NewValueHandlerError("expected local date time struct to have %d fields but received %d", localDateTimeSize, len(values))
		}

		sec := values[0].(int64)
		nsec := values[1].(int64)

		return LocalDateTime{sec, int(nsec)}, nil
	}

	return nil, gobolt.NewValueHandlerError("unexpected struct signature provided to localDateTimeValueHandler: %#x", signature)
}

func (handler *localDateTimeValueHandler) Write(value interface{}) (int16, []interface{}, error) {
	var localDateTime LocalDateTime
	var ok bool

	if localDateTime, ok = value.(LocalDateTime); !ok {
		return 0, nil, gobolt.NewValueHandlerError("passed in value %v is not supported by localDateTimeValueHandler", value)
	}

	return localDateTimeSignature, []interface{}{
		localDateTime.sec,
		localDateTime.nsec,
	}, nil
}

func (handler *dateTimeValueHandler) ReadableStructs() []int16 {
	return []int16{dateTimeWithOffsetSignature, dateTimeWithZoneIdSignature}
}

func (handler *dateTimeValueHandler) WritableTypes() []reflect.Type {
	return []reflect.Type{reflect.TypeOf(time.Time{})}
}

func (handler *dateTimeValueHandler) Read(signature int16, values []interface{}) (interface{}, error) {
	switch signature {
	case dateTimeWithZoneIdSignature:
		if len(values) != dateTimeSize {
			return nil, gobolt.NewValueHandlerError("expected date time with zone id struct to have %d fields but received %d", dateTimeSize, len(values))
		}

		sec := values[0].(int64)
		nsec := values[1].(int64)
		zone := values[2].(string)
		location, err := time.LoadLocation(zone)
		if err != nil {
			return nil, gobolt.NewValueHandlerError("Unable to load time zone '%s'", zone)
		}

		utcTime := epochUtc.Add(time.Duration(sec)*time.Second + time.Duration(nsec))

		return time.Date(utcTime.Year(), utcTime.Month(), utcTime.Day(), utcTime.Hour(), utcTime.Minute(), utcTime.Second(), utcTime.Nanosecond(), location), nil
	case dateTimeWithOffsetSignature:
		if len(values) != dateTimeSize {
			return nil, gobolt.NewValueHandlerError("expected date time with offset struct to have %d fields but received %d", dateTimeSize, len(values))
		}

		sec := values[0].(int64)
		nsec := values[1].(int64)
		offsec := values[2].(int64)
		location := time.FixedZone("Offset", int(offsec))

		utcTime := epochUtc.Add(time.Duration(sec)*time.Second + time.Duration(nsec))

		return time.Date(utcTime.Year(), utcTime.Month(), utcTime.Day(), utcTime.Hour(), utcTime.Minute(), utcTime.Second(), utcTime.Nanosecond(), location), nil
	}

	return nil, gobolt.NewValueHandlerError("unexpected struct signature provided to dateTimeValueHandler: %#x", signature)
}

func (handler *dateTimeValueHandler) Write(value interface{}) (int16, []interface{}, error) {
	var dateTime time.Time
	var ok bool

	if dateTime, ok = value.(time.Time); !ok {
		return 0, nil, gobolt.NewValueHandlerError("passed in value %v is not supported by dateTimeValueHandler", value)
	}

	location := dateTime.Location()
	zoneId, offset := dateTime.Zone()
	utcTime := time.Date(dateTime.Year(), dateTime.Month(), dateTime.Day(), dateTime.Hour(), dateTime.Minute(), dateTime.Second(), dateTime.Nanosecond(), time.UTC)
	sec := utcTime.Unix()
	nsec := utcTime.Nanosecond()

	if zoneId == "Offset" {
		// with offset seconds
		return dateTimeWithOffsetSignature, []interface{}{
			sec,
			nsec,
			offset,
		}, nil
	}

	// with zone id
	return dateTimeWithZoneIdSignature, []interface{}{
		sec,
		nsec,
		location.String(),
	}, nil

}
