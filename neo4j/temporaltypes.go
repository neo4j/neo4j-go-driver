/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
	"fmt"
	"time"
)

// Date represents a date value, without a time zone and time related components.
type Date struct {
	epochDays int64
}

// LocalTime represents a local time value.
type LocalTime struct {
	nanosOfDay time.Duration
}

// OffsetTime represents a time value with a UTC offset.
type OffsetTime struct {
	nanosOfDay time.Duration
	offset     int
}

// LocalDateTime represents a local date time value, without a time zone.
type LocalDateTime struct {
	sec  int64
	nsec int
}

// Duration represents temporal amount containing months, days, seconds and nanoseconds.
type Duration struct {
	months  int64
	days    int64
	seconds int64
	nanos   int
}

const (
	nanosPerDay int64 = 24 * int64(time.Hour)
)

var (
	epochUtc = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
)

// DateOf creates a local date from the provided instant by extracting year, month and day fields.
func DateOf(of time.Time) Date {
	ofUtc := time.Date(of.Year(), of.Month(), of.Day(), 0, 0, 0, 0, time.UTC)
	diffHours := ofUtc.Sub(epochUtc).Hours()
	epochDays := diffHours / 24

	return Date{epochDays: int64(epochDays)}
}

// Time converts the local date to a time instant with fields other than year, month and day set to 0.
// Returned time's location is time.UTC.
func (date Date) Time() time.Time {
	return epochUtc.Add(time.Duration(date.epochDays) * time.Duration(24) * time.Hour)
}

// Year returns the year component of this instance.
func (date Date) Year() int {
	return date.Time().Year()
}

// Month returns the month component of this instance.
func (date Date) Month() time.Month {
	return date.Time().Month()
}

// Day returns the day component of this instance.
func (date Date) Day() int {
	return date.Time().Day()
}

// String returns the string representation of this Date in ISO-8601 compliant form.
func (date Date) String() string {
	return date.Time().Format("2006-01-02")
}

// LocalTimeOf creates a local time from the provided instant by extracting hour, minute, second and nanosecond
// fields.
func LocalTimeOf(of time.Time) LocalTime {
	nanosOfDay := time.Duration(of.Hour())*time.Hour +
		time.Duration(of.Minute())*time.Minute +
		time.Duration(of.Second())*time.Second +
		time.Duration(of.Nanosecond())

	return LocalTime{nanosOfDay}
}

// Time converts the local time to a time instant with fields other than hour, minute, second and nanosecond
// set to 0. Returned time's location is time.Local.
func (localTime LocalTime) Time() time.Time {
	return time.Date(0, 0, 0, 0, 0, 0, 0, time.Local).Add(localTime.nanosOfDay)
}

// Hour returns the hour component of this instance.
func (localTime LocalTime) Hour() int {
	return localTime.Time().Hour()
}

// Minute returns the minute component of this instance.
func (localTime LocalTime) Minute() int {
	return localTime.Time().Minute()
}

// Second returns the second component of this instance.
func (localTime LocalTime) Second() int {
	return localTime.Time().Second()
}

// Nanosecond returns the nanosecond component of this instance.
func (localTime LocalTime) Nanosecond() int {
	return localTime.Time().Nanosecond()
}

// String returns the string representation of this LocalTime in ISO-8601 compliant form.
func (localTime LocalTime) String() string {
	return localTime.Time().Format("15:04:05.999999999")
}

// OffsetTimeOf creates an offset time from the provided instant by extracting hour, minute, second and nanosecond
// fields and it's zone offset.
func OffsetTimeOf(of time.Time) OffsetTime {
	nanosOfDay := time.Duration(of.Hour())*time.Hour +
		time.Duration(of.Minute())*time.Minute +
		time.Duration(of.Second())*time.Second +
		time.Duration(of.Nanosecond())
	_, offset := of.Zone()

	return OffsetTime{nanosOfDay, offset}
}

// Time converts the offset time to a time instant with fields other than hour, minute, second and nanosecond
// set to 0. Returned time's location is a fixed zone with name 'Offset' and corresponding offset value.
func (offsetTime OffsetTime) Time() time.Time {
	year, month, day := time.Now().Date()
	return time.Date(year, month, day, 0, 0, 0, 0, time.FixedZone("Offset", offsetTime.offset)).Add(offsetTime.nanosOfDay)
}

// Hour returns the hour component of this instance.
func (offsetTime OffsetTime) Hour() int {
	return offsetTime.Time().Hour()
}

// Minute returns the minute component of this instance.
func (offsetTime OffsetTime) Minute() int {
	return offsetTime.Time().Minute()
}

// Second returns the second component of this instance.
func (offsetTime OffsetTime) Second() int {
	return offsetTime.Time().Second()
}

// Nanosecond returns the nanosecond component of this instance.
func (offsetTime OffsetTime) Nanosecond() int {
	return offsetTime.Time().Nanosecond()
}

// Offset returns the offset of this instance in seconds.
func (offsetTime OffsetTime) Offset() int {
	return offsetTime.offset
}

// String returns the string representation of this OffsetTime in ISO-8601 compliant form.
func (offsetTime OffsetTime) String() string {
	return offsetTime.Time().Format("15:04:05.999999999Z07:00")
}

// LocalDateTimeOf creates an local date time from the provided instant by extracting its temporal fields.
func LocalDateTimeOf(of time.Time) LocalDateTime {
	utc := time.Date(of.Year(), of.Month(), of.Day(), of.Hour(), of.Minute(), of.Second(), of.Nanosecond(), time.UTC)
	return LocalDateTime{utc.Unix(), utc.Nanosecond()}
}

// Time converts the local date time to a corresponding time instant.
// Returned time's location is time.UTC.
func (localDateTime LocalDateTime) Time() time.Time {
	return time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Second*time.Duration(localDateTime.sec) + time.Duration(localDateTime.nsec))
}

// Year returns the year component of this instance.
func (localDateTime LocalDateTime) Year() int {
	return localDateTime.Time().Year()
}

// Month returns the month component of this instance.
func (localDateTime LocalDateTime) Month() time.Month {
	return localDateTime.Time().Month()
}

// Day returns the day component of this instance.
func (localDateTime LocalDateTime) Day() int {
	return localDateTime.Time().Day()
}

// Hour returns the hour component of this instance.
func (localDateTime LocalDateTime) Hour() int {
	return localDateTime.Time().Hour()
}

// Minute returns the minute component of this instance.
func (localDateTime LocalDateTime) Minute() int {
	return localDateTime.Time().Minute()
}

// Second returns the second component of this instance.
func (localDateTime LocalDateTime) Second() int {
	return localDateTime.Time().Second()
}

// Nanosecond returns the nanosecond component of this instance.
func (localDateTime LocalDateTime) Nanosecond() int {
	return localDateTime.Time().Nanosecond()
}

// String returns the string representation of this LocalDateTime in ISO-8601 compliant form.
func (localDateTime LocalDateTime) String() string {
	return localDateTime.Time().Format("2006-01-02T15:04:05.999999999")
}

// DurationOf creates a Duration with provided temporal fields.
func DurationOf(months int64, days int64, seconds int64, nanos int) Duration {
	return Duration{months, days, seconds, nanos}
}

// Months returns the number of months in this duration.
func (duration Duration) Months() int64 {
	return duration.months
}

// Days returns the number of days in this duration.
func (duration Duration) Days() int64 {
	return duration.days
}

// Seconds returns the number of seconds in this duration.
func (duration Duration) Seconds() int64 {
	return duration.seconds
}

// Nanos returns the number of nanoseconds in this duration.
func (duration Duration) Nanos() int {
	return duration.nanos
}

// String returns the string representation of this Duration in ISO-8601 compliant form.
func (duration Duration) String() string {
	sign := ""
	if duration.seconds < 0 && duration.nanos > 0 {
		duration.seconds++
		duration.nanos = int(time.Second) - duration.nanos

		if duration.seconds == 0 {
			sign = "-"
		}
	}

	timePart := ""
	if duration.nanos == 0 {
		timePart = fmt.Sprintf("%s%d", sign, duration.seconds)
	} else {
		timePart = fmt.Sprintf("%s%d.%09d", sign, duration.seconds, duration.nanos)
	}

	return fmt.Sprintf("P%dM%dDT%sS", duration.months, duration.days, timePart)
}
