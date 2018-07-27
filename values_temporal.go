/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

type Date struct {
	epochDays int64
}

type LocalTime struct {
	nanosOfDay time.Duration
}

type OffsetTime struct {
	nanosOfDay time.Duration
	offset     int
}

type LocalDateTime struct {
	sec  int64
	nsec int
}

type Duration struct {
	months  int64
	days    int64
	seconds int64
	nanos   int
}

func DateOf(of time.Time) Date {
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, of.Location())
	diff := of.Sub(epoch)
	epochDays := int64(diff.Seconds() / (60 * 60 * 24))

	return Date{epochDays: epochDays}
}

func (date Date) Time() time.Time {
	return epochUtc.Add(time.Duration(date.epochDays) * time.Duration(24) * time.Hour)
}

func (date Date) Year() int {
	return date.Time().Year()
}

func (date Date) Month() time.Month {
	return date.Time().Month()
}

func (date Date) Day() int {
	return date.Time().Day()
}

func (date Date) String() string {
	return date.Time().Format("2006-01-02")
}

func LocalTimeOf(of time.Time) LocalTime {
	nanosOfDay := time.Duration(of.Hour())*time.Hour +
		time.Duration(of.Minute())*time.Minute +
		time.Duration(of.Second())*time.Second +
		time.Duration(of.Nanosecond())

	return LocalTime{nanosOfDay}
}

func (localTime LocalTime) Time() time.Time {
	return time.Date(0, 0, 0, 0, 0, 0, 0, time.Local).Add(localTime.nanosOfDay)
}

func (localTime LocalTime) Hour() int {
	return localTime.Time().Hour()
}

func (localTime LocalTime) Minute() int {
	return localTime.Time().Minute()
}

func (localTime LocalTime) Second() int {
	return localTime.Time().Second()
}

func (localTime LocalTime) Nanosecond() int {
	return localTime.Time().Nanosecond()
}

func (localTime LocalTime) String() string {
	return localTime.Time().Format("15:04:05.999999999")
}

func OffsetTimeOf(of time.Time) OffsetTime {
	nanosOfDay := time.Duration(of.Hour())*time.Hour +
		time.Duration(of.Minute())*time.Minute +
		time.Duration(of.Second())*time.Second +
		time.Duration(of.Nanosecond())
	_, offset := of.Zone()

	return OffsetTime{nanosOfDay, offset}
}

func (offsetTime OffsetTime) Time() time.Time {
	year, month, day := time.Now().Date()
	return time.Date(year, month, day, 0, 0, 0, 0, time.FixedZone("Offset", offsetTime.offset)).Add(offsetTime.nanosOfDay)
}

func (offsetTime OffsetTime) Hour() int {
	return offsetTime.Time().Hour()
}

func (offsetTime OffsetTime) Minute() int {
	return offsetTime.Time().Minute()
}

func (offsetTime OffsetTime) Second() int {
	return offsetTime.Time().Second()
}

func (offsetTime OffsetTime) Nanosecond() int {
	return offsetTime.Time().Nanosecond()
}

func (offsetTime OffsetTime) Offset() int {
	return offsetTime.offset
}

func (offsetTime OffsetTime) String() string {
	return offsetTime.Time().Format("15:04:05.999999999Z07:00")
}

func LocalDateTimeOf(of time.Time) LocalDateTime {
	utc := time.Date(of.Year(), of.Month(), of.Day(), of.Hour(), of.Minute(), of.Second(), of.Nanosecond(), time.UTC)
	return LocalDateTime{utc.Unix(), utc.Nanosecond()}
}

func (localTime LocalDateTime) Time() time.Time {
	return time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Second*time.Duration(localTime.sec) + time.Duration(localTime.nsec))
}

func (localTime LocalDateTime) Year() int {
	return localTime.Time().Year()
}

func (localTime LocalDateTime) Month() time.Month {
	return localTime.Time().Month()
}

func (localTime LocalDateTime) Day() int {
	return localTime.Time().Day()
}

func (localTime LocalDateTime) Hour() int {
	return localTime.Time().Hour()
}

func (localTime LocalDateTime) Minute() int {
	return localTime.Time().Minute()
}

func (localTime LocalDateTime) Second() int {
	return localTime.Time().Second()
}

func (localTime LocalDateTime) Nanosecond() int {
	return localTime.Time().Nanosecond()
}

func (localTime LocalDateTime) String() string {
	return localTime.Time().Format("2006-01-02T15:04:05.999999999")
}

func DurationOf(months int64, days int64, seconds int64, nanos int) Duration {
	return Duration{months, days, seconds, nanos}
}

func (duration Duration) Months() int64 {
	return duration.months
}

func (duration Duration) Days() int64 {
	return duration.days
}

func (duration Duration) Seconds() int64 {
	return duration.seconds
}

func (duration Duration) Nanos() int {
	return duration.nanos
}

func (duration Duration) String() string {
	sign := ""
	if duration.seconds < 0 && duration.nanos > 0 {
		duration.seconds += 1
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
