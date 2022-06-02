# Migration guide for 1.x to 4.x

## Imports

All import statements need to be changed.

```go
// 1.x
import "github.com/neo4j/neo4j-go-driver/neo4j"

// 4.x, note the new v4 within the package name!
import "github.com/neo4j/neo4j-go-driver/v4/neo4j"

// 5.x, note the new v5 within the package name!
import "github.com/neo4j/neo4j-go-driver/v5/neo4j"
```

Note that it is possible to migrate a piece at the time by using both drivers
and gradually
rewriting application code to use the 4.x or 5.x driver.

## Interfaces to structs

A lot of interfaces on 1.x API has changed to being vanilla structs instead of
interfaces, this mainly involves entities that are "pure data" like nodes
and records, previously they were implemented as interfaces with only getters.

The compiler should find all these places for you and the change should be
simple, add a * or & where appropriate.

These types have changed from being interfaces to being structs:

* Record.
  `Record` now provides an easier access to values and keys. Functions 
  `Values()`and `Keys()` have been removed and replaced with the 
  corresponding slices instead.
  `GetByIndex` and `Get` (gets by key) remain but `GetByIndex` is marked as
  deprecated in version 4 and removed in version 5. Replace `record.
  GetByIndex(0)` with `record.Values[0]`.

* All graph types: `Node`, `Relationship` and `Path`.
  All access to data in these needs to be changed from function calls to direct
  access:
  node.Id() changes to node.Id.

## Session creation

For simplification the `NewSession` function on driver no longer returns an
error, for the rare occasion where an error occurred, the error will be
returned when the session is used.
This reduces the amount of error checking. The only error that could happen on
session creation in 1.x was creating a session on a closed driver.

## Aliases

Starting with 4.0, a lot of types has been moved from `neo4j` to a subpackages.
These types are aliased in the `neo4j` package, so it should not have any
impact other than the inconvenience of hitting the alias type instead of the
real one when navigating driver code in your IDE. Reason for moving the
types is to be able to use subpackages within the driver and at the same
time avoiding unnecessary copying/conversions between types.

These types have been aliased:

* `neo4j.Record` is alias for `neo4j/db.Record`
* All graph types (`Node`, `Relationship`, `Path`) are aliased from neo4j to
  corresponding type in `neo4j/dbtype`
* `Point` has been split into `Point2D` and `Point3D`. These are aliased as
  `neo4j/dbtype.Point2D` and `neo4j/dbtype.Point3D` (see more details below)
* All temporal types are aliased from `neo4j` to their equivalent
  in `neo4j/dbtype`

## Spatial types

Starting with 4.0, `Point` struct has been split into two types to better 
correlate with types in database. Previously there was one `Point` that 
stored number of dimensions (two or three).
Now there are two different types instead: `Point2D` and `Point3D`.

Previously, there were accessor functions to retrieve values for `X`, `Y`, 
`Z` (when appropriate) and `SrId`, now these are fields on the struct instead. 
So the accesses to points needs to change from `p.X()` to `p.X`.

BREAKING: previously the record contained `*Point` now it contains a value
instead. So all code using spatial types needs to change how those fields 
are being retrieved from the record:
```go
point := record.(*neo4j.Point)
```
changes to:
```go
point2 := record.(neo4j.Point2D)
```
or changes to:
```go
point3 := record.(neo4j.Point3D)
```

If it is unknown at query time if a 2D/3D point is expected, a type switch 
should be used when retrieving.

## Temporal types

This follows a big change in the 4.0 implementation but not necessarily a big 
change in the API. In 1.x there were separate struct types for each temporal 
types, all of these has been replaced with Go native `time.Time` type 
definitions (not aliased). 
From:
`type LocalTime struct {...}` to `type LocalTime time.Time`.

That means that type casting is used instead of function calls when going from a
native Go time to database time type:

```go
now := time.Now()
// 1.x
localNow := neo4j.LocalTimeOf(now) // Call
// 4.x and later
localNow := neo4j.LocalTime(now) // Cast
// Driver will not care about timezone information in localNow since it is of time neo4j.LocalTime
```

The conversion functions like `neo4j.LocalTimeOf` and similar still exists but
might be deprecated in the future.

Conversion from neo4j temporal types to Go `time.Time` is also just a cast, but
beware that going from a `neo4j.Time` to Go `time.Time` means that the date 
parts will not make sense. Similarly, going from `neo4j.Date` to Go `time.
Time`, the time of day parts also will not make sense.

```
Temporal type       Valid information after cast
====================================================
neo4j.Time          Hour, minute, second, millisecond, nanosecond, timezone
neo4j.LocalTime     Hour, minute, second, millisecond, nanosecond
neo4j.Date          Year, month, day
neo4j.LocalDateTime All but timezone that is always local
```

The only temporal type that remains the same is `Duration`.
The reason for not using the standard `time.Duration` is that Neo4j can 
represent longer durations.

## Errors

In 1.x, no error types were exposed in the API, there were functions to check if
an error was a specific class of error (`neo4j.IsSecurityError`, `neo4j.
IsAuthenticationError`, ...).

Starting with 4.0, four different error types are exposed with more details on 
the error and similar helper functions as 1.x to check if an error is of 
that type. In order to get access to the details in the struct, casting is 
needed.

The four different errors are:

* `Neo4jError`, represents error returned from Neo4j server. The struct contains
  of an error code and a message. The code can be used to further classify 
  the error into a classification, category and title.
* `UsageError`, used to signal a purely driver-side problem related to the usage
  of the driver API
* `ConnectivityError`, indicates some kind of problem related to communicating
  with a Neo4j server or a cluster of Neo4j servers. Includes connection 
  pool and authentication issues.
* `TransactionExecutionLimit`, indicates that a retryable transaction failed due
  to a timeout or other resource limit reached. The struct contains a list 
  of errors where each error represents a failed transaction attempt.

## Error handling

Previously syntax errors in Cypher queries and other errors originating from the
database were returned when iteration of a result started, now these errors 
are returned directly upon call to Run (on session and transaction).

In 1.x:

```go
_, result := tx.Run("invalid cypher")
for result.Next() {
// Would not happen, result.Err() will return the error
}
```

Starting with 4.0:

```go
err, result := tx.Run("invalid cypher")
// err will contain the error
```

## Result interface

Starting with 4.0, the `Summary` function has been removed, the same 
functionality can be accomplished by iterating through the result and then 
call `Consume` or if only the summary is wanted and not the records, 
directly call `Consume`.
The previous implementation of summary was a bit magic, it buffered all
rows in memory until it received the summary which could potentially consume a
lot of memory.

An alternative to `Next` called `NextRecord` has been added that saves a 
function call per iteration of the result.

## Logging

The interface used for custom driver logging has been changed to match the
interfaced used internally since 1.8. Since 1.8 driver was backwards 
compatible with 1.7 all internal logging calls were remapped to the public 
interface, this is now gone starting with 4.0.

The new interface gives more control over what is being logged than the
previous. All logging calls are given a component name and a component id 
along with the logging message and the arguments. This gives the opportunity 
to filter even more than on the level, all errors belonging to a certain 
component or instance of a component can be filtered out.

## New URI scheme

The trust strategies used when configuring a driver as well as the encrypted
flag on 1.x have been replaced in 4.0 and later with URI schemes that 
themselves  contain the trust strategy and the encryption.

```
    URI                 Encryption      Trust strategy
    ==================================================
    bolt://...          false           Not applicable
    bolt+s://...        true            TrustSystem
    bolt+ssc://...      true            TrustAny
```

The URI scheme used on 1.x to enable routing has changed from `bolt+routing` 
to `neo4j`. In the table above, all mentions of `bolt` are also applicable 
to the `neo4j` scheme.

The `neo4j` scheme is the recommended scheme regardless if a cluster or a single
instance is used on Neo4j version 4.0 and above. `neo4j` is not supported 
for standalone 3.x Neo4j servers.

## Utilities

`neo4j.Collect` and `neo4j.Single` that are used to collect a slice of records or a
single record from a result have been changed in 4.0  to use the `Result` 
interface. 

Previous example of usage in 1.x:

```go
records, err := neo4j.Collect(session.ReadTransaction("MATCH (n) RETURN n",
nil))
```

will not compile anymore in 4.x and later.

`Collect`/`Single` previously could consume a result returned from a transactional
function. This is not allowed and does not work unless all records are buffered 
internally and that could cause unintended buffering of very large results.

## Result usage out of transaction scope

In 1.x, it was possible to use a result outside its transaction 
boundary, this is no longer allowed in 4.x and later.

This code will no longer work:

```go
result, err := session.ReadTransaction(func (tx neo4j.Transaction) (interface{}, error) {
    return tx.Run("RETURN 1 as n", nil)
})
if err != nil {
    return err
}
// This will fail since result should be used within the transaction scope
var rec *neo4j.Record
if !(result.(neo4j.Result)).NextRecord(&rec) {
    return errors.New("No record")
}
```

Starting with 4.0, this is written as:

```go
rec, err := session.ReadTransaction(func (tx neo4j.Transaction) (interface{}, error) {
  result, err := tx.Run("RETURN 1 as n", nil)
  if err != nil {
      return nil, err
  }
  var rec *neo4j.Record
  if !result.NextRecord(&rec) {
      return nil, errors.New("No record")
  }
  return rec, nil
})
```

## Result usage out of session scope
In 1.x, it was possible to iterate through a result after the session has 
been closed, this is no longer allowed.

This code will no longer work:

```go
result, err := session.Run("RETURN 1 as n", nil)
session.Close()
// This will fail since result should be used within the session scope
var rec *neo4j.Record
if !result.NextRecord(&rec) {
    // This will always happen on 4.x
    return errors.New("No record")
}
```

Starting with 4.0, this is written as follows:

```go
result, err := session.Run("RETURN 1 as n", nil)
if err != nil {
return err
}
// Buffer all records in memory
records, err := result.Collect()
session.Close()
```

## 5.0 breaking changes

### 4.4 deprecation removals

Every deprecated element in 4.4 has been removed from 5.0.
This includes:

 - the `Driver#Session` function, use `Driver#NewSession` instead
 - the `ResultSummary#Statement` function, use `ResultSummary#Query` instead
 - the `Query#Params` function, use `Query#Parameters` instead
 - the `ServerInfo#Version` function, use `ServerInfo#Agent` and 
   `ServerInfo#ProtocolVersion` or call the Cypher procedure `dbms.components`
 - the `Record#GetByIndex` function, access the `Record#Values` slice instead

### Transaction Timeout value

In 4.x, all timeout values configured with `neo4j.WithTxTimeout` other than 
0 would be sent to the server and the default timeout value would be 0.

In 5.0:
 - the default value is now `math.MinInt`
 - if the timeout has the default value, no timeout are sent to the server 
   and the server uses the default server-defined timeout
 - negative values (other than `math.MinInt`) are forbidden and result in a 
   `UsageError`
 - a timeout of `0` is now sent to the server and disables the transaction 
   timeout

### Visibility changes

Starting with 5.0:

 - `db.AccessMode`
 - `db.TxHandle`
 - `db.StreamHandle`
 - `db.Command`
 - `db.TxConfig`
 - `db.Connection`
 - `db.RoutingTable`
 - `db.DefaultDatabase`
 - `db.DatabaseSelector`

are all internal to the driver.
This should have 0 impact as they were internal utilities unreachable from
the public APIs.