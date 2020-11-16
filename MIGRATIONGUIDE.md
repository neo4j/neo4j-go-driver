# Migration guide for 1.x to 4.x

## Imports
All import statements need to be changed.

```go
// 1.x
import "github.com/neo4j/neo4j-go-driver/neo4j"

// 4.x, note the new v4 within the package name!
import "github.com/neo4j/neo4j-go-driver/v4/neo4j"
```

Note that it is possible to migrate a piece at the time by using both drivers and gradually 
rewriting application code to use the 4.x driver.

## Interfaces to structs
A lot of interfaces on 1.x API has changed to being vanilla structs instead of interfaces,
this mainly involves entities that are "pure data" like nodes and records, previously they
were implemented as interfaces with only getters.

The compiler should find all these places for you and the change should be simple, add a * or &
where appropriate.

These types have changed from being interfaces to being structs:
* Record.
Easier access to values and keys. Functions Values() and Keys() has been removed and
been replaced with the corresponding slices instead.
GetByIndex and Get (gets by key) remains but GetByIndex is marked as deprecated, replace
record.GetByIndex(0) with record.Values[0].

* All graph types: Node, Relationship and Path.
All access to data in these needs to be changed from function calls to direct access:
    node.Id() changes to node.Id.

## Session creation
For simplification the NewSession function on driver no longer returns an error, for the rare
occasion where an error occured the error will be returned when the session is used.
This reduces amount of error checking. Only error that could happen on session creation
was creating a session on a closed driver.

## Aliases
A lot of types has been moved from neo4j to a subpackages. These types are aliased in the neo4j
package so it should not have any impact other than the inconvenince of hitting the alias type
instead of the real one  when navigating driver code in your IDE. Reason for moving the types is
to be able to use subpackages within the driver and at the same time avoiding unnecessary 
copying/conversions between types.

These types have been aliased:
* neo4j.Record is alias for neo4j/db.Record
* All graph types (Node, Relationship, Path) are aliased from neo4j to corresponsing type for neo4j/dbtype.X
* Point split into Point2D and Point3D aliased as neo4j/dbtype.Point2D and neo4j/dbtype.Point3D
* All temporal types are aliased from neo4j to neo4j/dbtype.X

## Spatial types
Point struct has been splitted into two types to better correlate with types in database.
Previously there was one Point that stored number of dimensions (two or three). Now there are
two different types instead: Point2D and Point3D.

Previously there were accessor functions to retrieve values for X, Y, Z (when appropriate) and
SrId, now these are fields on the struct instead. So all access to points needs to change from 
    p.X() to p.X. 
BREAKING: previously the record contained *Point now it contains a value instead. So all code using
spatial types needs to change how those fields are being retrieved from the record:
    point := record.(*neo4j.Point) changes to point2 := record.(neo4j.Point2D) or  point3 := record.(neo4j.Point3D)
If it is unknown at query time if a 2D/3D are expected a type switch should be used when retrieving.

## Temporal types
A big change in implementation but not necessarily a big change in the API. In 1.x there were
separate struct types for each temporal types, all of these has been replaced with Go native
time.Time type definitions (not aliased). so from:
    type LocalTime struct {...} to type LocalTime time.Time.
That means that type casting is used instead of function calls when going from a native Go time to
database time type:
```go
now := time.Now()
// 1.x
localNow := neo4j.LocalTimeOf(now) // Call
// 4.x
localNow := neo4j.LocalTime(now) // Cast
// Driver will not care about timezone information in localNow since it is of time neo4j.LocalTime
```

The conversion functions like neo4j.LocalTimeOf and similair still exists but might be deprecated
in the future.

Conversion from neo4j temporal types to Go time.Time is also just a cast, but beware that going
from a neo4j.Time to Go time.Time means that the date parts will not make sense, as going from
neo4j.Date to Go time.Time the time of day parts also will not make sense.

    Temporal type       Valid information after cast
    ====================================================
    neo4j.Time          Hour, minute, second, millisecond, nanosecond, timezone
    neo4j.LocalTime     Hour, minute, second, millisecond, nanosecond
    neo4j.Date          Year, month, day
    neo4j.LocalDateTime All but timezone that is always local

The only temporal type that remains the same is Duration, reason for not using built in time.Duration
the same way as we're doing with time.Time is that Neo4j can represent longer durations than Go
time.Duration.

## Errors
On 1.x no error types were exposed in the API, there were functions to check if an error was a
specific class of error (neo4j.IsSecurityError, neo4j.IsAuthenticationError, ...).

On 4.x four different error types are exposed with more details on the error and similair helper
functions as on 1.x to check if an error is of that type, to get access to the details in the
struct, casting is needed.

The four different errors are:
* Neo4jError, represents error returned from Neo4j server. The struct contains of an error
code and a message. The code can be used to further classify the error into a classification,
category and title.
* UsageError, used to signal a purely driver-side problem related to the usage of the driver API
* ConnectivityError, indicates some kind of problem related to communicating with a Neo4j server
or a cluster of Neo4j servers. Includes connections pool and authentication problems.
* TransactionExecutionLimit, indicates that a retryable transaction failed due to a timeout
or other resource limit reached. The struct contains a list of errors where each error represents
a failed transaction try.

## Error handling
Previously syntax errors in Cypher queries and other errors originating from the database were
returned when iteration of a result started, now these errors are returned directly upon call
to Run (on session and transaction).
On 1.x
```go
_, result := tx.Run("invalid cypher")
for result.Next() {
    // Would not happen, result.Err() will  return the error
}
```
On 4.x
```go
err, result := tx.Run("invalid cypher")
// err will contain the error
```


## Result interface
The Summary function has been removed, the same functionality can be accomplished by iterating
through the result and then call Consume or if only the summary is wanted and not the records,
directly call Consume. The previous implementation of summary was a bit magic, it buffered all
rows in memory until it received the summary wich could potentially consume a lot of memory.

An alternative to Next has been added called NextRecord that saves a function call per iteration of
the result.

## Logging
The interface used for custom driver logging has been changed to match the interfaced used
internally since 1.8. Since 1.8 driver was backwards compatible with 1.7 all internal logging
calls were remapped to the public interface, this is now gone.

The new interface gives more control over what is being logged than the previous. All logging
calls are given a component name and a component id along with the logging message and the
arguments. This gives the opportunity to filter even more than on the level, all errors belonging
to a certain component or instance of a component can be filtered out.

## New URI scheme
The trust strategies used when configuring a driver as well as the encrypted flag on 1.x has been
replaced with URI schemes that themselves contain the trust strategy and the encryption.

    URI                 Encryption      Trust strategy
    ==================================================
    bolt://...          false           Not applicable
    bolt+s://...        true            TrustSystem
    bolt+ssc://...      true            TrustAny

The URI scheme used on 1.x to enable routing has changed from bolt+routing://... to neo4j://...
So in table above all mentions of bolt are also applicable to neo4j scheme.

The neo4j scheme is the recommended scheme regardless if a cluster or a single instance is used
on Neo4j version 4.0 and above.

## Utilities
neo4j.Collect and neo4j.Single that are used to collect a slice of records or a single record
from a result has been changed to use the Result interface. Previous example of usage:
    records, err := neo4j.Collect(session.ReadTransaction("MATCH (n) RETURN n", nil))
is now discouraged and will not compile anymore
Collect/Single previously could consume a result returned from a transactional function,
which isn't allowed and doesn't work unless all records are buffered internally and that could
cause unintential buffering of very large results.

## Result usage out of transaction scope
On 1.x driver it was possible to use a result outside of it's transaction boundary, this is no
longer allowed.

This code will no longer work:
```go
result, err := session.ReadTransaction(func(tx neo4j.Transaction) (interface{}, error) {
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

Should be written as:
```go
rec, err := session.ReadTransaction(func(tx neo4j.Transaction) (interface{}, error) {
    result, err :=  tx.Run("RETURN 1 as n", nil)
    if err != nil {
        return nil, err
    }
    var rec *neo4j.Record
    if !result.NextRecord(&rec) {
        return nil, errors.New("No record")
    }
    return rec, nil
})

## Result usage out of session scope
On 1.x driver it was possible to iterate through a result after the sessions has been closed,
this is no longer allowed.

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

Should be rewritten to use the result before closing the session:
```go
result, err := session.Run("RETURN 1 as n", nil)
if err != nil {
    return err
}
// Buffer all records in memory
records, err := result.Collect()
session.Close()
```
