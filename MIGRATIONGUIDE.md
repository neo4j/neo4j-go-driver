Migration guide for 1.x to 4.x

Imports
ALL import statements need to be changed from "github.com/neo4j/neo4j-go-driver/neo4j/" to
"github.com/neo4j-go-driver/v4/neo4j". Note the new v4 within the package name! Note that it is
possible to migrate a piece at the time by using both drivers and gradually rewriting application
code to use the 4.x driver.

Interfaces to structs
A lot of interfaces on 1.x API has changed to being vanilla structs instead, this mainly involves
entities that is pure data like nodes and records, they only had methods to get data (getters) before,
now it will be struct accesses instead. Code changes to handle this involves letting the compiler find
all errors and add a * or & where appropriate.
These types have changed from being interfaces to being structs:
* Record. Easier access to values and keys. Functions Values() and Keys() has been removed and
been replaced with the corresponding slices instead. GetByIndex and Get (gets by key) remains
but GetByIndex is marked as deprecated, replace record.GetByIndex(0) with record.Values[0].
* All graph types: Node, Relationship and Path. All access to data in these needs to be changed
from function calls to direct access: node.Id() changes to node.Id.

Aliases
A lot of types has been moved from neo4j to subpackages. These types are aliased in neo4j package so
it should not have much of impact other than the inconvenince of hitting the alias type instead of the
real one  when navigating driver code. Reason for moving the types is to be able to have subpackages
and avoid unnecessary copying/conversion between types (and avoid circular references neo4j -> subpackage -> neo4j reference chains).
These types have been aliased:
* neo4j.Record is alias for neo4j/db.Record
* All graph types (Node, Relationship, Path) are aliased from neo4j to corresponsing type for neo4j/dbtype.X
* Point split into Point2D and Point3D aliased as neo4j/dbtype.Point2D and neo4j/dbtype.Point3D
* All temporal types are aliased from neo4j to neo4j/dbtype.X

Spatial types
Point has been splitted into two types to better correlate with types in database.
Previously there was one Point that stored number of dimensions (two or three). Now there are
two different types instead, Point2D and Point3D. Previously there were accessor functions to
retrieve values for X, Y, Z (when appropriate) and SrId, now these are fields on the struct
instead. So all access to points needs to change from p.X() to p.X. IMPORTANT: previously the record
contained *Point (pointer reference) now it contains values instead. So all code using spatial data
needs to change how that is being retrieved:
From point := record.(*neo4j.Point) to point2 := record.(*neo4j.Point2D) or point3 := record.(*neo4j.Point3D)
If it is unknown at query time a type switch should be used.

Temporal types.
A big change in implementation but not necessarily a big change in the API. In 1.x there were separate struct types for each temporal types, all of these has been replaced with time.Time type definitions (not aliased). so from type LocalTime struct {...} to type LocalTime time.Time. That means that type casting is used instead of function calls when going from a native Go time to database time type. 
    now := time.Now()
    // Previously
    localNow := neo4j.LocalTimeOf(now) // Call
    // Now
    localNow := neo4j.LocalTime(now) // Cast
    // Driver will not care about timezone information in localNow
The conversion functions like neo4j.LocalTimeOf and similair still exists but will probably be
deprecated.
Conversion from neo4j temporal types to Go time.Time is also just a cast, but beware that going
from a neo4j.Time to Go time.Time means that the date parts will not make sense, as going from
neo4j.Date to Go time.Time whe time of day parts also will not make sense.
The only temporal type that remains the same is Duration, reason for not using built in time.Duration
the same way as we're doint with time.Time is that database can represent longer durations than
built in duration.


Error handling
Previously syntax errors in Cypher queries and other errors originating from the database were
returned when iteration of a result started, now these errors are returned directly upon call
to Run (on session and transaction).
1.x
    _, result := tx.Run("invalid cypher")
    for result.Next() {
        // Would not happen, result.Err() will  return the error
    }
4.x
    err, result := tx.Run("invalid cypher")
    // err will contain the error

Result interface
The Summary function has been removed, the same functionality can be accomplished by iterating
through the result and then call Consume or if only the summary is wanted and not the records,
directly call Consume. The previous implementation of summary was a bit magic, it buffered all
rows in memory until it received the summary wich could potentially consume a lot of memory.
An alternative to Next has been added called NextRecord that saves a function call per iteration of
the result.

Logging
The interface used for custom driver logging has been changed to match the interfaced used
internally since 1.8. Since 1.8 driver was backwards compatible with 1.7 all internal logging
calls were remapped to the public interface, this is now gone.

Driver config (include logging here???)

