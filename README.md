# neo4j-go-driver

This is the official Neo4j Go Driver.

## Getting the Driver

### Module version

Make sure your application has been setup to use go modules (there should be a go.mod file in your application root). Add the driver with:

`go get github.com/neo4j/neo4j-go-driver/v4@<the 4.x tag>`

For versions 1.x of the driver (notice the absence of `/v4`), run instead the following:

`go get github.com/neo4j/neo4j-go-driver@<the 1.x tag>`

## Documentation
Drivers manual that describes general driver concepts in depth [here](https://neo4j.com/docs/go-manual/4.2/).

Go package API documentation [here](https://pkg.go.dev/github.com/neo4j/neo4j-go-driver/v4).

## Migration from 1.8
See [migrationguide](MIGRATIONGUIDE.md) for information on how to migrate from 1.8 (and 1.7) version of the driver.

## Minimum Viable Snippet

Connect, execute a statement and handle results

Make sure to use the configuration in the code that matches the version of Neo4j server that you run.

```go
// Neo4j 4.0, defaults to no TLS therefore use bolt:// or neo4j://
// Neo4j 3.5, defaults to self-signed cetificates, TLS on, therefore use bolt+ssc:// or neo4j+ssc://
dbUri := "neo4j://localhost:7687"
driver, err := neo4j.NewDriver(dbUri, neo4j.BasicAuth("username", "password", ""))

// Handle driver lifetime based on your application lifetime requirements  driver's lifetime is usually
// bound by the application lifetime, which usually implies one driver instance per application
defer driver.Close()

// Sessions are shortlived, cheap to create and NOT thread safe. Typically create one or more sessions
// per request in your web application. Make sure to call Close on the session when done.
// For multidatabase support, set sessionConfig.DatabaseName to requested database
// Session config will default to write mode, if only reads are to be used configure session for
// read mode.
session := driver.NewSession(neo4j.SessionConfig{})
defer session.Close()

result, err := session.Run("CREATE (n:Item { id: $id, name: $name }) RETURN n.id, n.name", map[string]interface{}{
	"id":   1,
	"name": "Item 1",
})
if err != nil {
	return err
}

var record * neo4j.Record
for result.NextRecord(&record) {
	fmt.Printf("Created Item with Id = '%d' and Name = '%s'\n", record.Values[0].(int64), record.Values[1].(string))
}
return result.Err()
```

## Neo4j and Bolt protocol versions

The driver implements Bolt protocol version 3. This means that either Neo4j server 3.5 or above can be used with the driver.

Neo4j server 4 supports both Bolt protocol version 3 and version 4.

## Connecting to a causal cluster

You just need to use `neo4j` as the URL scheme and set host of the URL to one of your core members of the cluster.

```go
if driver, err = neo4j.NewDriver("neo4j://localhost:7687", neo4j.BasicAuth("username", "password", "")); err != nil {
	return err // handle error
}
```

There are a few points that need to be highlighted:
* Each `Driver` instance maintains a pool of connections inside, as a result, it is recommended to only use **one driver per application**.
* It is considerably cheap to create new sessions and transactions, as sessions and transactions do not create new connections as long as there are free connections available in the connection pool.
* The driver is thread-safe, while the session or the transaction is not thread-safe.

## Parsing Result Values
### Record Stream
A cypher execution result is comprised of a stream of records followed by a result summary.
The records inside the result can be accessed via `Next()`/`Record()` functions defined on `Result`. It is important to check `Err()` after `Next()` returning `false` to find out whether it is end of result stream or an error that caused the end of result consumption.

### Accessing Values in a Record
Values in a `Record` can be accessed either by index or by alias. The return value is an `interface{}` which means you need to convert the interface to the type expected

```go
value := record.Values[0]
```

```go
if value, ok := record.Get('field_name'); ok {
	// a value with alias field_name was found
	// process value
}
```

### Value Types
The driver exposes values in the record as an `interface{}` type. 
The underlying types of the returned values depend on the corresponding Cypher types.

The mapping between Cypher types and the types used by this driver (to represent the Cypher type):

| Cypher Type | Driver Type
| ---: | :--- |
| *null* | nil |
| List | []interface{} |
| Map  | map[string]interface{} |
| Boolean| bool |
| Integer| int64 |
| Float| float |
| String| string |
| ByteArray| []byte |
| Node| neo4j.Node |
| Relationship| neo4j.Relationship |
| Path| neo4j.Path |

### Spatial Types - Point

| Cypher Type | Driver Type
| ---: | :--- |
| Point| neo4j.Point2D |
| Point| neo4j.Point3D |

The temporal types are introduced in Neo4j 3.4 series.

You can create a 2-dimensional `Point` value using;

```go
point := neo4j.Point2D {X: 1.0, Y: 2.0, SpatialRefId: srId }
```

or a 3-dimensional point value using;

```go
point := neo4j.Point3D {X: 1.0, Y: 2.0, Z: 3.0, SpatialRefId: srId }
```

NOTE:

* For a list of supported `srId` values, please refer to the docs [here](https://neo4j.com/docs/cypher-manual/current/syntax/spatial/#cypher-spatial-crs-geographic).

### Temporal Types - Date and Time

The temporal types are introduced in Neo4j 3.4 series. Given the fact that database supports a range of different temporal types, most of them are backed by custom types defined at the driver level.

The mapping among the Cypher temporal types and actual exposed types are as follows:

| Cypher Type | Driver Type |
| :----------: | :-----------: |
| Date | neo4j.Date |
| Time | neo4j.OffsetTime |
| LocalTime| neo4j.LocalTime |
| DateTime | time.Time |
| LocalDateTime | neo4j.LocalDateTime |
| Duration | neo4j.Duration |


Receiving a temporal value as driver type:
```go
dateValue := record.Values[0].(neo4j.Date)
```

All custom temporal types can be constructed from a `time.Time` value using `<Type>Of()` (`DateOf`, `OffsetTimeOf`, ...) functions.

```go
dateValue := DateOf(time.Date(2005, time.December, 16, 0, 0, 0, 0, time.Local)
```

Converting a custom temporal value into `time.Time` (all `neo4j` temporal types expose `Time()` function to gets its corresponding `time.Time` value):
```go
dateValueAsTime := dateValue.Time()
```

Note:
* When `neo4j.OffsetTime` is converted into `time.Time` or constructed through `OffsetTimeOf(time.Time)`, its `Location` is given a fixed name of `Offset` (i.e. assigned `time.FixedZone("Offset", offsetTime.offset)`).
* When `time.Time` values are sent/received through the driver, if its `Zone()` returns a name of `Offset` the value is stored with its offset value and with its zone name otherwise.

## Logging

Logging at the driver level can be configured by setting `Log` field of `neo4j.Config` through configuration functions that can be passed to `neo4j.NewDriver` function.

### Console Logger

For simplicity, we provide a predefined console logger which can be constructed by `neo4j.ConsoleLogger` function. To enable console logger, you need to specify which level you need to enable (`neo4j.ERROR`, `neo4j.WARNING`, `neo4j.INFO` and `neo4j.DEBUG` which are ordered by the level of detail).

A simple code snippet that will enable console logging is as follows;

```go
useConsoleLogger := func(level neo4j.LogLevel) func(config *neo4j.Config) {
	return func(config *neo4j.Config) {
		config.Log = neo4j.ConsoleLogger(level)
	}
}

// Construct a new driver
if driver, err = neo4j.NewDriver(uri, neo4j.BasicAuth(username, password, ""), useConsoleLogger(neo4j.ERROR)); err != nil {
	return err
}
defer driver.Close()
```

### Custom Logger

The `Log` field of the `neo4j.Config` struct is defined to be of interface `neo4j/log.Logger` which has the following definition.

```go
type Logger interface {
	Error(name string, id string, err error)
	Warnf(name string, id string, msg string, args ...interface{})
	Infof(name string, id string, msg string, args ...interface{})
	Debugf(name string, id string, msg string, args ...interface{})
}
```

For a customised logging target, you can implement the above interface and pass an instance of that implementation to the `Log` field.
