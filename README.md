# neo4j-go-driver

This is the official Neo4j Go Driver.

## Getting the Driver

### Module version

Make sure your application has been setup to use go modules (there should be a go.mod file in your application root). Add the driver with:

`go mod edit -require github.com/neo4j/neo4j-go-driver@<the tag>`

### With `go get`

Add the driver with `go get github.com/neo4j/neo4j-go-driver/neo4j`

## Documentation
Drivers manual that describes general driver concepts in depth [here](https://neo4j.com/docs/driver-manual/1.7/).

Go package API documentation [here](https://pkg.go.dev/github.com/neo4j/neo4j-go-driver/neo4j).

## Minimum Viable Snippet

Connect, execute a statement and handle results

Make sure to use the configuration in the code that matches the version of Neo4j server that you run.

```go
// configForNeo4j35 := func(conf *neo4j.Config) {}
configForNeo4j40 := func(conf *neo4j.Config) { conf.Encrypted = false }

driver, err := neo4j.NewDriver("bolt://localhost:7687", neo4j.BasicAuth("username", "password", ""), configForNeo4j40)
if err != nil {
	return err
}
// handle driver lifetime based on your application lifetime requirements
// driver's lifetime is usually bound by the application lifetime, which usually implies one driver instance per application
defer driver.Close()

// For multidatabase support, set sessionConfig.DatabaseName to requested database
sessionConfig := neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite}
session, err := driver.NewSession(sessionConfig)
if err != nil {
	return err
}
defer session.Close()

result, err := session.Run("CREATE (n:Item { id: $id, name: $name }) RETURN n.id, n.name", map[string]interface{}{
	"id":   1,
	"name": "Item 1",
})
if err != nil {
	return err
}

for result.Next() {
	fmt.Printf("Created Item with Id = '%d' and Name = '%s'\n", result.Record().GetByIndex(0).(int64), result.Record().GetByIndex(1).(string))
}
return result.Err()
```

## Neo4j and Bolt protocol versions

The driver implements Bolt protocol version 3. This means that either Neo4j server 3.5 or above can be used with the driver.

Neo4j server 4 supports both Bolt protocol version 3 and version 4.

There will be an updated driver version that supports Bolt protocol version 4 to make use of new features introduced there.

## Connecting to a causal cluster

You just need to use `bolt+routing` as the URL scheme and set host of the URL to one of your core members of the cluster.

```go
if driver, err = neo4j.NewDriver("bolt+routing://localhost:7687", neo4j.BasicAuth("username", "password", "")); err != nil {
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
value := record.GetByIndex(0)
```

```go
if value, ok := record.Get('field_name'); ok {
	// a value with alias field_name was found
	// process value
}
```

### Value Types
The driver currently exposes values in the record as an `interface{}` type. 
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
| Point| neo4j.Point |

The temporal types are introduced in Neo4j 3.4 series.

You can create a 2-dimensional `Point` value using;

```go
point := NewPoint2D(srId, 1.0, 2.0)
```

or a 3-dimensional `Point` value using;

```go
point := NewPoint3D(srId, 1.0, 2.0, 3.0)
```

NOTE:
* For a list of supported `srId` values, please refer to the docs [here](https://neo4j.com/docs/developer-manual/current/cypher/functions/spatial/).

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
dateValue := record.GetByIndex(0).(neo4j.Date)
```

All custom temporal types can be constructing from a `time.Time` value using `<Type>Of()` (`DateOf`, `OffsetTimeOf`, ...) functions.

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

The `Log` field of the `neo4j.Config` struct is defined to be of interface `neo4j.Logging` which has the following definition.

```go
type Logging interface {
	ErrorEnabled() bool
	WarningEnabled() bool
	InfoEnabled() bool
	DebugEnabled() bool

	Errorf(message string, args ...interface{})
	Warningf(message string, args ...interface{})
	Infof(message string, args ...interface{})
	Debugf(message string, args ...interface{})
}
```

For a customised logging target, you can implement the above interface and pass an instance of that implementation to the `Log` field.
