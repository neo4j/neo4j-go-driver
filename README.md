# neo4j-go-driver

This is the pre-release version of the official Neo4j Go Driver. It is based on our C Connector, and depends on seabolt to be available on the host system.

##  Requirements

This package requires the following tools/libraries to be installed in order to be built.

1. seabolt (see requirements [here](http://github.com/neo4j-drivers/seabolt))
2. pkg-config tool for your platform
3. working cgo environment for your platform

## Building

### Linux (Ubuntu)

1. Make sure you have pkg-config installed via `apt install pkg-config`
2. Clone [seabolt](http://github.com/neo4j-drivers/seabolt) (assume `<seabolt_dir>` to be the absolute path in which the clone resides) and make sure you can build it successfully (follow it's own instructions)
3. Add/Update environment variable `PKG_CONFIG_PATH` to include `<seabolt_dir>/build`
4. Add/Update environment variable `LD_LIBRARY_PATH` to include `<seabolt_dir>/build/lib`
5. Get this package via `go get github.com/neo4j/neo4j-go-driver`

### MacOS

1. Install pkg-config via `brew install pkg-config`
2. Clone [seabolt](http://github.com/neo4j-drivers/seabolt) (assume `<seabolt_dir>` to be the absolute path in which the clone resides) and make sure you can build it successfully,
3. Add/Update environment variable `PKG_CONFIG_PATH` to include `build` subdirectory of seabolt, i.e. `$PKG_CONFIG_PATH:<seabolt_dir>/build`
4. Add/Update environment variable `LD_LIBRARY_PATH` to include `<seabolt_dir>/build/lib`
5. Go Get this package via `go get github.com/neo4j/neo4j-go-driver`

### Windows

1. Install a mingw toolchain (for instance MSYS2 from https://www.msys2.org/) for cgo support (seabolt include some instructions),
2. Clone [seabolt](http://github.com/neo4j-drivers/seabolt) (assume `<seabolt_dir>` to be the absolute path in which the clone resides) and make sure you can build it successfully,
3. Add/Update environment variable `PKG_CONFIG_PATH` to include `build` subdirectory of seabolt, i.e. `%PKG_CONFIG_PATH%;<seabolt_dir>/build`
4. Update environment variable `PATH` to include `<seabolt_dir>/build/bin`
5. Go Get this package via `go get github.com/neo4j/neo4j-go-driver`

## Versioning

Although `master` branch contains the source code for the latest available release, we are also tagging releases after semantic versioning scheme so that `dep` will find latest release and add the driver as a dependency on your project.

## Getting the Driver

Add the driver as a dependency with `dep`.

```
dep ensure -add github.com/neo4j/neo4j-go-driver
```

## Minimum Viable Snippet

Connect, execute a statement and handle results

```go
var (
	driver neo4j.Driver
	session *neo4j.Session
	result *neo4j.Result
	err error
)

if driver, err = neo4j.NewDriver("bolt://localhost:7687", neo4j.BasicAuth("username", "password", "")); err != nil {
	return err // handle error
}
// handle driver lifetime based on your application lifetime requirements
// driver's lifetime is usually bound by the application lifetime, which usually implies one driver instance per application
defer driver.Close()

if session, err = driver.Session(neo4j.AccessModeWrite); err != nil {
	return err
}
defer session.Close() 

result, err = session.Run("CREATE (n:Item { id: $id, name: $name }) RETURN n.id, n.name", &map[string]interface{}{
	"id": 1,
	"name": "Item 1",
})
if err != nil {
	return err // handle error
}

for result.Next() {
	fmt.Printf("Created Item with Id = '%d' and Name = '%s'\n", result.Record().GetByIndex(0).(int64), result.Record().GetByIndex(1).(string))
}
if err = result.Err(); err != nil {
	return err // handle error
}
```

There are a few points that need to be highlighted:
* Each `Driver` instance maintains a pool of connections inside, as a result, it is recommended to only use **one driver per application**.
* It is considerably cheap to create new sessions and transactions, as sessions and transactions do not create new connections as long as there are free connections available in the connection pool.
* The driver is thread-safe, while the session or the transaction is not thread-safe.

### Parsing Result Values
#### Record Stream
A cypher execution result is comprised of a stream of records followed by a result summary.
The records inside the result can be accessed via `Next()`/`Record()` functions defined on `Result`. It is important to check `Err()` after `Next()` returning `false` to find out whether it is end of result stream or an error that caused the end of result consumption.

#### Accessing Values in a Record
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

#### Value Types
The driver currently exposes values in the record as an `interface{}` type. 
The underlying types of the returned values depend on the corresponding Cypher types.

The mapping between Cypher types and the types used by this driver (to represent the Cypher type):

| Cypher Type | Driver Type
| ---: | :--- |
| *null* | null |
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

#### Spatial Types - Point

| Cypher Type | Driver Type
| ---: | :--- |
| Point| neo4j.Point |

The temporal types are introduced in Neo4j 3.4 series.

You can create a 2-dimensional `Point` value using;

```go
point := NewPoint(srId, 1.0, 2.0)
```

or a 3-dimensional `Point` value using;

```go
point := NewPoint3D(srId, 1.0, 2.0, 3.0)
```

NOTE:
* For a list of supported `srId` values, please refer to the docs [here](https://neo4j.com/docs/developer-manual/current/cypher/functions/spatial/).

#### Temporal Types - Date and Time

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
dateValue := DateOf(time.Date(2005, time.December, 16, 0, 0, 0, 0, time.Local)`
```

Converting a custom temporal value into `time.Time` (all `neo4j` temporal types expose `Time()` function to gets its corresponding `time.Time` value):
```go
dateValueAsTime := dateValue.Time()
```

Note:
* When `neo4j.OffsetTime` is converted into `time.Time` or constructed through `OffsetTimeOf(time.Time)`, its `Location` is given a fixed name of `Offset` (i.e. assigned `time.FixedZone("Offset", offsetTime.offset)`).
* When `time.Time` values are sent/received through the driver, if its `Zone()` returns a name of `Offset` the value is stored with its offset value and with its zone name otherwise.
