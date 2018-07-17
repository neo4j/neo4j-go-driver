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
2. Clone [seabolt](http://github.com/neo4j-drivers/seabolt) (assume `<seabolt_dir>` to point to the directory where the clone resides) and make sure you can build it successfully (follow it's own instructions)
3. Set environment variable `PKG_CONFIG_PATH` to be `$PKG_CONFIG_PATH:<seabolt_dir>/build`
4. Set environment variable `LD_LIBRARY_PATH` to include `<seabolt_dir>/build/bin`
5. Get this package via `go get github.com/neo4j/neo4j-go-driver`

### MacOS

1. Install pkg-config via `brew install pkg-config`
2. Clone [seabolt](http://github.com/neo4j-drivers/seabolt) (assume `<seabolt_dir>` to point to the directory where the clone resides) and make sure you can build it successfully,
3. Define environment variable `PKG_CONFIG_PATH` to include `build` subdirectory of seabolt, i.e. `$PKG_CONFIG_PATH:<seabolt_dir>/build`
4. Set environment variable `LD_LIBRARY_PATH` to include `<seabolt_dir>/build/bin`
5. Go Get this package via `go get github.com/neo4j/neo4j-go-driver`

### Windows

1. Install a mingw toolchain (for instance MSYS2 from https://www.msys2.org/) for cgo support (seabolt include some instructions),
2. Clone [seabolt](http://github.com/neo4j-drivers/seabolt) (assume `<seabolt_dir>` to point to the directory where the clone resides) and make sure you can build it successfully,
3. Define environment variable `PKG_CONFIG_PATH` to include `build` subdirectory of seabolt, i.e. `%PKG_CONFIG_PATH%;<seabolt_dir>/build`
4. Set environment variable `PATH` to include `<seabolt_dir>/build/bin`
5. Go Get this package via `go get github.com/neo4j/neo4j-go-driver`

## Versioning

Although `master` branch contains the source code for the latest available release, we are also tagging releases after semantic versioning scheme so that `dep add github.com/neo4j/neo4j-go-driver` will find latest release and add the driver as a dependency on your project.

## Getting the Driver

Add the driver as a dependency with `dep`.

```
dep add github.com/neo4j/neo4j-go-driver
```

## Minimum Viable Snippet

Connect, execute a statement and handle results

```go
var (
	driver neo4j.Driver
	session neo4j.Session
	result neo4j.Result
	err error
)

if driver, err = neo4j.NewDriver("bolt://localhost:7687", BasicAuth("username", "password", "")); err != nil {
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
| Boolean| boolean |
| Integer| int64 |
| Float| float |
| String| string |
| ByteArray| []byte |
| Node| Node |
| Relationship| Relationship |
| Path| _not supported yet_ |

