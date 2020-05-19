# neo4j-go-driver

This is the the official Neo4j Go Driver. It is built on our C Connector, [seabolt](https://github.com/neo4j-drivers/seabolt) and depends on it to be available on the host system.

## Upcoming pure Go version
There will be a new version of the driver that doesn't rely on cgo. This version is in beta on branch 1.8 and will be replacing the 1.7 cgo based version soon. 

##  Requirements

This package requires the following tools/libraries to be installed in order to be built.

1. C Compiler Toolchain  
2. seabolt
3. pkg-config tool for your platform

## Requirements

### Compiler Toolchain

Install C/C++ compiler toolchain supported by your operating system. You'll need a mingw toolchain (for instance MSYS2 from https://www.msys2.org/) for cgo support (seabolt includes [instructions](https://github.com/neo4j-drivers/seabolt)) on Windows.

### Pkg-Config

Linux distributions provide `pkg-config` as part of their package management systems (`apt install pkg-config` or `yum install pkgconfig`).

MacOS doesn't provide `pkg-config`, so have it installed through `brew install pkg-config`.

Windows doesn't provide `pkg-config`, so have it installed by following [these instructions](https://stackoverflow.com/questions/1710922/how-to-install-pkg-config-in-windows?answertab=active#tab-top), make the `bin` folder available in PATH before any MSYS2 PATH entries. *This is mandatory because `pkg-config` that ships with MSYS2 has some path handling problems*

### Seabolt

[Seabolt](http://github.com/neo4j-drivers/seabolt) is the C Connector Library for Neo4j and is used by the Go driver as the base layer.
Seabolt can either be installed from binaries or from source.

#### Binaries

We're provide _**experimental**_ binaries for `Linux`, `MacOS` and `Windows` [here](https://github.com/neo4j-drivers/seabolt/releases). Please remember that `OpenSSL` is a required dependency for `Linux` and `MacOS` - TLS on `Windows` depends on native SCHANNEL provider and doesn't require any additional third-party dependencies from version 1.7.3 onwards. 

Download the binary matching your system and extract the package using the following command (assuming that you saved the package as ~/Downloads/seabolt.tar.gz).
The command actually skips the top-level folder inside the tar.gz package and extracts its contents into the root folder `/`.:

```
tar zxvf ~/Downloads/seabolt.tar.gz --strip-components=1 -C /
```

#### Source

Clone [seabolt](http://github.com/neo4j-drivers/seabolt) (assume `<seabolt_dir>` to be the absolute path in which the clone resides) and build it using one the provided `make_debug.[sh|cmd]` or `make_release.[sh|cmd]` scripts. The scripts set the installation path to `<seabolt_dir>/build/dist` and `<seabolt_install_dir>` in the following steps refer to it.

Set `PKG_CONFIG_PATH` to `<seabolt_install_dir>/share/pkgconfig` on Linux/MacOS or to `<seabolt_install_dir>` on Windows.

Set `LD_LIBRARY_PATH`/`DYLD_LIBRARY_PATH` to include `<seabolt_install_dir>/lib` on Linux/MacOS respectively. This is not necessary if you will link statically to seabolt or use `RPATH` feature on your platform (see below).

Add `<seabolt_install_dir>\bin` to `PATH` environment variable on Windows.

There's a 3rd party tap for [brew](https://brew.sh) that you can use to download, compile and install Seabolt into the correct place on macOS: [homebrew-seabolt/seabolt](https://github.com/michael-simons/homebrew-seabolt):

```
brew install michael-simons/homebrew-seabolt/seabolt
```

## Getting the Driver

### With `dep`

Add the driver as a dependency with `dep ensure -add github.com/neo4j/neo4j-go-driver/neo4j`

### With `go get`

Add the driver with `go get github.com/neo4j/neo4j-go-driver/neo4j`

## Static Linking

We provide `seabolt_static` build tag to support static linking against seabolt and its dependencies. You can just pass `--tags seabolt_static` to your `go` toolset (like `go build --tags seabolt_static`) for your project and the generated binary will not have any runtime dependencies to `seabolt`.

## Setting RPATH on Linux/MacOS

Both Linux and MacOS dynamic loader support `RPATH` entries into executables so that they can locate their dependent shared libraries based on those entries. To have a `RUNPATH` entry to be added to your executable, you can pass `-ldflags "-r $(pkg-config --variable=libdir seabolt17)"` to your `go` toolset (like `go build -ldflags "-r $(pkg-config --variable=libdir seabolt17)"`) and it will add an `RPATH` entry to your executable that points to the location where seabolt shared library resides.

## Minimum Viable Snippet

Connect, execute a statement and handle results

```go
var (
	driver neo4j.Driver
	session neo4j.Session
	result neo4j.Result
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

result, err = session.Run("CREATE (n:Item { id: $id, name: $name }) RETURN n.id, n.name", map[string]interface{}{
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
