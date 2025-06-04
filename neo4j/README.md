## Vector Type Support

Vectors are implemented as a generic type that can hold numeric values of different types.

### Using Vectors

```go
// Create vector
vec := neo4j.Vector[float64]{1.0, 2.0, 3.0}

// Use the vector in a query
result, _ := neo4j.ExecuteQuery[*neo4j.EagerResult](
    ctx,
    driver,
    "CREATE (n:Test {vec: $vec}) RETURN n.vec AS vec",
    map[string]any{"vec": vec},
    neo4j.EagerResultTransformer,
)

// Read back a vector from a result (returned as a List for now...)
record := result.Records[0]
rawVec := record.Values[0].([]any)
readVec := make(neo4j.Vector[float64], len(rawVec))
for i, v := range rawVec {
    readVec[i] = v.(float64)
}
```

### Supported Types

The Vector type supports the following numeric types:
- `float64`
- `float32`
- `int64`
- `int32`
- `int16`
- `int8`

### Note on Protocol Support

Currently, vectors are encoded as `LIST<Int64|Float64>` over the wire since the Bolt protocol doesn't yet support a native vector type. This will be updated in a future version of the driver when Bolt protocol support is added. 

### General Notes

- Numeric will need expanded to include more types (int, uint), which ones?
- Vector has it's own type now but Lists are still slices, design issue?