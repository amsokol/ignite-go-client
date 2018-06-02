# ignite-go-client

## Apache Ignite (GridGain) v2.4+ client for Go programming language

This library is production ready.

Version is less than v1.0 because not all functionality is implemented yet (see [Road map](#road-map) for details). But the implemented functionality is production ready.

### Requirements

- Apache Ignite v2.4+ (because of binary communication protocol is used)
- go v1.9+

### Road map

Project status:

1. Develop "[Cache Configuration](https://apacheignite.readme.io/docs/binary-client-protocol-cache-configuration-operations)" methods (Completed)
1. Develop "[Key-Value Queries](https://apacheignite.readme.io/docs/binary-client-protocol-key-value-operations)" methods (Completed*)
1. Develop "[SQL and Scan Queries](https://apacheignite.readme.io/docs/binary-client-protocol-sql-operations)" methods (Completed)
1. Develop SQL driver (Completed)
1. Develop "[Binary Types](https://apacheignite.readme.io/docs/binary-client-protocol-binary-type-operations)" methods (Not started)

*Not all types are supported. See **[type mapping](#type-mapping)** for detail.

### How to install

```shell
go get -u github.com/amsokol/ignite-go-client
```

or use go [dep](https://golang.github.io/dep/) tool:

```shell
dep ensure -add github.com/amsokol/ignite-go-client
```

### How to use client

Import client package:

```go
import (
    "github.com/amsokol/ignite-go-client/binary/v1"
)
```

Connect to server:

```go
ctx := context.Background()

// connect
c, err := ignite.Connect(ctx, ignite.ConnInfo{
    Network: "tcp",
    Host:    "localhost",
    Port:    10800,
    Major:   1,
    Minor:   0,
    Patch:   0,
    Dialer: net.Dialer{
        Timeout: 10 * time.Second,
    },
})
if err != nil {
    t.Fatalf("failed connect to server: %v", err)
}
defer c.Close()

```

See [example of Key-Value Queries](https://github.com/amsokol/ignite-go-client/blob/master/examples_test.go#L98) for more.

See [example of SQL Queries](https://github.com/amsokol/ignite-go-client/blob/master/examples_test.go#L162) for more.

See ["_test.go" files](https://github.com/amsokol/ignite-go-client/tree/master/binary/v1) for other examples.

### How to use SQL driver

Import SQL driver:

```go
import (
    "database/sql"

    _ "github.com/amsokol/ignite-go-client/sql"
)
```

Connect to server:

```go
ctx := context.Background()

// open connection
db, err := sql.Open("ignite", "tcp://localhost:10800/ExampleDB?version=1.0.0&&page-size=10000&timeout=5000")
if err != nil {
    t.Fatalf("failed to open connection: %v", err)
}
defer db.Close()

```

See [example](https://github.com/amsokol/ignite-go-client/blob/master/examples_test.go#L15) for more.

Connection URL format:

```bash
protocol://host:port/cache?param1=value1&param2=value2&paramN=valueN
```

**URL parts:**

| Name               | Mandatory | Description                                   | Default value                   |
|--------------------|-----------|-----------------------------------------------|---------------------------------|
| protocol           | no        | Connection protocol                           | tcp                             |
| host               | no        | Apache Ignite Cluster host name or IP address | 127.0.0.1                       |
| port               | no        | Max rows to return by query                   | 10800                           |
| cache              | yes       | Cache name                                    |                                 |

**URL parameters (param1,...paramN):**

| Name               | Mandatory | Description                                                   | Default value                     |
|--------------------|-----------|---------------------------------------------------------------|-----------------------------------|
| schema             | no        | Database schema                                               | "" (PUBLIC schema is used)   |
| version            | no        | Binary protocol version in Semantic Version format            | 1.0.0                             |
| page-size          | no        | Query cursor page size                                        | 10000                             |
| max-rows           | no        | Max rows to return by query                                   | 0 (looks like it means unlimited) |
| timeout            | no        | Timeout in milliseconds to execute query                      | 0 (disable timeout)               |
| distributed-joins  | no        | Distributed joins (yes/no)                                    | no                                |
| local-query        | no        | Local query (yes/no)                                          | no                                |
| replicated-only    | no        | Whether query contains only replicated tables or not (yes/no) | no                                |
| enforce-join-order | no        | Enforce join order (yes/no)                                   | no                                |
| collocated         | no        | Whether your data is co-located or not (yes/no)               | no                                |
| lazy-query         | no        | Lazy query execution (yes/no)                                 | no                                |

### How to run tests

1. Download `Apache Ignite 2.4+` from [official site](https://ignite.apache.org/download.cgi#binaries)
1. Extract distributive to any folder
1. `cd` to `testdata` folder with `configuration-for-tests.xml` file
1. Start Ignite server with `configuration-for-tests.xml` configuration file:

```bash
# For Windows:
<path_with_ignite>\bin\ignite.bat .\configuration-for-tests.xml

# For Linux:
<path_with_ignite>/bin/ignite.sh ./configuration-for-tests.xml
```

1. Run tests into the root folder of this project:

```bash
go test ./...
```

### Type mapping

| Apache Ignite Type | Go language type                                                       |
|--------------------|------------------------------------------------------------------------|
| byte               | byte                                                                   |
| short              | int16                                                                  |
| int                | int32                                                                  |
| long               | int64                                                                  |
| float              | float32                                                                |
| double             | float64                                                                |
| char               | ignite.Char                                                            |
| bool               | bool                                                                   |
| String             | string                                                                 |
| UUID (Guid)        | uuid.UUID ([UUID library from Google](https://github.com/google/uuid)) |
| Date*              | ignite.Date / time.Time                                                |
| byte array         | []byte                                                                 |
| short array        | []int16                                                                |
| int array          | []int32                                                                |
| long array         | []int64                                                                |
| float array        | []float32                                                              |
| double array       | []float64                                                              |
| char array         | []ignite.Char                                                          |
| bool array         | []bool                                                                 |
| String array       | []string                                                               |
| UUID (Guid) array  | []uuid.UUID                                                            |
| Date array*        | []ignite.Date / []time.Time                                            |
| Object array       | Not supported. Need help.                                              |
| Collection         | Not supported. Need help.                                              |
| Map                | Not supported. Need help.                                              |
| Enum               | Not supported. Need help.                                              |
| Enum array         | Not supported. Need help.                                              |
| Decimal            | Not supported. Need help.                                              |
| Decimal array      | Not supported. Need help.                                              |
| Timestamp          | time.Time                                                              |
| Timestamp array    | []time.Time                                                            |
| Time**             | ignite.Time / time.Time                                                |
| Time array**       | []ignite.Time / []time.Time                                            |
| NULL               | nil                                                                    |
| Complex Object     | ignite.ComplexObject                                                   |

*`Date` is outdated type. It's recommended to use `Timestamp` type.
If you still need `Date` type use `ignite.ToDate()` function when you **put** date:

```go
t := time.Date(2018, 4, 3, 14, 25, 32, int(time.Millisecond*123), time.UTC)
err := c.CachePut("CacheGet", false, "Date", ignite.ToDate(t)) // ToDate() converts time.Time to ignite.Date
...

t, err = c.CacheGet("CacheGet", false, "Date") // 't' is time.Time, you don't need any converting
```

*`Time` is outdated type. It's recommended to use `Timestamp` type.
If you still need `Time` type use `ignite.ToTime()` function when you **put** time:

```go
t := time.Date(1, 1, 1, 14, 25, 32, int(time.Millisecond*123), time.UTC)
err := c.CachePut("CacheGet", false, "Time", ignite.ToTime(t)) // ToTime() converts time.Time to ignite.Time (year, month and day are ignored)
...

t, err = c.CacheGet("CacheGet", false, "Time") // 't' is time.Time (where year=1, month=1 and day=1), you don't need any converting
```

### Example how to use **Complex Object** type

```go
// put complex object
c1 := ignite.NewComplexObject("ComplexObject1")
c1.Set("field1", "value 1")
c1.Set("field2", int32(2))
c1.Set("field3", true)
c2 := ignite.NewComplexObject("ComplexObject2")
c2.Set("complexField1", c1)
if err := c.CachePut(cache, false, "key", c2); err != nil {
    return err
}
...

// get complex object
v, err := c.CacheGet(cache, false, "key")
if err != nil {
    return err
}
c2 = v.(ignite.ComplexObject)
log.Printf("key=\"%s\", value=\"%#v\"", "key", c2)
v, _ = c2.Get("complexField1")
c1 = v.(ignite.ComplexObject)
log.Printf("key=\"%s\", value=\"%#v\"", "complexField1", c1)
v, _ = c1.Get("field1")
log.Printf("key=\"%s\", value=\"%s\"", "field1", v)
v, _ = c1.Get("field2")
log.Printf("key=\"%s\", value=%d", "field2", v)
v, _ = c1.Get("field3")
log.Printf("key=\"%s\", value=%t", "field3", v)
```

### SQL and Scan Queries supported operations

| Operation                           | Status of implementation                                |
|-------------------------------------|---------------------------------------------------------|
| OP_QUERY_SQL                        | Done.                                                   |
| OP_QUERY_SQL_CURSOR_GET_PAGE        | Done.                                                   |
| OP_QUERY_SQL_FIELDS                 | Done.                                                   |
| OP_QUERY_SQL_FIELDS_CURSOR_GET_PAGE | Done.                                                   |
| OP_QUERY_SCAN                       | Done (without filter object support).                   |
| OP_QUERY_SCAN_CURSOR_GET_PAGE       | Done (without filter object support).                   |
| OP_RESOURCE_CLOSE                   | Done.                                                   |

### Error handling

In case of operation execution error you can get original status and error message from Apache Ignite server.\
Example:

```go
if err := client.CachePut("TestCache", false, "key", "value"); err != nil {
    // try to cast to *IgniteError type
    original, ok := err.(*IgniteError)
    if ok {
        // log Apache Ignite status and message
        log.Printf("[%d] %s", original.IgniteStatus, original.IgniteMessage)
    }
    return err
}
```
