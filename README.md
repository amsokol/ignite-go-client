# ignite-go-client

## Apache Ignite (GridGain) v2.4+ client for Go programming language

### Requirements

- Apache Ignite v2.4+ (because of binary communication protocol is used)
- go v1.9+

### Road map

Project is in active development:

1. Develop "[Cache Configuration](https://apacheignite.readme.io/docs/binary-client-protocol-cache-configuration-operations)" methods (Completed)
1. Develop "[Key-Value Queries](https://apacheignite.readme.io/docs/binary-client-protocol-key-value-operations)" methods (Completed*)
1. Develop "[SQL and Scan Queries](https://apacheignite.readme.io/docs/binary-client-protocol-sql-operations)" methods (Completed**)
1. Develop SQL driver (Completed)
1. Develop "[Binary Types](https://apacheignite.readme.io/docs/binary-client-protocol-binary-type-operations)" methods (Not started)

*Not all types are supported. See **[type mapping](#type-mapping)** for detail.\
**Not all operations are supported. See **[table](#sql-and-scan-queries-supported-operations)** for detail.

### How to install

```shell
go get -u github.com/amsokol/ignite-go-client
```

or use go [dep](https://golang.github.io/dep/) tool:

```shell
dep ensure -add github.com/amsokol/ignite-go-client
```

### How to use client

See "_test.go" files for details. Examples will be added soon.

### How to use SQL driver

Import driver SQL driver:

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
db, err := sql.Open("ignite", "tcp://localhost:10800/TestDB?version=1.0.0&&page-size=10000&timeout=5000")
if err != nil {
    t.Fatalf("failed to open connection: %v", err)
}
defer db.Close()

```

See [example](https://github.com/amsokol/ignite-go-client/blob/master/examples_test.go) for more.

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
| schema             | no        | Database schema                                               | "" (PUBLIC schema will be used) |
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
1. `cd` to `test-data` folder with `configuration-for-tests.xml` file
1. Start Ignite server with `configuration-for-tests.xml` configuration file:

```bash
# For Windows:
<path_with_ignite>\bin\ignite.bat .\configuration-for-tests.xml

# For Linux:
<path_with_ignite>/bin/ignite.sh ./configuration-for-tests.xml
```

1. Run tests into the root folder of this project:

```shell
# go test ./...
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
| date*              | ignite.Date                                                            |
| byte array         | []byte                                                                 |
| short array        | []int16                                                                |
| int array          | []int32                                                                |
| long array         | []int64                                                                |
| float array        | []float32                                                              |
| double array       | []float64                                                              |
| char array         | []ignite.Char                                                          |
| bool array         | []bool                                                                 |
| String array       | []string                                                               |
| UUID (Guid) array  | Not supported. Need help from Apache Ignite team.                      |
| Date array         | Not supported. Need help from Apache Ignite team.                      |
| Object array       | Not supported. Need help.                                              |
| Collection         | Not supported. Need help.                                              |
| Map                | Not supported. Need help.                                              |
| Enum               | Not supported. Need help.                                              |
| Enum array         | Not supported. Need help.                                              |
| Decimal            | Not supported. Need help.                                              |
| Decimal array      | Not supported. Need help.                                              |
| Timestamp          | time.Time                                                              |
| Timestamp array    | Not supported. Need help.                                              |
| Time**             | ignite.Time                                                            |
| Time array         | Not supported. Need help.                                              |
| NULL               | nil                                                                    |

*`date` is outdated type. It's recommended to use `Timestamp` type.
If you still need `date` type use `ignite.NativeTime2Date` and `ignite.Date2NativeTime` functions to convert between Golang `time.Time` and `ignite.Date` types.

**`Time` is outdated type. It's recommended to use `Timestamp` type.
If you still need `Time` type use `ignite.NativeTime2Date` and `ignite.Date2NativeTime` functions to convert between Golang `time.Time` and `ignite.Time` types.

### SQL and Scan Queries supported operations

| Operation                           | Status of implementation                                |
|-------------------------------------|---------------------------------------------------------|
| OP_QUERY_SQL                        | Done without unit test. Need help to develop unit test. |
| OP_QUERY_SQL_CURSOR_GET_PAGE        | Done without unit test. Need help to develop unit test. |
| OP_QUERY_SQL_FIELDS                 | Done.                                                   |
| OP_QUERY_SQL_FIELDS_CURSOR_GET_PAGE | Done.                                                   |
| OP_QUERY_SCAN                       | Not started. Need help.                                 |
| OP_QUERY_SCAN_CURSOR_GET_PAGE       | Not started. Need help.                                 |
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
