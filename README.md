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
1. Develop SQL driver (**In progress**)
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

### How to use

See "_test.go" files for details. Examples will be added soon.

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
