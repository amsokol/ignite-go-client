# ignite-go-client

## Apache Ignite (GridGain) v2.4+ client for Go programming language

### Requirements

- Apache Ignite v2.4+ (because of binary communication protocol is used)
- go v1.9+

### Road map

Project is in active development:

1. Develop "[Cache Configuration](https://apacheignite.readme.io/docs/binary-client-protocol-cache-configuration-operations)" methods (Completed)
1. Develop "[Key-Value Queries](https://apacheignite.readme.io/docs/binary-client-protocol-key-value-operations)" methods (Completed*)
1. Develop "[SQL and Scan Queries](https://apacheignite.readme.io/docs/binary-client-protocol-sql-operations)" methods (In progress)
1. Develop SQL driver (Not started)
1. Develop "[Binary Types](https://apacheignite.readme.io/docs/binary-client-protocol-binary-type-operations)" methods (Not started)

*Not all types are supported. See [type mapping](#type-mapping) for detail.

### How to install

```bash
# go get -u github.com/amsokol/ignite-go-client
```

or use go [dep](https://golang.github.io/dep/) tool:

```bash
# dep ensure -add github.com/amsokol/ignite-go-client
```

### How to use

See "_test.go" files for details. Examples will be added soon.

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
| date               | ignite.Date                                                            |
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
| Time               | Not supported. Need help.                                              |
| Time array         | Not supported. Need help.                                              |
| NULL               | nil                                                                    |
