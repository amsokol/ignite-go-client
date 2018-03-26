# ignite-go-client

## Apache Ignite (GridGain) v2.4+ client for Go programming language.

### Requirements

- Apache Ignite v2.4+ (because of binary communication protocol is used)
- go v1.10+

### Road map

Project is in active development:

1. Develop "[Cache Configuration](https://apacheignite.readme.io/docs/binary-client-protocol-cache-configuration-operations)" methods (Completed)
2. Develop "[Key-Value Queries](https://apacheignite.readme.io/docs/binary-client-protocol-key-value-operations)" methods (In progress)
3. Develop "[Binary Types](https://apacheignite.readme.io/docs/binary-client-protocol-binary-type-operations)" methods (Not started)
4. Develop "[SQL and Scan Queries](https://apacheignite.readme.io/docs/binary-client-protocol-sql-operations)" methods (Not started)
5. Develop SQL driver (Not started)

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