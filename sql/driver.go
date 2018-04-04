package ignitesql

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/Masterminds/semver"

	"github.com/amsokol/ignite-go-client/sql/common"
)

// Driver is exported to allow it to be used directly.
type Driver struct{}

// Open a Connection to the server.
// url format: <protocol>://<host>:<port>/<schema>?param1=<value1>&param2=<value2>&paramN=<valueN>
//
// URL parts:
// | Name               | Mandatory | Description                                   | Default value                   |
// |--------------------|-----------|-----------------------------------------------|---------------------------------|
// | protocol           | no        | Connection protocol                           | tcp                             |
// | host               | no        | Apache Ignite Cluster host name or IP address | 127.0.0.1                       |
// | port               | no        | Max rows to return by query                   | 10800                           |
// | schema             | no        | Database schema                               | "" (PUBLIC schema will be used) |
//
// URL parameters (param1,...paramN):
// | Name               | Mandatory | Description                                                   | Default value                     |
// |--------------------|-----------|---------------------------------------------------------------|-----------------------------------|
// | version            | no        | Binary protocol version in Semantic Version format            | 1.0.0                             |
// | page-size          | no        | Query cursor page size                                        | 10000                             |
// | max-rows           | no        | Max rows to return by query                                   | 0 (looks like it means unlimited) |
// | timeout            | no        | Timeout to execute query                                      | 0 (disable timeout)               |
// | distributed-joins  | no        | Distributed joins (yes/no)                                    | no                                |
// | local-query        | no        | Local query (yes/no)                                          | no                                |
// | replicated-only    | no        | Whether query contains only replicated tables or not (yes/no) | no                                |
// | enforce-join-order | no        | Enforce join order (yes/no)                                   | no                                |
// | collocated         | no        | Whether your data is co-located or not (yes/no)               | no                                |
// | lazy-query         | no        | Lazy query execution (yes/no)                                 | no                                |
//
// url example: tcp://127.0.0.1:10800/?version=v1.0.0&page-size=100000
func (d *Driver) Open(name string) (driver.Conn, error) {
	u, err := url.Parse(name)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection name: %v", err)
	}

	var ci common.ConnectionInfo

	if ci.Network = u.Scheme; len(ci.Network) == 0 {
		ci.Network = "tcp"
	}

	if ci.Address = u.Hostname(); len(ci.Address) == 0 {
		ci.Address = "127.0.0.1"
	}
	if len(u.Port()) == 0 {
		ci.Address += ":10800"
	} else {
		ci.Address += ":" + u.Port()
	}

	ci.Schema = strings.Trim(u.Path, "/")

	for k, v := range u.Query() {
		var val string
		if len(v) > 0 {
			val = strings.TrimSpace(v[0])
		}
		switch strings.ToLower(k) {
		case "version":
			if len(val) == 0 {
				val = "1.0.0"
			}
			ci.Version, err = semver.NewVersion(val)
		case "page-size":
			if len(val) == 0 {
				val = "10000"
			}
			ci.PageSize, err = strconv.Atoi(val)
		case "max-rows":
			if len(val) == 0 {
				val = "0"
			}
			ci.MaxRows, err = strconv.Atoi(val)
		case "timeout":
			if len(val) == 0 {
				val = "0"
			}
			ci.Timeout, err = strconv.ParseInt(val, 0, 64)
		case "distributed-joins":
			ci.DistributedJoins, err = parseYesNo(val)
		case "local-query":
			ci.LocalQuery, err = parseYesNo(val)
		case "replicated-only":
			ci.ReplicatedOnly, err = parseYesNo(val)
		case "enforce-join-order":
			ci.EnforceJoinOrder, err = parseYesNo(val)
		case "collocated":
			ci.Collocated, err = parseYesNo(val)
		case "lazy-query":
			ci.LazyQuery, err = parseYesNo(val)
		default:
			return nil, fmt.Errorf("unknown connection parameter \"%s\" with value \"%v\"", k, v)
		}
		if err != nil {
			return nil, fmt.Errorf("unexpected parameter \"%s\" with value \"%s\": %v", k, val, err)
		}
	}

	return nil, fmt.Errorf("not implemented")
}

func parseYesNo(s string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "yes":
		return true, nil
	case "no":
		return false, nil
	default:
		return false, fmt.Errorf("invalid boolean value (expected \"yes\" or \"no\"): %s", s)
	}
}

// Init Initializes driver
func init() {
	sql.Register("ignite", &Driver{})
}
