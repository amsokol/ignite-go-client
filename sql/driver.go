package ignitesql

import (
	"database/sql"
	"database/sql/driver"
	"net/url"
	"strconv"
	"strings"

	"github.com/Masterminds/semver"

	"github.com/amsokol/ignite-go-client/binary/errors"
	"github.com/amsokol/ignite-go-client/sql/common"
	"github.com/amsokol/ignite-go-client/sql/v1"
)

// Driver is exported to allow it to be used directly.
type Driver struct {
	driver.Driver
	// driver.DriverContext
}

// Open a Connection to the server.
func (d *Driver) Open(name string) (driver.Conn, error) {
	ci, err := d.parseURL(name)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to parse connection name")
	}
	switch ci.Major {
	case 1:
		return v1.Connect(ci)
	default:
		return nil, errors.Errorf("unsupported protocol version: v%d.%d.%d", ci.Major, ci.Minor, ci.Patch)
	}
}

// parseURL parses connection name
// url format: <protocol>://<host>:<port>/<cache>?param1=<value1>&param2=<value2>&paramN=<valueN>
//
// URL parts:
// | Name               | Mandatory | Description                                   | Default value                   |
// |--------------------|-----------|-----------------------------------------------|---------------------------------|
// | protocol           | no        | Connection protocol                           | tcp                             |
// | host               | no        | Apache Ignite Cluster host name or IP address | 127.0.0.1                       |
// | port               | no        | Max rows to return by query                   | 10800                           |
// | cache              | yes       | Cache name                                    |                                 |
//
// URL parameters (param1,...paramN):
// | Name               | Mandatory | Description                                                   | Default value                     |
// |--------------------|-----------|---------------------------------------------------------------|-----------------------------------|
// | schema             | no        | Database schema                                               | "" (PUBLIC schema will be used)   |
// | version            | no        | Binary protocol version in Semantic Version format            | 1.0.0                             |
// | page-size          | no        | Query cursor page size                                        | 10000                             |
// | max-rows           | no        | Max rows to return by query                                   | 0 (looks like it means unlimited) |
// | timeout            | no        | Timeout in milliseconds to execute query                      | 0 (disable timeout)               |
// | distributed-joins  | no        | Distributed joins (yes/no)                                    | no                                |
// | local-query        | no        | Local query (yes/no)                                          | no                                |
// | replicated-only    | no        | Whether query contains only replicated tables or not (yes/no) | no                                |
// | enforce-join-order | no        | Enforce join order (yes/no)                                   | no                                |
// | collocated         | no        | Whether your data is co-located or not (yes/no)               | no                                |
// | lazy-query         | no        | Lazy query execution (yes/no)                                 | no                                |
//
// url example: tcp://127.0.0.1:10800/?version=v1.0.0&page-size=100000
func (d *Driver) parseURL(name string) (common.ConnInfo, error) {
	var ci common.ConnInfo

	u, err := url.Parse(name)
	if err != nil {
		return ci, errors.Wrapf(err, "failed to parse connection name")
	}

	if ci.Network = u.Scheme; len(ci.Network) == 0 {
		ci.Network = "tcp"
	}

	if ci.Host = u.Hostname(); len(ci.Host) == 0 {
		ci.Host = "127.0.0.1"
	}
	ci.Port, _ = strconv.Atoi(u.Port())
	if ci.Port == 0 {
		ci.Port = 10800
	}

	ci.Cache = strings.Trim(u.Path, "/")

	// default values
	ver, _ := semver.NewVersion("1.0.0")
	ci.PageSize = 10000

	for k, v := range u.Query() {
		var val string
		if len(v) > 0 {
			val = strings.TrimSpace(v[0])
		}
		switch strings.ToLower(k) {
		case "schema":
			ci.Schema = val
		case "version":
			if len(val) > 0 {
				ver, err = semver.NewVersion(val)
			}
		case "page-size":
			if len(val) > 0 {
				ci.PageSize, err = strconv.Atoi(val)
			}
		case "max-rows":
			if len(val) > 0 {
				ci.MaxRows, err = strconv.Atoi(val)
			}
		case "timeout":
			if len(val) > 0 {
				ci.Timeout, err = strconv.ParseInt(val, 0, 64)
			}
		case "distributed-joins":
			ci.DistributedJoins, err = d.parseYesNo(val)
		case "local-query":
			ci.LocalQuery, err = d.parseYesNo(val)
		case "replicated-only":
			ci.ReplicatedOnly, err = d.parseYesNo(val)
		case "enforce-join-order":
			ci.EnforceJoinOrder, err = d.parseYesNo(val)
		case "collocated":
			ci.Collocated, err = d.parseYesNo(val)
		case "lazy-query":
			ci.LazyQuery, err = d.parseYesNo(val)
		default:
			return ci, errors.Errorf("unknown connection parameter \"%s\" with value \"%v\"", k, v)
		}
		if err != nil {
			return ci, errors.Wrapf(err, "unexpected parameter \"%s\" with value \"%s\"", k, val)
		}
	}

	ci.URL = name
	ci.ConnInfo.Major = int(ver.Major())
	ci.ConnInfo.Minor = int(ver.Minor())
	ci.ConnInfo.Patch = int(ver.Patch())

	return ci, nil
}

// parseYesNo parses boolean value (yes/no)
func (d *Driver) parseYesNo(s string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "yes":
		return true, nil
	case "no":
		return false, nil
	default:
		return false, errors.Errorf("invalid boolean value (expected \"yes\" or \"no\"): %s", s)
	}
}

// Init Initializes driver
func init() {
	sql.Register("ignite", &Driver{})
}
