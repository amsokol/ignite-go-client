// +build go1.10

package ignitesql

import (
	"context"
	"database/sql/driver"

	"github.com/amsokol/ignite-go-client/binary/errors"
	"github.com/amsokol/ignite-go-client/sql/common"
	"github.com/amsokol/ignite-go-client/sql/v1"
)

type connector struct {
	info common.ConnInfo

	driver.Connector
}

// Connect returns a connection to the database.
// Connect may return a cached connection (one previously
// closed), but doing so is unnecessary; the sql package
// maintains a pool of idle connections for efficient re-use.
//
// The provided context.Context is for dialing purposes only
// (see net.DialContext) and should not be stored or used for
// other purposes.
//
// The returned connection is only used by one goroutine at a
// time.
func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	switch c.info.Version.Major() {
	case 1:
		return v1.Connect(ctx, c.info)
	default:
		return nil, errors.Errorf("unsupported protocol version: %v", c.info.Version)
	}
}

// Driver returns the underlying Driver of the Connector,
// mainly to maintain compatibility with the Driver method
// on sql.DB.
func (c *connector) Driver() driver.Driver {
	return &Driver{}
}
