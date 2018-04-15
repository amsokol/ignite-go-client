// +build go1.10

package ignitesql

import (
	"database/sql/driver"

	"github.com/amsokol/ignite-go-client/binary/errors"
)

// OpenConnector must parse the name in the same format that Driver.
// Open parses the name parameter.
func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	ci, err := d.parseURL(name)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to parse connection name")
	}
	return &connector{info: ci}, nil
}
