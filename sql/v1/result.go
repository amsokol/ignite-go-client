package v1

import (
	"database/sql/driver"

	"github.com/amsokol/ignite-go-client/binary/errors"
)

type result struct {
	ra int64

	driver.Result
}

// LastInsertId returns the database's auto-generated ID
// after, for example, an INSERT into a table with primary
// key.
func (r *result) LastInsertId() (int64, error) {
	return 0, errors.Errorf("LastInsertId is not supported by Apache Ignite binary client v1.x")
}

// RowsAffected returns the number of rows affected by the
// query.
func (r *result) RowsAffected() (int64, error) {
	return r.ra, nil
}

// newResult creates new Result
func newResult(ra int64) (driver.Result, error) {
	return &result{ra: ra}, nil
}
