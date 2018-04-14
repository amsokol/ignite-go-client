package v1

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"runtime"

	"github.com/amsokol/ignite-go-client/binary/errors"
	"github.com/amsokol/ignite-go-client/binary/v1"
	"github.com/amsokol/ignite-go-client/debug"
)

// Rows is an iterator over an executed query's results.
type Rows interface {
	driver.Rows
}

type rows struct {
	conn     *conn
	response ignite.Response
	id       int64
	fields   []string
	rowsLeft int
}

// Columns returns the names of the columns. The number of
// columns of the result is inferred from the length of the
// slice. If a particular column name isn't known, an empty
// string should be returned for that entry.
func (r *rows) Columns() []string {
	return r.fields
}

// Close closes the rows iterator.
func (r *rows) Close() error {
	if r.rowsLeft > 0 {
		// to prevent resource leak on server try to close cursor
		r.rowsLeft = 0
		return r.conn.resourceClose(r.id)
	}
	return nil
}

// Next is called to populate the next row of data into
// the provided slice. The provided slice will be the same
// size as the Columns() are wide.
//
// Next should return io.EOF when there are no more rows.
//
// The dest should not be written to outside of Next. Care
// should be taken when closing Rows not to modify
// a buffer held in dest.
func (r *rows) Next(dest []driver.Value) error {
	var err error
	if r.rowsLeft == 0 {
		var hasMore bool
		if err = r.response.ReadPrimitives(&hasMore); err != nil {
			// prevent resource leak on server
			_ = r.Close()
			return errors.Wrapf(err, "failed to read more records flag")
		}
		if !hasMore {
			return io.EOF
		}
		if r.response, err = r.conn.QueryNexPageContext(context.Background(), r.id); err != nil {
			// prevent resource leak on server
			_ = r.Close()
			return errors.Wrapf(err, "failed to read cursor page")
		}
		// read data
		var rowCount int32
		if err = r.response.ReadPrimitives(&rowCount); err != nil {
			// prevent resource leak on server
			_ = r.Close()
			return errors.Wrapf(err, "failed to read row count")
		}
		r.rowsLeft = int(rowCount)
	}
	if len(r.fields) != len(dest) {
		return errors.Errorf("destination slice size must be %d but got %d", len(r.fields), len(dest))
	}
	for i := 0; i < len(r.fields); i++ {
		if dest[i], err = r.response.ReadObject(); err != nil {
			return fmt.Errorf("failed to read field value with index %d: %v", i, err)
		}
	}
	r.rowsLeft--
	return nil
}

// newRows creates new Rows object
func newRows(conn *conn, r ignite.Response) (driver.Rows, error) {
	// read field names
	var id int64
	var fieldCount int32
	if err := r.ReadPrimitives(&id, &fieldCount); err != nil {
		return nil, errors.Wrapf(err, "failed to read field count")
	}
	// response MUST return field names
	fields := make([]string, 0, fieldCount)
	for i := 0; i < int(fieldCount); i++ {
		var s string
		if err := r.ReadPrimitives(&s); err != nil {
			return nil, errors.Wrapf(err, "failed to read field name with index %d", i)
		}
		fields = append(fields, s)
	}

	// read row count
	var rowCount int32
	if err := r.ReadPrimitives(&rowCount); err != nil {
		return nil, errors.Wrapf(err, "failed to read row count")
	}

	rs := &rows{
		conn:     conn,
		response: r,
		id:       id,
		fields:   fields,
		rowsLeft: int(rowCount),
	}
	runtime.SetFinalizer(rs, rowsFinalizer)

	return rs, nil
}

// connFinalizer is memory leak spy
func rowsFinalizer(r *rows) {
	if r.rowsLeft > 0 {
		debug.ResourceLeakLogger.Printf("rows with cursor ID=\"%d\" is not closed", r.id)
		r.Close()
	}
}
