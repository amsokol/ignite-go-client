package v1

import (
	"context"
	"database/sql/driver"

	"github.com/amsokol/ignite-go-client/binary/errors"
)

// SQL statement struct
type stmt struct {
	conn  *conn
	query string

	driver.Stmt
	driver.StmtExecContext
	driver.StmtQueryContext
}

// <driver.Stmt>

// Close closes the statement.
//
// As of Go 1.1, a Stmt will not be closed if it's in use
// by any queries.
func (s *stmt) Close() error {
	return nil
}

// NumInput returns the number of placeholder parameters.
//
// If NumInput returns >= 0, the sql package will sanity check
// argument counts from callers and return errors to the caller
// before the statement's Exec or Query methods are called.
//
// NumInput may also return -1, if the driver doesn't know
// its number of placeholders. In that case, the sql package
// will not sanity check Exec or Query argument counts.
func (s *stmt) NumInput() int {
	return -1
}

// Exec executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
//
// Deprecated: Drivers should implement StmtExecContext instead (or additionally).
func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	// TODO: implement wrapper
	return nil, errors.Errorf("Deprecated: Drivers should implement StmtExecContext instead (or additionally)")
}

// Query executes a query that may return rows, such as a
// SELECT.
//
// Deprecated: Drivers should implement StmtQueryContext instead (or additionally).
func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	// TODO: implement wrapper
	return nil, errors.Errorf("Deprecated: Drivers should implement StmtQueryContext instead (or additionally)")
}

// </driver.Stmt>

// <StmtExecContext>

// ExecContext executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
//
// ExecContext must honor the context timeout and return when it is canceled.
func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return s.conn.ExecContext(ctx, s.query, args)
}

// </StmtExecContext>

// <StmtQueryContext>

// QueryContext executes a query that may return rows, such as a
// SELECT.
//
// QueryContext must honor the context timeout and return when it is canceled.
func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return s.conn.QueryContext(ctx, s.query, args)
}

// </StmtQueryContext>

// NewStmt creates new Stmt object
func newStmt(conn *conn, query string) driver.Stmt {
	return &stmt{conn: conn, query: query}
}
