package ignite

import (
	"github.com/amsokol/ignite-go-client/binary/errors"
)

const (
	// StatementTypeAny is ANY = 0
	StatementTypeAny byte = 0

	// StatementTypeSelect is SELECT = 1
	StatementTypeSelect byte = 1

	// StatementTypeUpdate is UPDATE = 2
	StatementTypeUpdate byte = 2
)

// QuerySQLData input parameter for QuerySQL func
type QuerySQLData struct {
	// Name of a type or SQL table.
	Table string

	// SQL query string.
	Query string

	// Query arguments.
	QueryArgs []interface{}

	// Distributed joins.
	DistributedJoins bool

	// Local query.
	LocalQuery bool

	// Replicated only - Whether query contains only replicated tables or not.
	ReplicatedOnly bool

	// Cursor page size.
	PageSize int

	// Timeout(milliseconds) value should be non-negative. Zero value disables timeout.
	Timeout int64
}

// QuerySQLPage is query result page
type QuerySQLPage struct {
	// Keys (field names)
	Keys []interface{}

	// Values
	Values []interface{}

	// Indicates whether more results are available to be fetched with QuerySQLCursorGetPage.
	// When true, query cursor is closed automatically.
	HasMore bool
}

// QuerySQLResult output from QuerySQL func
type QuerySQLResult struct {
	// Cursor id. Can be closed with ResourceClose func.
	ID int64

	// Query result first page
	QuerySQLPage
}

// QuerySQLFieldsData input parameter for QuerySQLFields func
type QuerySQLFieldsData struct {
	// Schema for the query; can be empty, in which case default PUBLIC schema will be used.
	Schema string

	// Query cursor page size.
	PageSize int

	// Max rows.
	MaxRows int

	// SQL query string.
	Query string

	// Query arguments.
	QueryArgs []interface{}

	// Statement type.
	// ANY = 0
	// SELECT = 1
	// UPDATE = 2
	StatementType byte

	// Distributed joins.
	DistributedJoins bool

	// Local query.
	LocalQuery bool

	// Replicated only - Whether query contains only replicated tables or not.
	ReplicatedOnly bool

	// Enforce join order.
	EnforceJoinOrder bool

	// Collocated - Whether your data is co-located or not.
	Collocated bool

	// Lazy query execution.
	LazyQuery bool

	// Timeout(milliseconds) value should be non-negative. Zero value disables timeout.
	Timeout int64

	// Include field names.
	IncludeFieldNames bool
}

// QuerySQLFieldsPage is query result page
type QuerySQLFieldsPage struct {
	// Values
	Rows [][]interface{}

	// Indicates whether more results are available to be fetched with QuerySQLFieldsCursorGetPage.
	// When true, query cursor is closed automatically.
	HasMore bool
}

// QuerySQLFieldsResult output from QuerySQLFields func
type QuerySQLFieldsResult struct {
	// Cursor id. Can be closed with ResourceClose func.
	ID int64

	// Field (column) count.
	FieldCount int

	// Needed only when IncludeFieldNames is true in the request.
	// Column names.
	Fields []string

	// Query result first page
	QuerySQLFieldsPage
}

func (c *client) QuerySQL(cache string, binary bool, data QuerySQLData) (QuerySQLResult, error) {
	o := c.Prepare(OpQuerySQL)

	var res QuerySQLResult

	if err := o.WritePrimitives(HashCode(cache), binary, data.Table, data.Query); err != nil {
		return res, errors.Wrapf(err, "failed to write request data")
	}

	var l int32
	if data.QueryArgs != nil {
		l = int32(len(data.QueryArgs))
	}
	// write args
	if err := o.WritePrimitives(l); err != nil {
		return res, errors.Wrapf(err, "failed to write query arg count")
	}
	if l > 0 {
		for i, v := range data.QueryArgs {
			if err := o.WriteObjects(v); err != nil {
				return res, errors.Wrapf(err, "failed to write query arg with index %d", i)
			}
		}
	}

	if err := o.WritePrimitives(data.DistributedJoins, data.LocalQuery, data.ReplicatedOnly,
		int32(data.PageSize), data.Timeout); err != nil {
		return res, errors.Wrapf(err, "failed to write request data")
	}

	// execute
	r, err := c.Call(o)
	if err != nil {
		return res, errors.Wrapf(err, "failed to execute OP_QUERY_SQL operation")
	}
	if err = r.CheckStatus(); err != nil {
		return res, err
	}

	var count int32
	if err = r.ReadPrimitives(&res.ID, &count); err != nil {
		return res, errors.Wrapf(err, "failed to read response data")
	}
	res.Keys = make([]interface{}, 0, int(count))
	res.Values = make([]interface{}, 0, int(count))
	// read data
	for i := 0; i < int(count); i++ {
		pair, err := r.ReadObjects(2)
		if err != nil {
			return res, errors.Wrapf(err, "failed to read pair with index %d", i)
		}
		res.Keys = append(res.Keys, pair[0])
		res.Values = append(res.Values, pair[1])
	}
	if err = r.ReadPrimitives(&res.HasMore); err != nil {
		return res, errors.Wrapf(err, "failed to read response data")
	}
	return res, nil
}

// QuerySQLCursorGetPage retrieves the next SQL query cursor page by cursor id from QuerySQL.
func (c *client) QuerySQLCursorGetPage(id int64) (QuerySQLPage, error) {
	var res QuerySQLPage

	r, err := c.Exec(OpQuerySQLCursorGetPage, id)
	if err != nil {
		return res, errors.Wrapf(err, "failed to execute OP_QUERY_SQL_CURSOR_GET_PAGE operation")
	}
	if err = r.CheckStatus(); err != nil {
		return res, err
	}

	// read data
	var count int32
	if err = r.ReadPrimitives(&count); err != nil {
		return res, errors.Wrapf(err, "failed to read response data")
	}
	res.Keys = make([]interface{}, 0, int(count))
	res.Values = make([]interface{}, 0, int(count))
	// read data
	for i := 0; i < int(count); i++ {
		pair, err := r.ReadObjects(2)
		if err != nil {
			return res, errors.Wrapf(err, "failed to read pair with index %d", i)
		}
		res.Keys = append(res.Keys, pair[0])
		res.Values = append(res.Values, pair[1])
	}
	if err = r.ReadPrimitives(&res.HasMore); err != nil {
		return res, errors.Wrapf(err, "failed to read response data")
	}

	return res, nil
}

// QuerySQLFields performs SQL fields query.
func (c *client) QuerySQLFields(cache string, binary bool, data QuerySQLFieldsData) (QuerySQLFieldsResult, error) {
	var res QuerySQLFieldsResult

	r, err := c.QuerySQLFieldsRaw(cache, binary, data)
	if err != nil {
		return res, errors.Wrapf(err, "failed to execute OP_QUERY_SQL_FIELDS operation")
	}

	// read field names
	var fieldCount int32
	if err = r.ReadPrimitives(&res.ID, &fieldCount); err != nil {
		return res, errors.Wrapf(err, "failed to read field count")
	}
	res.FieldCount = int(fieldCount)
	if data.IncludeFieldNames {
		res.Fields = make([]string, 0, res.FieldCount)
		for i := 0; i < res.FieldCount; i++ {
			var s string
			if err = r.ReadPrimitives(&s); err != nil {
				return res, errors.Wrapf(err, "failed to read field name with index %d", i)
			}
			res.Fields = append(res.Fields, s)
		}
	} else {
		res.Fields = []string{}
	}

	// read data
	var rowCount int32
	if err = r.ReadPrimitives(&rowCount); err != nil {
		return res, errors.Wrapf(err, "failed to read row count")
	}
	res.Rows = make([][]interface{}, rowCount)
	for i := 0; i < int(rowCount); i++ {
		res.Rows[i], err = r.ReadObjects(res.FieldCount)
		if err != nil {
			return res, errors.Wrapf(err, "failed to read row with index %d", i)
		}
	}
	if err = r.ReadPrimitives(&res.HasMore); err != nil {
		return res, errors.Wrapf(err, "failed to read response data")
	}

	return res, nil
}

func (c *client) QuerySQLFieldsRaw(cache string, binary bool, data QuerySQLFieldsData) (Response, error) {
	o := c.Prepare(OpQuerySQLFields)

	var r Response

	if err := o.WritePrimitives(HashCode(cache), binary); err != nil {
		return r, errors.Wrapf(err, "failed to write request data")
	}

	if len(data.Schema) > 0 {
		if err := o.WriteObjects(data.Schema); err != nil {
			return r, errors.Wrapf(err, "failed to write schema for the query")
		}
	} else {
		if err := o.WriteObjects(nil); err != nil {
			return r, errors.Wrapf(err, "failed to write nil for schema for the query")
		}
	}

	if err := o.WritePrimitives(int32(data.PageSize), int32(data.MaxRows), data.Query); err != nil {
		return r, errors.Wrapf(err, "failed to write request data")
	}

	var l int32
	if data.QueryArgs != nil {
		l = int32(len(data.QueryArgs))
	}
	// write args
	if err := o.WritePrimitives(l); err != nil {
		return r, errors.Wrapf(err, "failed to write query arg count")
	}
	if l > 0 {
		for i, v := range data.QueryArgs {
			if err := o.WriteObjects(v); err != nil {
				return r, errors.Wrapf(err, "failed to write query arg with index %d", i)
			}
		}
	}

	if err := o.WritePrimitives(data.StatementType, data.DistributedJoins, data.LocalQuery, data.ReplicatedOnly,
		data.EnforceJoinOrder, data.Collocated, data.LazyQuery, data.Timeout, data.IncludeFieldNames); err != nil {
		return r, errors.Wrapf(err, "failed to write request data")
	}

	// execute
	r, err := c.Call(o)
	if err != nil {
		return r, errors.Wrapf(err, "failed to execute OP_QUERY_SQL_FIELDS operation")
	}
	if err = r.CheckStatus(); err != nil {
		return r, err
	}

	return r, nil
}

// QuerySQLFieldsCursorGetPage retrieves the next query result page by cursor id from QuerySQLFields.
func (c *client) QuerySQLFieldsCursorGetPage(id int64, fieldCount int) (QuerySQLFieldsPage, error) {
	var res QuerySQLFieldsPage

	r, err := c.QuerySQLFieldsCursorGetPageRaw(id)
	if err != nil {
		return res, errors.Wrapf(err, "failed to execute OP_QUERY_SQL_FIELDS_CURSOR_GET_PAGE operation")
	}

	// read data
	var rowCount int32
	if err = r.ReadPrimitives(&rowCount); err != nil {
		return res, errors.Wrapf(err, "failed to read row count")
	}
	res.Rows = make([][]interface{}, rowCount)
	for i := 0; i < int(rowCount); i++ {
		res.Rows[i], err = r.ReadObjects(fieldCount)
		if err != nil {
			return res, errors.Wrapf(err, "failed to read row with index %d", i)
		}
	}
	if err = r.ReadPrimitives(&res.HasMore); err != nil {
		return res, errors.Wrapf(err, "failed to read response data")
	}

	return res, nil
}

func (c *client) QuerySQLFieldsCursorGetPageRaw(id int64) (Response, error) {
	r, err := c.Exec(OpQuerySQLFieldsCursorGetPage, id)
	if err != nil {
		return r, errors.Wrapf(err, "failed to execute OP_QUERY_SQL_FIELDS_CURSOR_GET_PAGE operation")
	}
	if err = r.CheckStatus(); err != nil {
		return r, err
	}

	return r, nil
}

// ResourceClose closes a resource, such as query cursor.
func (c *client) ResourceClose(id int64) error {
	r, err := c.Exec(OpResourceClose, id)
	if err != nil {
		return errors.Wrapf(err, "failed to execute OP_RESOURCE_CLOSE operation")
	}

	return r.CheckStatus()
}
