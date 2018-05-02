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
	// request and response
	req := NewRequestOperation(OpQuerySQL)
	res := NewResponseOperation(req.UID)

	var r QuerySQLResult
	var err error

	// set parameters
	if err = req.WriteInt(HashCode(cache)); err != nil {
		return r, errors.Wrapf(err, "failed to write cache name")
	}
	if err = req.WriteBool(binary); err != nil {
		return r, errors.Wrapf(err, "failed to write binary flag")
	}
	if err = req.WriteOString(data.Table); err != nil {
		return r, errors.Wrapf(err, "failed to write table name")
	}
	if err = req.WriteOString(data.Query); err != nil {
		return r, errors.Wrapf(err, "failed to write query")
	}

	var l int32
	if data.QueryArgs != nil {
		l = int32(len(data.QueryArgs))
	}
	// write args
	if err = req.WriteInt(l); err != nil {
		return r, errors.Wrapf(err, "failed to write query arg count")
	}
	if l > 0 {
		for i, v := range data.QueryArgs {
			if err = req.WriteObject(v); err != nil {
				return r, errors.Wrapf(err, "failed to write query arg with index %d", i)
			}
		}
	}

	if err = req.WriteBool(data.DistributedJoins); err != nil {
		return r, errors.Wrapf(err, "failed to write distributed joins flag")
	}
	if err = req.WriteBool(data.LocalQuery); err != nil {
		return r, errors.Wrapf(err, "failed to write local query flag")
	}
	if err = req.WriteBool(data.ReplicatedOnly); err != nil {
		return r, errors.Wrapf(err, "failed to write replicated only flag")
	}
	if err = req.WriteInt(int32(data.PageSize)); err != nil {
		return r, errors.Wrapf(err, "failed to write page size")
	}
	if err = req.WriteLong(data.Timeout); err != nil {
		return r, errors.Wrapf(err, "failed to write timeout")
	}

	// execute operation
	if err = c.Do(req, res); err != nil {
		return r, errors.Wrapf(err, "failed to execute OP_QUERY_SQL operation")
	}
	if err = res.CheckStatus(); err != nil {
		return r, err
	}

	// process result
	if r.ID, err = res.ReadLong(); err != nil {
		return r, errors.Wrapf(err, "failed to read cursor ID")
	}
	count, err := res.ReadInt()
	if err != nil {
		return r, errors.Wrapf(err, "failed to read row count")
	}
	r.Keys = make([]interface{}, 0, int(count))
	r.Values = make([]interface{}, 0, int(count))
	// read data
	for i := 0; i < int(count); i++ {
		key, err := res.ReadObject()
		if err != nil {
			return r, errors.Wrapf(err, "failed to read key with index %d", i)
		}
		value, err := res.ReadObject()
		if err != nil {
			return r, errors.Wrapf(err, "failed to read value with index %d", i)
		}
		r.Keys = append(r.Keys, key)
		r.Values = append(r.Values, value)
	}
	if r.HasMore, err = res.ReadBool(); err != nil {
		return r, errors.Wrapf(err, "failed to read has more flag")
	}
	return r, nil
}

// QuerySQLCursorGetPage retrieves the next SQL query cursor page by cursor id from QuerySQL.
func (c *client) QuerySQLCursorGetPage(id int64) (QuerySQLPage, error) {
	// request and response
	req := NewRequestOperation(OpQuerySQLCursorGetPage)
	res := NewResponseOperation(req.UID)

	var r QuerySQLPage
	var err error

	// set parameters
	if err = req.WriteLong(id); err != nil {
		return r, errors.Wrapf(err, "failed to write cursor id")
	}

	// execute operation
	if err = c.Do(req, res); err != nil {
		return r, errors.Wrapf(err, "failed to execute OP_QUERY_SQL_CURSOR_GET_PAGE operation")
	}
	if err = res.CheckStatus(); err != nil {
		return r, err
	}

	// process result
	count, err := res.ReadInt()
	if err != nil {
		return r, errors.Wrapf(err, "failed to read row count")
	}
	r.Keys = make([]interface{}, 0, int(count))
	r.Values = make([]interface{}, 0, int(count))
	// read data
	for i := 0; i < int(count); i++ {
		key, err := res.ReadObject()
		if err != nil {
			return r, errors.Wrapf(err, "failed to read key with index %d", i)
		}
		value, err := res.ReadObject()
		if err != nil {
			return r, errors.Wrapf(err, "failed to read value with index %d", i)
		}
		r.Keys = append(r.Keys, key)
		r.Values = append(r.Values, value)
	}
	if r.HasMore, err = res.ReadBool(); err != nil {
		return r, errors.Wrapf(err, "failed to read has more flag")
	}

	return r, nil
}

func (c *client) QuerySQLFieldsRaw(cache string, binary bool, data QuerySQLFieldsData) (*ResponseOperation, error) {
	// request and response
	req := NewRequestOperation(OpQuerySQLFields)
	res := NewResponseOperation(req.UID)

	var err error

	// set parameters
	if err := req.WriteInt(HashCode(cache)); err != nil {
		return nil, errors.Wrapf(err, "failed to write cache name")
	}
	if err := req.WriteBool(binary); err != nil {
		return nil, errors.Wrapf(err, "failed to write binary flag")
	}
	if len(data.Schema) > 0 {
		if err := req.WriteOString(data.Schema); err != nil {
			return nil, errors.Wrapf(err, "failed to write schema for the query")
		}
	} else {
		if err := req.WriteNull(); err != nil {
			return nil, errors.Wrapf(err, "failed to write nil for schema for the query")
		}
	}
	if err = req.WriteInt(int32(data.PageSize)); err != nil {
		return nil, errors.Wrapf(err, "failed to write page size")
	}
	if err = req.WriteInt(int32(data.MaxRows)); err != nil {
		return nil, errors.Wrapf(err, "failed to write max rows")
	}
	if err = req.WriteOString(data.Query); err != nil {
		return nil, errors.Wrapf(err, "failed to write query")
	}

	var l int32
	if data.QueryArgs != nil {
		l = int32(len(data.QueryArgs))
	}
	// write args
	if err = req.WriteInt(l); err != nil {
		return nil, errors.Wrapf(err, "failed to write query arg count")
	}
	if l > 0 {
		for i, v := range data.QueryArgs {
			if err = req.WriteObject(v); err != nil {
				return nil, errors.Wrapf(err, "failed to write query arg with index %d", i)
			}
		}
	}

	if err = req.WriteByte(data.StatementType); err != nil {
		return nil, errors.Wrapf(err, "failed to write statement type")
	}
	if err = req.WriteBool(data.DistributedJoins); err != nil {
		return nil, errors.Wrapf(err, "failed to write distributed joins flag")
	}
	if err = req.WriteBool(data.LocalQuery); err != nil {
		return nil, errors.Wrapf(err, "failed to write local query flag")
	}
	if err = req.WriteBool(data.ReplicatedOnly); err != nil {
		return nil, errors.Wrapf(err, "failed to write replicated only flag")
	}
	if err = req.WriteBool(data.EnforceJoinOrder); err != nil {
		return nil, errors.Wrapf(err, "failed to write enforce join order flag")
	}
	if err = req.WriteBool(data.Collocated); err != nil {
		return nil, errors.Wrapf(err, "failed to write collocated flag")
	}
	if err = req.WriteBool(data.LazyQuery); err != nil {
		return nil, errors.Wrapf(err, "failed to write lazy query flag")
	}
	if err = req.WriteLong(data.Timeout); err != nil {
		return nil, errors.Wrapf(err, "failed to write timeout")
	}
	if err = req.WriteBool(data.IncludeFieldNames); err != nil {
		return nil, errors.Wrapf(err, "failed to write include field names flag")
	}

	// execute operation
	if err = c.Do(req, res); err != nil {
		return nil, errors.Wrapf(err, "failed to execute OP_QUERY_SQL_FIELDS operation")
	}
	if err = res.CheckStatus(); err != nil {
		return nil, err
	}

	return res, nil
}

// QuerySQLFields performs SQL fields query.
func (c *client) QuerySQLFields(cache string, binary bool, data QuerySQLFieldsData) (QuerySQLFieldsResult, error) {
	var r QuerySQLFieldsResult

	res, err := c.QuerySQLFieldsRaw(cache, binary, data)
	if err != nil {
		return r, err
	}

	// read field names
	if r.ID, err = res.ReadLong(); err != nil {
		return r, errors.Wrapf(err, "failed to read cursor ID")
	}
	fieldCount, err := res.ReadInt()
	if err != nil {
		return r, errors.Wrapf(err, "failed to read field count")
	}
	r.FieldCount = int(fieldCount)
	if data.IncludeFieldNames {
		r.Fields = make([]string, 0, fieldCount)
		for i := 0; i < r.FieldCount; i++ {
			var s string
			if s, err = res.ReadOString(); err != nil {
				return r, errors.Wrapf(err, "failed to read field name with index %d", i)
			}
			r.Fields = append(r.Fields, s)
		}
	} else {
		r.Fields = []string{}
	}

	// read data
	rowCount, err := res.ReadInt()
	if err != nil {
		return r, errors.Wrapf(err, "failed to read row count")
	}
	r.Rows = make([][]interface{}, rowCount)
	for i := 0; i < int(rowCount); i++ {
		r.Rows[i] = make([]interface{}, r.FieldCount)
		for j := 0; j < r.FieldCount; j++ {
			r.Rows[i][j], err = res.ReadObject()
			if err != nil {
				return r, errors.Wrapf(err, "failed to read value (row with index %d, column with index %d", i, j)
			}
		}
	}
	if r.HasMore, err = res.ReadBool(); err != nil {
		return r, errors.Wrapf(err, "failed to read has more flag")
	}

	return r, nil
}

func (c *client) QuerySQLFieldsCursorGetPageRaw(id int64) (*ResponseOperation, error) {
	// request and response
	req := NewRequestOperation(OpQuerySQLFieldsCursorGetPage)
	res := NewResponseOperation(req.UID)

	// set parameters
	if err := req.WriteLong(id); err != nil {
		return nil, errors.Wrapf(err, "failed to write cursor id")
	}

	// execute operation
	if err := c.Do(req, res); err != nil {
		return nil, errors.Wrapf(err, "failed to execute OP_QUERY_SQL_FIELDS_CURSOR_GET_PAGE operation")
	}
	if err := res.CheckStatus(); err != nil {
		return nil, err
	}

	return res, nil
}

// QuerySQLFieldsCursorGetPage retrieves the next query result page by cursor id from QuerySQLFields.
func (c *client) QuerySQLFieldsCursorGetPage(id int64, fieldCount int) (QuerySQLFieldsPage, error) {
	var r QuerySQLFieldsPage

	res, err := c.QuerySQLFieldsCursorGetPageRaw(id)
	if err != nil {
		return r, err
	}

	// read data
	rowCount, err := res.ReadInt()
	if err != nil {
		return r, errors.Wrapf(err, "failed to read row count")
	}
	r.Rows = make([][]interface{}, rowCount)
	for i := 0; i < int(rowCount); i++ {
		r.Rows[i] = make([]interface{}, fieldCount)
		for j := 0; j < int(fieldCount); j++ {
			r.Rows[i][j], err = res.ReadObject()
			if err != nil {
				return r, errors.Wrapf(err, "failed to read value (row with index %d, column with index %d", i, j)
			}
		}
	}
	if r.HasMore, err = res.ReadBool(); err != nil {
		return r, errors.Wrapf(err, "failed to read has more flag")
	}

	return r, nil
}

// ResourceClose closes a resource, such as query cursor.
func (c *client) ResourceClose(id int64) error {
	// request and response
	req := NewRequestOperation(OpResourceClose)
	res := NewResponseOperation(req.UID)

	// set parameters
	if err := req.WriteLong(id); err != nil {
		return errors.Wrapf(err, "failed to write cursor id")
	}

	// execute operation
	if err := c.Do(req, res); err != nil {
		return errors.Wrapf(err, "failed to execute OP_RESOURCE_CLOSE operation")
	}

	return res.CheckStatus()
}
