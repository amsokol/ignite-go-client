package ignite

import (
	"fmt"
	"math/rand"
)

const (
	// StatementTypeAny is ANY = 0
	StatementTypeAny byte = 0

	// StatementTypeSelect is SELECT = 1
	StatementTypeSelect byte = 1

	// StatementTypeUpdate is UPDATE = 2
	StatementTypeUpdate byte = 2
)

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
	Fields []interface{}

	// Query result first page
	QuerySQLFieldsPage
}

func (c *client) QuerySQLFields(cache string, binary bool, data QuerySQLFieldsData, status *int32) (QuerySQLFieldsResult, error) {
	if status != nil {
		*status = StatusSuccess
	}

	uid := rand.Int63()
	o := c.Prepare(opQuerySQLFields, uid)

	var res QuerySQLFieldsResult

	if err := o.WritePrimitives(hashCode(cache), binary); err != nil {
		return res, fmt.Errorf("failed to write request data: %s", err.Error())
	}

	if len(data.Schema) > 0 {
		if err := o.WriteObjects(data.Schema); err != nil {
			return res, fmt.Errorf("failed to write schema for the query: %s", err.Error())
		}
	} else {
		if err := o.WriteObjects(nil); err != nil {
			return res, fmt.Errorf("failed to write nil for schema for the query: %s", err.Error())
		}
	}

	if err := o.WritePrimitives(int32(data.PageSize), int32(data.MaxRows), data.Query); err != nil {
		return res, fmt.Errorf("failed to write request data: %s", err.Error())
	}

	var l int32
	if data.QueryArgs != nil {
		l = int32(len(data.QueryArgs))
	}
	// write args
	if err := o.WritePrimitives(l); err != nil {
		return res, fmt.Errorf("failed to write query arg count: %s", err.Error())
	}
	if l > 0 {
		for i, v := range data.QueryArgs {
			if err := o.WriteObjects(v); err != nil {
				return res, fmt.Errorf("failed to write query arg with index %d: %s", i, err.Error())
			}
		}
	}

	if err := o.WritePrimitives(data.StatementType, data.DistributedJoins, data.LocalQuery, data.ReplicatedOnly,
		data.EnforceJoinOrder, data.Collocated, data.LazyQuery, data.Timeout, data.IncludeFieldNames); err != nil {
		return res, fmt.Errorf("failed to write request data: %s", err.Error())
	}

	// execute
	r, err := c.Call(o)
	if err != nil {
		return res, fmt.Errorf("failed to execute operation: %s", err.Error())
	}
	if r.UID != uid {
		return res, fmt.Errorf("invalid response id (expected %d, but received %d)", uid, r.UID)
	}
	if status != nil {
		*status = r.Status
	}
	if r.Status != StatusSuccess {
		return res, fmt.Errorf("failed to execute operation: status=%d, message=%s", r.Status, r.Message)
	}

	// read field names
	var fieldCount int32
	if err = r.ReadPrimitives(&res.ID, &fieldCount); err != nil {
		return res, fmt.Errorf("failed to read field count: %s", err.Error())
	}
	res.FieldCount = int(fieldCount)
	if data.IncludeFieldNames {
		res.Fields, err = r.ReadObjects(res.FieldCount)
	} else {
		res.Fields = []interface{}{}
	}

	// read data
	var rowCount int32
	if err = r.ReadPrimitives(&rowCount); err != nil {
		return res, fmt.Errorf("failed to read row count: %s", err.Error())
	}
	res.Rows = make([][]interface{}, rowCount)
	for i := 0; i < int(rowCount); i++ {
		res.Rows[i], err = r.ReadObjects(res.FieldCount)
		if err != nil {
			return res, fmt.Errorf("failed to read row with index %d: %s", i, err.Error())
		}
	}
	if err = r.ReadPrimitives(&res.HasMore); err != nil {
		return res, fmt.Errorf("failed to read response data: %s", err.Error())
	}

	return res, nil
}
