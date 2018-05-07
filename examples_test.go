package examples

import (
	"context"
	"database/sql"
	"log"
	"net"
	"testing"
	"time"

	"github.com/amsokol/ignite-go-client/binary/v1"
	_ "github.com/amsokol/ignite-go-client/sql"
)

func Test_SQL_Driver(t *testing.T) {
	ctx := context.Background()

	// open connection
	db, err := sql.Open("ignite", "tcp://localhost:10800/ExampleDB?version=1.0.0&&page-size=10000&timeout=5000")
	if err != nil {
		t.Fatalf("failed to open connection: %v", err)
	}
	defer db.Close()

	// ping
	if err = db.PingContext(ctx); err != nil {
		t.Fatalf("ping failed: %v", err)
	}

	// clear test data from server
	defer db.ExecContext(ctx, "DELETE FROM Organization")

	// delete
	res, err := db.ExecContext(ctx, "DELETE FROM Organization")
	if err != nil {
		t.Fatalf("failed sql execute: %v", err)
	}
	c, _ := res.RowsAffected()
	log.Printf("deleted rows: %d", c)

	// insert
	res, err = db.ExecContext(ctx, "INSERT INTO Organization(_key, name) VALUES (11, 'Org 11')")
	if err != nil {
		t.Fatalf("failed sql execute: %v", err)
	}
	c, _ = res.RowsAffected()
	log.Printf("inserted rows: %d", c)

	// insert using prepare statement
	stmt, err := db.PrepareContext(ctx, "INSERT INTO Organization(_key, name, foundDateTime) VALUES"+
		"(?, ?, ?),(?, ?, ?),(?, ?, ?)")
	if err != nil {
		t.Fatalf("failed to prepare statement: %v", err)
	}
	res, err = stmt.ExecContext(ctx,
		int64(12), "Org 12", time.Now(),
		int64(13), "Org 13", time.Now(),
		int64(14), "Org 14", time.Now())
	if err != nil {
		t.Fatalf("failed sql execute: %v", err)
	}
	c, _ = res.RowsAffected()
	log.Printf("inserted rows: %d", c)

	// update
	res, err = db.ExecContext(ctx, "UPDATE Organization SET foundDateTime=? WHERE _key=?", time.Now(), int64(11))
	if err != nil {
		t.Fatalf("failed sql execute: %v", err)
	}
	c, _ = res.RowsAffected()
	log.Printf("updated rows: %d", c)

	// select
	stmt, err = db.PrepareContext(ctx,
		"SELECT _key, name, foundDateTime FROM Organization WHERE _key>=? AND _key<? ORDER BY _key ASC")
	if err != nil {
		t.Fatalf("failed to prepare statement: %v", err)
	}
	rows, err := stmt.QueryContext(ctx, int64(11), int64(14))
	if err != nil {
		t.Fatalf("failed sql query: %v", err)
	}
	cols, _ := rows.Columns()
	log.Printf("columns: %v", cols)
	var (
		key  int64
		name string
		tm   time.Time
	)
	for rows.Next() {
		if err := rows.Scan(&key, &name, &tm); err != nil {
			t.Fatalf("failed to get row: %v", err)
		}
		log.Printf("key=%d, name=\"%s\", found=\"%v\"", key, name, tm)
	}
}

func Test_Key_Value(t *testing.T) {
	// connect
	c, err := ignite.Connect(ignite.ConnInfo{
		Network: "tcp",
		Host:    "localhost",
		Port:    10800,
		Major:   1,
		Minor:   0,
		Patch:   0,
		Dialer: net.Dialer{
			Timeout: 10 * time.Second,
		},
	})
	if err != nil {
		t.Fatalf("failed connect to server: %v", err)
	}
	defer c.Close()

	cache := "MyCache"

	// create cache
	if err = c.CacheCreateWithName(cache); err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}
	defer c.CacheDestroy(cache)

	// put values
	if err = c.CachePut(cache, false, "key1", "value1"); err != nil {
		t.Fatalf("failed to put pair: %v", err)
	}
	if err = c.CachePut(cache, false, "key2", "value2"); err != nil {
		t.Fatalf("failed to put pair: %v", err)
	}

	// get key value
	v, err := c.CacheGet(cache, false, "key1")
	if err != nil {
		t.Fatalf("failed to get key value: %v", err)
	}
	log.Printf("key=\"%s\", value=\"%v\"", "key1", v)

	// put complex object
	c1 := ignite.NewComplexObject("ComplexObject1")
	c1.Set("field1", "value 1")
	c1.Set("field2", int32(2))
	c1.Set("field3", true)
	c2 := ignite.NewComplexObject("ComplexObject2")
	c2.Set("complexField1", c1)
	if err = c.CachePut(cache, false, "key3", c2); err != nil {
		t.Fatalf("failed to put complex value: %v", err)
	}

	// get complex object
	v, err = c.CacheGet(cache, false, "key3")
	if err != nil {
		t.Fatalf("failed to get complex value: %v", err)
	}
	c2 = v.(ignite.ComplexObject)
	log.Printf("key=\"%s\", value=\"%#v\"", "key3", c2)
	v, _ = c2.Get("complexField1")
	c1 = v.(ignite.ComplexObject)
	log.Printf("key=\"%s\", value=\"%#v\"", "complexField1", c1)
}

func Test_SQL_Queries(t *testing.T) {
	// connect
	c, err := ignite.Connect(ignite.ConnInfo{
		Network: "tcp",
		Host:    "localhost",
		Port:    10800,
		Major:   1,
		Minor:   0,
		Patch:   0,
		Dialer: net.Dialer{
			Timeout: 10 * time.Second,
		},
	})
	if err != nil {
		t.Fatalf("failed connect to server: %v", err)
	}
	defer c.Close()

	cache := "ExampleSQLQueries"

	// insert data
	tm := time.Date(2018, 4, 3, 14, 25, 32, int(time.Millisecond*123+time.Microsecond*456+789), time.UTC)
	_, err = c.QuerySQLFields(cache, false, ignite.QuerySQLFieldsData{
		PageSize: 10,
		Query: "INSERT INTO Organization(_key, name, foundDateTime) VALUES" +
			"(?, ?, ?)," +
			"(?, ?, ?)," +
			"(?, ?, ?)",
		QueryArgs: []interface{}{
			int64(1), "Org 1", tm,
			int64(2), "Org 2", tm,
			int64(3), "Org 3", tm},
	})
	if err != nil {
		t.Fatalf("failed insert data: %v", err)
	}

	// select data using QuerySQL
	r, err := c.QuerySQL(cache, false, ignite.QuerySQLData{
		Table:    "Organization",
		Query:    "SELECT * FROM Organization ORDER BY name ASC",
		PageSize: 10000,
	})
	if err != nil {
		t.Fatalf("failed query data: %v", err)
	}
	row := r.Rows[int64(1)].(ignite.ComplexObject)
	log.Printf("%d=\"%s\", %d=%#v", 1, row.Fields[1], 2, row.Fields[2])
	row = r.Rows[int64(2)].(ignite.ComplexObject)
	log.Printf("%d=\"%s\", %d=%#v", 1, row.Fields[1], 2, row.Fields[2])
	row = r.Rows[int64(3)].(ignite.ComplexObject)
	log.Printf("%d=\"%s\", %d=%#v", 1, row.Fields[1], 2, row.Fields[2])

	// insert more data
	_, err = c.QuerySQLFields(cache, false, ignite.QuerySQLFieldsData{
		PageSize: 10,
		Query: "INSERT INTO Person(_key, orgId, firstName, lastName, resume, salary) VALUES" +
			"(?, ?, ?, ?, ?, ?)," +
			"(?, ?, ?, ?, ?, ?)," +
			"(?, ?, ?, ?, ?, ?)," +
			"(?, ?, ?, ?, ?, ?)," +
			"(?, ?, ?, ?, ?, ?)",
		QueryArgs: []interface{}{
			int64(4), int64(1), "First name 1", "Last name 1", "Resume 1", float64(100.0),
			int64(5), int64(1), "First name 2", "Last name 2", "Resume 2", float64(200.0),
			int64(6), int64(2), "First name 3", "Last name 3", "Resume 3", float64(300.0),
			int64(7), int64(2), "First name 4", "Last name 4", "Resume 4", float64(400.0),
			int64(8), int64(3), "First name 5", "Last name 5", "Resume 5", float64(500.0)},
	})
	if err != nil {
		t.Fatalf("failed insert data: %v", err)
	}

	// select data using QuerySQLFields
	r2, err := c.QuerySQLFields(cache, false, ignite.QuerySQLFieldsData{
		PageSize: 10,
		Query: "SELECT " +
			"o.name AS Name, " +
			"o.foundDateTime AS Found, " +
			"p.firstName AS FirstName, " +
			"p.lastName AS LastName, " +
			"p.salary AS Salary " +
			"FROM Person p INNER JOIN Organization o ON p.orgId = o._key " +
			"WHERE o._key = ? " +
			"ORDER BY p.firstName",
		QueryArgs: []interface{}{
			int64(2)},
		Timeout:           10000,
		IncludeFieldNames: true,
	})
	if err != nil {
		t.Fatalf("failed query data: %v", err)
	}
	log.Printf("res=%#v", r2.Rows)
}
