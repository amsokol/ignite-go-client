package examples

import (
	"context"
	"database/sql"
	"log"
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
}
