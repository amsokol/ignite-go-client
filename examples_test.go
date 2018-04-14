package examples

import (
	"context"
	"database/sql"
	"log"
	"testing"
	"time"

	_ "github.com/amsokol/ignite-go-client/sql"
)

func Test_SQL_Driver(t *testing.T) {
	ctx := context.Background()

	// open connection
	db, err := sql.Open("ignite", "tcp://localhost:10800/TestDB?version=1.0.0&&page-size=10000&timeout=5000")
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
	res, err = db.ExecContext(ctx, "INSERT INTO Organization(_key, name) VALUES (1, 'Org 1')")
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
		int64(2), "Org 2", time.Now(),
		int64(3), "Org 3", time.Now(),
		int64(4), "Org 4", time.Now())
	if err != nil {
		t.Fatalf("failed sql execute: %v", err)
	}
	c, _ = res.RowsAffected()
	log.Printf("inserted rows: %d", c)

	// update
	res, err = db.ExecContext(ctx, "UPDATE Organization SET foundDateTime=? WHERE _key=?", time.Now(), int64(1))
	if err != nil {
		t.Fatalf("failed sql execute: %v", err)
	}
	c, _ = res.RowsAffected()
	log.Printf("updated rows: %d", c)

	// select
	stmt, err = db.PrepareContext(ctx,
		"SELECT _key, name, foundDateTime FROM Organization WHERE _key<? ORDER BY _key ASC")
	if err != nil {
		t.Fatalf("failed to prepare statement: %v", err)
	}
	rows, err := stmt.QueryContext(ctx, int64(4))
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
