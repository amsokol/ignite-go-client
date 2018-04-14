package v1

import (
	"context"
	"database/sql/driver"
	"reflect"
	"testing"
	"time"

	"github.com/Masterminds/semver"
	"github.com/amsokol/ignite-go-client/sql/common"
)

func TestConnect(t *testing.T) {
	ver, _ := semver.NewVersion("1.0.0")

	type args struct {
		ctx context.Context
		ci  common.ConnInfo
	}
	tests := []struct {
		name    string
		args    args
		want    driver.Conn
		wantErr bool
	}{
		{
			name: "success test 1",
			args: args{
				ctx: context.Background(),
				ci: common.ConnInfo{
					URL:      "tcp://localhost:10800/TestDB",
					Network:  "tcp",
					Address:  "localhost:10800",
					Cache:    "TestDB",
					Version:  ver,
					PageSize: 10000,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Connect(tt.args.ctx, tt.args.ci)
			if (err != nil) != tt.wantErr {
				t.Errorf("Connect() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			/*
				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("Connect() = %v, want %v", got, tt.want)
				}
			*/
			_ = got.Close()
		})
	}
}

func Test_conn_Close(t *testing.T) {
	ver, _ := semver.NewVersion("1.0.0")
	ci, err := Connect(context.Background(), common.ConnInfo{
		URL:      "tcp://localhost:10800/TestDB",
		Network:  "tcp",
		Address:  "localhost:10800",
		Cache:    "TestDB",
		Version:  ver,
		PageSize: 10000,
	})
	if err != nil {
		t.Errorf("failed to connect: %v", err)
		return
	}
	c, _ := ci.(*conn)

	tests := []struct {
		name    string
		c       *conn
		wantErr bool
	}{
		{
			name: "success test 1",
			c:    c,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.c.Close(); (err != nil) != tt.wantErr {
				t.Errorf("conn.Close() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_conn_ExecContext(t *testing.T) {
	ver, _ := semver.NewVersion("1.0.0")
	ci, err := Connect(context.Background(), common.ConnInfo{
		URL:      "tcp://localhost:10800/TestDB",
		Network:  "tcp",
		Address:  "localhost:10800",
		Cache:    "TestDB",
		Version:  ver,
		PageSize: 10000,
	})
	if err != nil {
		t.Errorf("failed to connect: %v", err)
		return
	}
	c, _ := ci.(*conn)
	defer c.Close()
	defer c.client.CacheRemoveAll("TestDB", false)
	_ = c.client.CacheRemoveAll("TestDB", false)
	tm := time.Date(2018, 4, 3, 14, 25, 32, int(time.Millisecond*123+time.Microsecond*456+789), time.UTC)

	type args struct {
		ctx   context.Context
		query string
		args  []driver.NamedValue
	}
	tests := []struct {
		name    string
		c       *conn
		args    args
		want    driver.Result
		wantErr bool
	}{
		{
			name: "success test 1",
			c:    c,
			args: args{
				ctx:   context.Background(),
				query: "INSERT INTO Organization(_key, name) VALUES (1, 'Org 1')",
			},
			want: &result{ra: 1},
		},
		{
			name: "success test 2",
			c:    c,
			args: args{
				ctx: context.Background(),
				query: "INSERT INTO Organization(_key, name, foundDateTime) VALUES" +
					"(?, ?, ?)," +
					"(?, ?, ?)," +
					"(?, ?, ?)",
				args: []driver.NamedValue{
					driver.NamedValue{Name: "", Ordinal: 1, Value: int64(2)},
					driver.NamedValue{Name: "", Ordinal: 2, Value: "Org 2"},
					driver.NamedValue{Name: "", Ordinal: 3, Value: tm},
					driver.NamedValue{Name: "", Ordinal: 4, Value: int64(3)},
					driver.NamedValue{Name: "", Ordinal: 5, Value: "Org 3"},
					driver.NamedValue{Name: "", Ordinal: 6, Value: tm},
					driver.NamedValue{Name: "", Ordinal: 7, Value: int64(4)},
					driver.NamedValue{Name: "", Ordinal: 8, Value: "Org 4"},
					driver.NamedValue{Name: "", Ordinal: 9, Value: tm},
				},
			},
			want: &result{ra: 3},
		},
		{
			name: "success test 3",
			c:    c,
			args: args{
				ctx:   context.Background(),
				query: "UPDATE Organization SET foundDateTime=? WHERE _key=?",
				args: []driver.NamedValue{
					driver.NamedValue{Name: "", Ordinal: 1, Value: tm},
					driver.NamedValue{Name: "", Ordinal: 2, Value: int64(1)},
				},
			},
			want: &result{ra: 1},
		},
		{
			name: "success test 4",
			c:    c,
			args: args{
				ctx:   context.Background(),
				query: "DELETE FROM Organization",
			},
			want: &result{ra: 4},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.c.ExecContext(tt.args.ctx, tt.args.query, tt.args.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("conn.ExecContext() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("conn.ExecContext() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_conn_QueryContext(t *testing.T) {
	ver, _ := semver.NewVersion("1.0.0")
	ci, err := Connect(context.Background(), common.ConnInfo{
		URL:      "tcp://localhost:10800/TestDB",
		Network:  "tcp",
		Address:  "localhost:10800",
		Cache:    "TestDB",
		Version:  ver,
		PageSize: 2, /* test server cursor */
	})
	if err != nil {
		t.Errorf("failed to connect: %v", err)
		return
	}
	c, _ := ci.(*conn)
	defer c.Close()
	defer c.client.CacheRemoveAll("TestDB", false)
	_ = c.client.CacheRemoveAll("TestDB", false)
	tm := time.Date(2018, 4, 3, 14, 25, 32, int(time.Millisecond*123+time.Microsecond*456+789), time.UTC)
	_, err = c.ExecContext(context.Background(),
		"INSERT INTO Organization(_key, name, foundDateTime) VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?), (?, ?, ?)",
		[]driver.NamedValue{
			driver.NamedValue{Name: "", Ordinal: 1, Value: int64(1)},
			driver.NamedValue{Name: "", Ordinal: 2, Value: "Org 1"},
			driver.NamedValue{Name: "", Ordinal: 3, Value: tm},
			driver.NamedValue{Name: "", Ordinal: 4, Value: int64(2)},
			driver.NamedValue{Name: "", Ordinal: 5, Value: "Org 2"},
			driver.NamedValue{Name: "", Ordinal: 6, Value: tm},
			driver.NamedValue{Name: "", Ordinal: 7, Value: int64(3)},
			driver.NamedValue{Name: "", Ordinal: 8, Value: "Org 3"},
			driver.NamedValue{Name: "", Ordinal: 9, Value: tm},
			driver.NamedValue{Name: "", Ordinal: 10, Value: int64(4)},
			driver.NamedValue{Name: "", Ordinal: 11, Value: "Org 4"},
			driver.NamedValue{Name: "", Ordinal: 12, Value: tm},
		})
	if err != nil {
		t.Errorf("failed to insert test data: %v", err)
		return
	}

	type args struct {
		ctx   context.Context
		query string
		args  []driver.NamedValue
	}
	tests := []struct {
		name    string
		c       *conn
		args    args
		want    [][]driver.Value
		wantErr bool
	}{
		{
			name: "success test 1",
			c:    c,
			args: args{
				ctx:   context.Background(),
				query: "SELECT _key, name, foundDateTime FROM Organization ORDER BY _key ASC",
			},
			want: [][]driver.Value{
				[]driver.Value{int64(1), "Org 1", tm},
				[]driver.Value{int64(2), "Org 2", tm},
				[]driver.Value{int64(3), "Org 3", tm},
				[]driver.Value{int64(4), "Org 4", tm},
			},
		},
		{
			name: "success test 2",
			c:    c,
			args: args{
				ctx:   context.Background(),
				query: "SELECT _key, name, foundDateTime FROM Organization WHERE _key=?",
				args: []driver.NamedValue{
					driver.NamedValue{Name: "", Ordinal: 1, Value: int64(1)},
				},
			},
			want: [][]driver.Value{
				[]driver.Value{int64(1), "Org 1", tm},
			},
		},
		{
			name: "success test 3",
			c:    c,
			args: args{
				ctx:   context.Background(),
				query: "SELECT 1",
			},
			want: [][]driver.Value{
				[]driver.Value{int32(1)},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.c.QueryContext(tt.args.ctx, tt.args.query, tt.args.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("conn.QueryContext() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			defer got.Close()
			var dest [3]driver.Value
			for _, v := range tt.want {
				if err = got.Next(dest[:]); err != nil {
					t.Errorf("conn.QueryContext() = %v, failed to get row", err)
					break
				}
				if !reflect.DeepEqual(v, dest[:]) {
					t.Errorf("conn.QueryContext(), want %v but got %v", v, dest)
				}
			}
		})
	}
}

func Test_conn_Ping(t *testing.T) {
	ver, _ := semver.NewVersion("1.0.0")
	ci, err := Connect(context.Background(), common.ConnInfo{
		URL:      "tcp://localhost:10800/TestDB",
		Network:  "tcp",
		Address:  "localhost:10800",
		Cache:    "TestDB",
		Version:  ver,
		PageSize: 10000,
	})
	if err != nil {
		t.Errorf("failed to connect: %v", err)
		return
	}
	c, _ := ci.(*conn)
	defer c.Close()

	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name    string
		c       *conn
		args    args
		wantErr bool
	}{
		{
			name: "success test 3",
			c:    c,
			args: args{
				ctx: context.Background(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.c.Ping(tt.args.ctx); (err != nil) != tt.wantErr {
				t.Errorf("conn.Ping() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
