package binary

import (
	"io"
	"reflect"
	"sort"
	"testing"
)

func TestCacheGetNames(t *testing.T) {
	conn, err := GetTestConnection()
	if err != nil {
		t.Error(err)
		return
	}
	defer conn.Close()

	// create test data
	if res, err := CacheCreateWithName(conn, "TestCache1"); err != nil {
		t.Errorf("Create test data 1 error = %v, status = %d", err, res.Status)
	}
	defer CacheDestroy(conn, "TestCache1")
	if res, err := CacheCreateWithName(conn, "TestCache2"); err != nil {
		t.Errorf("Create test data 2 error = %v, status = %d", err, res.Status)
	}
	defer CacheDestroy(conn, "TestCache2")
	if res, err := CacheCreateWithName(conn, "TestCache3"); err != nil {
		t.Errorf("Create test data 3 error = %v, status = %d", err, res.Status)
	}
	defer CacheDestroy(conn, "TestCache3")

	type args struct {
		rw io.ReadWriter
	}
	tests := []struct {
		name    string
		args    args
		want    Result
		want1   []string
		wantErr bool
	}{
		{
			name: "test success",
			args: args{
				rw: conn,
			},
			want:  Result{Status: 0, Message: ""},
			want1: []string{"TestCache1", "TestCache2", "TestCache3"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := CacheGetNames(tt.args.rw)
			if (err != nil) != tt.wantErr {
				t.Errorf("CacheGetNames() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CacheGetNames() got = %v, want %v", got, tt.want)
			}
			sort.Strings(got1)
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("CacheGetNames() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
