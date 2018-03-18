package binary

import (
	"io"
	"testing"
)

func TestCacheDestroy(t *testing.T) {
	conn, err := GetTestConnection()
	if err != nil {
		t.Error(err)
		return
	}
	defer conn.Close()

	// create test data
	if res, err := CacheCreateWithName(conn, "TestCache1"); err != nil {
		t.Errorf("Create test data error = %v, status = %d", err, res.Status)
	}

	type args struct {
		rw   io.ReadWriter
		name string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "test success",
			args: args{
				rw:   conn,
				name: "TestCache1",
			},
		},
		{
			name: "test success",
			args: args{
				rw:   conn,
				name: "NotFound",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CacheDestroy(tt.args.rw, tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("CacheDestroy() error = %v, status = %d, wantErr %v", err, got.Status, tt.wantErr)
				return
			}
		})
	}
}
