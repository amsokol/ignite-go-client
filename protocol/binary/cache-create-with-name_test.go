package binary

import (
	"io"
	"testing"
)

func TestCacheCreateWithName(t *testing.T) {
	conn, err := GetTestConnection()
	if err != nil {
		t.Error(err)
		return
	}
	defer conn.Close()

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
			name: "test failed",
			args: args{
				rw:   conn,
				name: "TestCache1",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if res, err := CacheCreateWithName(tt.args.rw, tt.args.name); err != nil {
				if tt.wantErr {
					t.Logf("CacheCreateWithName() error = %v, status = %d, wantErr %v", err, res.Status, tt.wantErr)
				} else {
					t.Errorf("CacheCreateWithName() error = %v, status = %d, wantErr %v", err, res.Status, tt.wantErr)
				}
			}
		})
	}

	// clean test data
	if res, err := CacheDestroy(conn, "TestCache1"); err != nil {
		t.Errorf("Clean test data error = %v, status = %d", err, res.Status)
	}
}
