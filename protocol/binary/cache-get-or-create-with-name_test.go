package binary

import (
	"io"
	"testing"
)

func TestCacheGetOrCreateWithName(t *testing.T) {
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
			name: "test success",
			args: args{
				rw:   conn,
				name: "TestCache1",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := CacheGetOrCreateWithName(tt.args.rw, tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("CacheGetOrCreateWithName() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}

	// clean test data
	if res, err := CacheDestroy(conn, "TestCache1"); err != nil {
		t.Errorf("Clean test data error = %v, status = %d", err, res.Status)
	}
}
