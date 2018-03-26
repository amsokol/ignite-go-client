package ignite

import (
	"testing"

	"github.com/google/uuid"
)

func Test_client_CachePut(t *testing.T) {
	// get test data
	c, err := getTestClient()
	if err != nil {
		t.Fatalf("failed to open test connection: %s", err.Error())
	}
	defer c.Close()
	var status int32
	if err = c.CacheCreateWithName("TestCache1", nil); err != nil {
		t.Fatalf("failed to create test cache: %s", err.Error())
	}
	defer c.CacheDestroy("TestCache1", nil)

	type args struct {
		cache  string
		binary bool
		key    interface{}
		value  interface{}
		status *int32
	}
	tests := []struct {
		name    string
		c       *client
		args    args
		wantErr bool
	}{
		{
			name: "success test 1",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key1",
				value:  byte(123),
				status: &status,
			},
		},
		{
			name: "success test 2",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key2",
				value:  int16(1234),
				status: &status,
			},
		},
		{
			name: "success test 3",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key3",
				value:  int32(1234),
				status: &status,
			},
		},
		{
			name: "success test 4",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key4",
				value:  int64(123456789),
				status: &status,
			},
		},
		{
			name: "success test 5",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key5",
				value:  float32(1.123),
				status: &status,
			},
		},
		{
			name: "success test 6",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key6",
				value:  float64(1.123456),
				status: &status,
			},
		},
		{
			name: "success test 7",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key7",
				value:  Char('W'),
				status: &status,
			},
		},
		{
			name: "success test 8",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key8",
				value:  true,
				status: &status,
			},
		},
		{
			name: "success test 9",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key9",
				value:  "value",
				status: &status,
			},
		},
		{
			name: "success test 10",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key10",
				value:  uuid.New(),
				status: &status,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.c.CachePut(tt.args.cache, tt.args.binary, tt.args.key, tt.args.value, tt.args.status); (err != nil) != tt.wantErr {
				t.Errorf("client.CachePut() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
