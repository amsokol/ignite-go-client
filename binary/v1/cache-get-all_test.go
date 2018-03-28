package ignite

import (
	"reflect"
	"testing"
)

func Test_client_CacheGetAll(t *testing.T) {
	// get test data
	c, err := getTestClient()
	if err != nil {
		t.Fatalf("failed to open test connection: %s", err.Error())
	}
	defer c.Close()
	if err = c.CacheCreateWithName("TestCache1", nil); err != nil {
		t.Fatalf("failed to create test cache: %s", err.Error())
	}
	defer c.CacheDestroy("TestCache1", nil)

	testClientCachePutAll(t, c)
	var status int32

	type args struct {
		cache  string
		binary bool
		keys   []interface{}
		status *int32
	}
	tests := []struct {
		name    string
		c       *client
		args    args
		want    map[interface{}]interface{}
		wantErr bool
	}{
		{
			name: "success test 1",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				keys: []interface{}{
					byte(123),
					float64(678),
					"key",
				},
				status: &status,
			},
			want: map[interface{}]interface{}{
				byte(123):    "test",
				float64(678): Char('w'),
				"key":        int64(128),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.c.CacheGetAll(tt.args.cache, tt.args.binary, tt.args.keys, tt.args.status)
			if (err != nil) != tt.wantErr {
				t.Errorf("client.CacheGetAll() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("client.CacheGetAll() = %v, want %v", got, tt.want)
			}
		})
	}
}
