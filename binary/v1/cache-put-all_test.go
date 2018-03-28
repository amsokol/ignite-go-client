package ignite

import "testing"

func Test_client_CachePutAll(t *testing.T) {
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
}

func testClientCachePutAll(t *testing.T, c *client) {
	var status int32

	type args struct {
		cache  string
		binary bool
		data   map[interface{}]interface{}
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
				data: map[interface{}]interface{}{
					byte(123):    "test",
					float64(678): Char('w'),
					"key":        int64(128),
				},
				status: &status,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.c.CachePutAll(tt.args.cache, tt.args.binary, tt.args.data, tt.args.status); (err != nil) != tt.wantErr {
				t.Errorf("client.CachePutAll() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
