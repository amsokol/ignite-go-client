package ignite

import "testing"

func Test_client_CacheContainsKey(t *testing.T) {
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
	var status int32

	// put test values
	if err = c.CachePut("TestCache1", false, "key", "value", &status); err != nil {
		t.Fatalf("failed to put test pair: %s", err.Error())
	}

	type args struct {
		cache  string
		binary bool
		key    interface{}
		status *int32
	}
	tests := []struct {
		name    string
		c       *client
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "success test 1",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key",
				status: &status,
			},
			want: true,
		},
		{
			name: "success test 2",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key-not-found",
				status: &status,
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.c.CacheContainsKey(tt.args.cache, tt.args.binary, tt.args.key, tt.args.status)
			if (err != nil) != tt.wantErr {
				t.Errorf("client.CacheContainsKey() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("client.CacheContainsKey() = %v, want %v", got, tt.want)
			}
		})
	}
}
