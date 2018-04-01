package ignite

import "testing"

func Test_client_CacheGetOrCreateWithName(t *testing.T) {
	// get test data
	c, err := getTestClient()
	if err != nil {
		t.Fatalf("failed to open test connection: %s", err.Error())
	}
	defer c.Close()
	var status int32

	type args struct {
		cache  string
		status *int32
	}
	tests := []struct {
		name    string
		c       *client
		args    args
		wantErr bool
	}{
		{
			name: "success test",
			c:    c,
			args: args{
				cache:  "TestCache1",
				status: &status,
			},
		},
		{
			name: "success test",
			c:    c,
			args: args{
				cache:  "TestCache1",
				status: &status,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.c.CacheGetOrCreateWithName(tt.args.cache, tt.args.status); (err != nil) != tt.wantErr {
				t.Errorf("client.CacheGetOrCreateWithName() status = %d, error = %v, wantErr %v",
					*tt.args.status, err, tt.wantErr)
			}
		})
	}

	// clear test data
	c.CacheDestroy("TestCache1", nil)
}
