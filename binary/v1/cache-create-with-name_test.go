package ignite

import "testing"

func Test_client_CacheCreateWithName(t *testing.T) {
	// get test data
	c, err := getTestClient()
	if err != nil {
		t.Fatalf("failed to open test connection: %s", err.Error())
	}
	defer c.Close()
	var status int32

	type args struct {
		name   string
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
				name:   "TestCache1",
				status: &status,
			},
		},
		{
			name: "failed test",
			c:    c,
			args: args{
				name:   "TestCache1",
				status: &status,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.c.CacheCreateWithName(tt.args.name, tt.args.status); (err != nil) != tt.wantErr {
				t.Errorf("client.CacheCreateWithName() status = %d, error = %v, wantErr %v",
					*tt.args.status, err, tt.wantErr)
			}
		})
	}

	// clear test data
	c.CacheDestroy("TestCache1", nil)
}
