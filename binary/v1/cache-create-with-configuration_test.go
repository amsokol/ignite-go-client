package ignite

import "testing"

func Test_client_CacheCreateWithConfiguration(t *testing.T) {
	// get test data
	c, err := getTestClient()
	if err != nil {
		t.Fatalf("failed to open test connection: %s", err.Error())
	}
	defer c.Close()
	var status int32
	cache := "TestCache1"

	type args struct {
		cc     *CacheConfigurationRefs
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
				cc: &CacheConfigurationRefs{
					Name: &cache,
				},
				status: &status,
			},
		},
		{
			name: "failed test",
			c:    c,
			args: args{
				cc: &CacheConfigurationRefs{
					Name: &cache,
				},
				status: &status,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.c.CacheCreateWithConfiguration(tt.args.cc, tt.args.status); (err != nil) != tt.wantErr {
				t.Errorf("client.CacheCreateWithConfiguration() status = %d, error = %v, wantErr %v",
					status, err, tt.wantErr)
			}
		})
	}

	// clear test data
	c.CacheDestroy(cache, nil)
}
