package ignite

import (
	"testing"
)

func Test_client_CacheGetConfiguration(t *testing.T) {
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
		flag   byte
		status *int32
	}
	tests := []struct {
		name    string
		c       *client
		args    args
		want    *CacheConfiguration
		wantErr bool
	}{
		{
			name: "success test",
			c:    c,
			args: args{
				cache:  "TestCache1",
				flag:   0,
				status: &status,
			},
		},
		{
			name: "failed test",
			c:    c,
			args: args{
				cache:  "TestCache2",
				flag:   0,
				status: &status,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.c.CacheGetConfiguration(tt.args.cache, tt.args.flag, tt.args.status)
			if (err != nil) != tt.wantErr {
				t.Errorf("client.CacheGetConfiguration() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}
