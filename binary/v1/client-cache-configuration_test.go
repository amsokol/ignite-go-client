package ignite

import (
	"context"
	"testing"
)

func Test_client_CacheCreateWithName(t *testing.T) {
	c, err := Connect(context.Background(), "tcp", "localhost", 10800, 1, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	type args struct {
		cache string
	}
	tests := []struct {
		name    string
		c       Client
		args    args
		wantErr bool
	}{
		{
			name: "1",
			c:    c,
			args: args{
				cache: "CacheCreateWithName1",
			},
		},
		{
			name: "2",
			c:    c,
			args: args{
				cache: "CacheCreateWithName1",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.c.CacheCreateWithName(tt.args.cache); (err != nil) != tt.wantErr {
				t.Errorf("client.CacheCreateWithName() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
