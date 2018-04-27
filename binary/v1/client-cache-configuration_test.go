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
				cache: "CacheCreateWithName",
			},
		},
		{
			name: "2",
			c:    c,
			args: args{
				cache: "CacheCreateWithName",
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

func Test_client_CacheGetOrCreateWithName(t *testing.T) {
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
				cache: "CacheGetOrCreateWithName",
			},
		},
		{
			name: "2",
			c:    c,
			args: args{
				cache: "CacheGetOrCreateWithName",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.c.CacheGetOrCreateWithName(tt.args.cache); (err != nil) != tt.wantErr {
				t.Errorf("client.CacheGetOrCreateWithName() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_client_CacheGetNames(t *testing.T) {
	c, err := Connect(context.Background(), "tcp", "localhost", 10800, 1, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	tests := []struct {
		name    string
		c       Client
		want    []string
		wantErr bool
	}{
		{
			name: "1",
			c:    c,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_ /*got*/, err := tt.c.CacheGetNames()
			if (err != nil) != tt.wantErr {
				t.Errorf("client.CacheGetNames() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			/*
				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("client.CacheGetNames() = %v, want %v", got, tt.want)
				}
			*/
		})
	}
}
