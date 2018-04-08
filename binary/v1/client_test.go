package ignite

import (
	"context"
	"io"
	"testing"
)

func TestNewClient(t *testing.T) {
	type args struct {
		ctx     context.Context
		network string
		address string
		major   int16
		minor   int16
		patch   int16
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "success test",
			args: args{
				ctx:     context.Background(),
				network: "tcp",
				address: "127.0.0.1:10800",
				major:   1,
				minor:   0,
				patch:   0,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewClient(tt.args.ctx, tt.args.network, tt.args.address,
				tt.args.major, tt.args.minor, tt.args.patch)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewClient100() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != nil {
				got.Close()
			}
		})
	}
}

func Test_client_Close(t *testing.T) {
	// get test data
	c, err := getTestClient()
	if err != nil {
		t.Fatalf("failed to open test connection: %s", err.Error())
	}

	tests := []struct {
		name    string
		c       *client
		wantErr bool
	}{
		{
			name: "success test 1",
			c:    c,
		},
		{
			name: "success test 2",
			c:    c,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.c.Close(); (err != nil) != tt.wantErr {
				t.Errorf("client.Close() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_handshake(t *testing.T) {
	// get test data
	c1, err := getTestConnection()
	if err != nil {
		t.Fatalf("failed to open test connection: %s", err.Error())
	}
	defer c1.Close()
	c2, err := getTestConnection()
	if err != nil {
		t.Fatalf("failed to open test connection: %s", err.Error())
	}
	defer c2.Close()

	type args struct {
		rw    io.ReadWriter
		major int16
		minor int16
		patch int16
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "success test",
			args: args{
				rw:    c1,
				major: 1,
				minor: 0,
				patch: 0,
			},
		},
		{
			name: "failed test",
			args: args{
				rw:    c2,
				major: 1000,
				minor: 0,
				patch: 0,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := handshake(tt.args.rw, tt.args.major, tt.args.minor, tt.args.patch); (err != nil) != tt.wantErr {
				t.Errorf("handshake() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
