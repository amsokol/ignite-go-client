package ignite

import (
	"context"
	"testing"
)

func TestConnect(t *testing.T) {
	type args struct {
		ctx     context.Context
		network string
		host    string
		port    int
		major   int
		minor   int
		patch   int
	}
	tests := []struct {
		name    string
		args    args
		want    Client
		wantErr bool
	}{
		{
			name: "1",
			args: args{
				ctx:     context.Background(),
				network: "tcp",
				host:    "localhost",
				port:    10800,
				major:   1,
				minor:   0,
				patch:   0,
			},
		},
		{
			name: "2",
			args: args{
				ctx:     context.Background(),
				network: "tcp",
				host:    "localhost",
				port:    10800,
				major:   999,
				minor:   0,
				patch:   0,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Connect(tt.args.ctx, tt.args.network, tt.args.host, tt.args.port, tt.args.major, tt.args.minor, tt.args.patch)
			if (err != nil) != tt.wantErr {
				t.Errorf("Connect() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != nil {
				got.Close()
			}
		})
	}
}
