package binary

import (
	"net"
	"testing"
)

func TestConnect(t *testing.T) {
	type args struct {
		network string
		address string
		major   int
		minor   int
		patch   int
	}
	tests := []struct {
		name    string
		args    args
		want    net.Conn
		wantErr bool
	}{
		{
			name: "success test",
			args: args{
				network: "tcp",
				address: "127.0.0.1:10800",
				major:   1,
				minor:   0,
				patch:   0,
			},
		},
		{
			name: "failed test",
			args: args{
				network: "tcp",
				address: "127.0.0.1:10800",
				major:   999,
				minor:   0,
				patch:   0,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Connect(tt.args.network, tt.args.address, tt.args.major, tt.args.minor, tt.args.patch)
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

func TestConnect100(t *testing.T) {
	type args struct {
		network string
		address string
	}
	tests := []struct {
		name    string
		args    args
		want    net.Conn
		wantErr bool
	}{
		{
			name: "success test",
			args: args{
				network: "tcp",
				address: "127.0.0.1:10800",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Connect100(tt.args.network, tt.args.address)
			if (err != nil) != tt.wantErr {
				t.Errorf("Connect100() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != nil {
				got.Close()
			}
		})
	}
}
