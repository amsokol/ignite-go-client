package internal

import (
	"net"
	"testing"
)

func TestHandshake(t *testing.T) {
	conn, err := GetTestConnection()
	if err != nil {
		t.Error(err)
		return
	}
	defer conn.Close()

	type args struct {
		conn net.Conn
		v    Version
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "test failed",
			args: args{
				conn: conn,
				v:    Version{Major: 999, Minor: 0, Patch: 0},
			},
			wantErr: true,
		},
		{
			name: "test success",
			args: args{
				conn: conn,
				v:    Version{Major: 1, Minor: 0, Patch: 0},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := Handshake(tt.args.conn, tt.args.v); err != nil {
				if tt.wantErr {
					t.Logf("Handshake() error = %v, wantErr %v", err, tt.wantErr)
				} else {
					t.Errorf("Handshake() error = %v, wantErr %v", err, tt.wantErr)
				}
			}
		})
	}
}
