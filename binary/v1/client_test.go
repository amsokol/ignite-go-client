package ignite

import (
	"crypto/tls"
	"testing"
)

func TestConnect(t *testing.T) {
	type args struct {
		ci ConnInfo
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
				ci: ConnInfo{
					Network:  "tcp",
					Host:     "localhost",
					Port:     10800,
					Major:    1,
					Minor:    1,
					Patch:    0,
					Username: "ignite",
					Password: "ignite",
					TLSConfig: &tls.Config{
						InsecureSkipVerify: true,
					},
				},
			},
		},
		{
			name: "2",
			args: args{
				ci: ConnInfo{
					Network:  "tcp",
					Host:     "localhost",
					Port:     10800,
					Major:    999,
					Minor:    0,
					Patch:    0,
					Username: "ignite",
					Password: "ignite",
					TLSConfig: &tls.Config{
						InsecureSkipVerify: true,
					},
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Connect(tt.args.ci)
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
