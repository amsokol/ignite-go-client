// +build go1.10

package ignitesql

import (
	"database/sql/driver"
	"testing"
)

func TestDriver_OpenConnector(t *testing.T) {
	type args struct {
		name string
	}
	tests := []struct {
		name    string
		d       *Driver
		args    args
		want    driver.Connector
		wantErr bool
	}{
		{
			name: "success test 1",
			d:    &Driver{},
			args: args{
				name: "tcp://localhost:10800/OpenConnector?version=1.1.0&username=ignite&password=ignite",
			},
		},
		{
			name: "failed test 2",
			d:    &Driver{},
			args: args{
				name: "tcp://localhost:10800/OpenConnector?invalid-param=true",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.d.OpenConnector(tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("Driver.OpenConnector() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}
