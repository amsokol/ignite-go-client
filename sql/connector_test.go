// +build go1.10

package ignitesql

import (
	"context"
	"database/sql/driver"
	"testing"
)

func Test_connector_Connect(t *testing.T) {
	d := &Driver{}
	ci, err := d.OpenConnector("tcp://localhost:10800/DriverOpen?version=1.1.0&username=ignite&password=ignite&tls=yes&tls-insecure-skip-verify=yes")
	if err != nil {
		t.Errorf("failed to open connector: %v", err)
		return
	}
	c, _ := ci.(*connector)

	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name    string
		c       *connector
		args    args
		want    driver.Conn
		wantErr bool
	}{
		{
			name: "success test1",
			c:    c,
			args: args{
				context.Background(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.c.Connect(tt.args.ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("connector.Connect() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != nil {
				_ = got.Close()
			}
		})
	}
}
