package ignitesql

import (
	"context"
	"database/sql/driver"
	//"reflect"
	"testing"
)

func Test_connector_Connect(t *testing.T) {
	d := &Driver{}
	ci, err := d.OpenConnector("tcp://localhost:10800/TestDB2")
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
			/*
				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("connector.Connect() = %v, want %v", got, tt.want)
				}
			*/
			_ = got.Close()
		})
	}
}
