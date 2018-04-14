package ignitesql

import (
	"database/sql/driver"
	"reflect"
	"testing"

	"github.com/Masterminds/semver"

	"github.com/amsokol/ignite-go-client/sql/common"
)

func TestDriver_parseYesNo(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name    string
		d       *Driver
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "success test 1",
			d:    &Driver{},
			args: args{
				s: "yes",
			},
			want: true,
		},
		{
			name: "success test 2",
			d:    &Driver{},
			args: args{
				s: "yEs",
			},
			want: true,
		},
		{
			name: "success test 3",
			d:    &Driver{},
			args: args{
				s: "no",
			},
			want: false,
		},
		{
			name: "success test 4",
			d:    &Driver{},
			args: args{
				s: "nO",
			},
			want: false,
		},
		{
			name: "failed test 5",
			d:    &Driver{},
			args: args{
				s: "y",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.d.parseYesNo(tt.args.s)
			if (err != nil) != tt.wantErr {
				t.Errorf("Driver.parseYesNo() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("Driver.parseYesNo() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDriver_parseURL(t *testing.T) {
	ver1, _ := semver.NewVersion("1.1.1")
	ver2, _ := semver.NewVersion("1.0.0")

	type args struct {
		name string
	}
	tests := []struct {
		name    string
		d       *Driver
		args    args
		want    common.ConnInfo
		wantErr bool
	}{
		{
			name: "success test 1",
			d:    &Driver{},
			args: args{
				name: "tcp://localhost:10800/TestDB?" +
					"schema=SCHEMA" +
					"&version=1.1.1" +
					"&page-size=100" +
					"&max-rows=99" +
					"&timeout=5555" +
					"&distributed-joins=yes" +
					"&local-query=yes" +
					"&replicated-only=yes" +
					"&enforce-join-order=yes" +
					"&collocated=yes" +
					"&lazy-query=yes",
			},
			want: common.ConnInfo{
				URL: "tcp://localhost:10800/TestDB?" +
					"schema=SCHEMA" +
					"&version=1.1.1" +
					"&page-size=100" +
					"&max-rows=99" +
					"&timeout=5555" +
					"&distributed-joins=yes" +
					"&local-query=yes" +
					"&replicated-only=yes" +
					"&enforce-join-order=yes" +
					"&collocated=yes" +
					"&lazy-query=yes",
				Network:          "tcp",
				Address:          "localhost:10800",
				Cache:            "TestDB",
				Version:          ver1,
				Schema:           "SCHEMA",
				PageSize:         100,
				MaxRows:          99,
				Timeout:          5555,
				DistributedJoins: true,
				LocalQuery:       true,
				ReplicatedOnly:   true,
				EnforceJoinOrder: true,
				Collocated:       true,
				LazyQuery:        true,
			},
		},
		{
			name: "success test 2",
			d:    &Driver{},
			args: args{
				name: "tcp://localhost/TestDB",
			},
			want: common.ConnInfo{
				URL:      "tcp://localhost/TestDB",
				Network:  "tcp",
				Address:  "localhost:10800",
				Cache:    "TestDB",
				Version:  ver2,
				PageSize: 10000,
			},
		},
		{
			name: "failed test 3",
			d:    &Driver{},
			args: args{
				name: "tcp://localhost/TestDB?invalid-param=true",
			},
			want: common.ConnInfo{
				URL:      "tcp://localhost/TestDB",
				Network:  "tcp",
				Address:  "localhost:10800",
				Cache:    "TestDB",
				Version:  ver2,
				PageSize: 10000,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.d.parseURL(tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("Driver.parseURL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Driver.parseURL() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDriver_Open(t *testing.T) {
	type args struct {
		name string
	}
	tests := []struct {
		name    string
		d       *Driver
		args    args
		want    driver.Conn
		wantErr bool
	}{
		{
			name: "success test 1",
			d:    &Driver{},
			args: args{
				name: "tcp://localhost:10800/TestDB",
			},
		},
		{
			name: "failed test 2",
			d:    &Driver{},
			args: args{
				name: "tcp://localhost:10800/TestDB?invalid-param=true",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			/*got*/ _, err := tt.d.Open(tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("Driver.Open() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			/*
				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("Driver.Open() = %v, want %v", got, tt.want)
				}
			*/
		})
	}
}

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
				name: "tcp://localhost:10800/TestDB",
			},
		},
		{
			name: "failed test 2",
			d:    &Driver{},
			args: args{
				name: "tcp://localhost:10800/TestDB?invalid-param=true",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			/*got*/ _, err := tt.d.OpenConnector(tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("Driver.OpenConnector() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			/*
				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("Driver.OpenConnector() = %v, want %v", got, tt.want)
				}
			*/
		})
	}
}
