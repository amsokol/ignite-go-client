package ignite

import (
	"reflect"
	"sort"
	"testing"
)

func Test_client_CacheGetNames(t *testing.T) {
	// get test data
	c, err := getTestClient()
	if err != nil {
		t.Fatalf("failed to open test connection: %s", err.Error())
	}
	defer c.Close()
	var status int32
	if err = c.CacheCreateWithName("TestCache1", nil); err != nil {
		t.Fatalf("failed to create test cache: %s", err.Error())
	}
	defer c.CacheDestroy("TestCache1", nil)
	if err = c.CacheCreateWithName("TestCache2", nil); err != nil {
		t.Fatalf("failed to create test cache: %s", err.Error())
	}
	defer c.CacheDestroy("TestCache2", nil)
	if err = c.CacheCreateWithName("TestCache3", nil); err != nil {
		t.Fatalf("failed to create test cache: %s", err.Error())
	}
	defer c.CacheDestroy("TestCache3", nil)

	type args struct {
		status *int32
	}
	tests := []struct {
		name    string
		c       *client
		args    args
		want    []string
		wantErr bool
	}{
		{
			name: "test success",
			c:    c,
			args: args{
				status: &status,
			},
			want: []string{"TestCache1", "TestCache2", "TestCache3"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.c.CacheGetNames(tt.args.status)
			if (err != nil) != tt.wantErr {
				t.Errorf("client.CacheGetNames() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			sort.Strings(got)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("client.CacheGetNames() = %v, want %v", got, tt.want)
			}
		})
	}
}
