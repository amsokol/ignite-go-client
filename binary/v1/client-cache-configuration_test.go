package ignite

import (
	"reflect"
	"sort"
	"testing"
)

func Test_client_CacheCreateWithName(t *testing.T) {
	// get test data
	c, err := getTestClient()
	if err != nil {
		t.Fatalf("failed to open test connection: %s", err.Error())
	}
	defer c.Close()

	type args struct {
		cache string
	}
	tests := []struct {
		name    string
		c       *client
		args    args
		wantErr bool
	}{
		{
			name: "success test",
			c:    c,
			args: args{
				cache: "TestCache1",
			},
		},
		{
			name: "failed test",
			c:    c,
			args: args{
				cache: "TestCache1",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.c.CacheCreateWithName(tt.args.cache); (err != nil) != tt.wantErr {
				t.Errorf("client.CacheCreateWithName() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}

	// clear test data
	c.CacheDestroy("TestCache1")
}

func Test_client_CacheGetOrCreateWithName(t *testing.T) {
	// get test data
	c, err := getTestClient()
	if err != nil {
		t.Fatalf("failed to open test connection: %s", err.Error())
	}
	defer c.Close()

	type args struct {
		cache string
	}
	tests := []struct {
		name    string
		c       *client
		args    args
		wantErr bool
	}{
		{
			name: "success test",
			c:    c,
			args: args{
				cache: "TestCache1",
			},
		},
		{
			name: "success test",
			c:    c,
			args: args{
				cache: "TestCache1",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.c.CacheGetOrCreateWithName(tt.args.cache); (err != nil) != tt.wantErr {
				t.Errorf("client.CacheGetOrCreateWithName() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}

	// clear test data
	c.CacheDestroy("TestCache1")
}

func Test_client_CacheGetNames(t *testing.T) {
	// get test data
	c, err := getTestClient()
	if err != nil {
		t.Fatalf("failed to open test connection: %s", err.Error())
	}
	defer c.Close()
	if err = c.CacheCreateWithName("TestCache1"); err != nil {
		t.Fatalf("failed to create test cache: %s", err.Error())
	}
	defer c.CacheDestroy("TestCache1")
	if err = c.CacheCreateWithName("TestCache2"); err != nil {
		t.Fatalf("failed to create test cache: %s", err.Error())
	}
	defer c.CacheDestroy("TestCache2")
	if err = c.CacheCreateWithName("TestCache3"); err != nil {
		t.Fatalf("failed to create test cache: %s", err.Error())
	}
	defer c.CacheDestroy("TestCache3")

	tests := []struct {
		name    string
		c       *client
		want    []string
		wantErr bool
	}{
		{
			name: "test success",
			c:    c,
			want: []string{"TestCache1", "TestCache2", "TestCache3", "TestDB1", "TestDB2", "TestDB3"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.c.CacheGetNames()
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

func Test_client_CacheGetConfiguration(t *testing.T) {
	// get test data
	c, err := getTestClient()
	if err != nil {
		t.Fatalf("failed to open test connection: %s", err.Error())
	}
	defer c.Close()
	if err = c.CacheCreateWithName("TestCache1"); err != nil {
		t.Fatalf("failed to create test cache: %s", err.Error())
	}
	defer c.CacheDestroy("TestCache1")

	type args struct {
		cache string
		flag  byte
	}
	tests := []struct {
		name    string
		c       *client
		args    args
		want    *CacheConfiguration
		wantErr bool
	}{
		{
			name: "success test",
			c:    c,
			args: args{
				cache: "TestCache1",
				flag:  0,
			},
		},
		{
			name: "failed test",
			c:    c,
			args: args{
				cache: "TestCache2",
				flag:  0,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.c.CacheGetConfiguration(tt.args.cache, tt.args.flag)
			if (err != nil) != tt.wantErr {
				t.Errorf("client.CacheGetConfiguration() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func Test_client_CacheCreateWithConfiguration(t *testing.T) {
	// get test data
	c, err := getTestClient()
	if err != nil {
		t.Fatalf("failed to open test connection: %s", err.Error())
	}
	defer c.Close()
	cache := "TestCache1"

	type args struct {
		cc *CacheConfigurationRefs
	}
	tests := []struct {
		name    string
		c       *client
		args    args
		wantErr bool
	}{
		{
			name: "success test",
			c:    c,
			args: args{
				cc: &CacheConfigurationRefs{
					Name: &cache,
				},
			},
		},
		{
			name: "failed test",
			c:    c,
			args: args{
				cc: &CacheConfigurationRefs{
					Name: &cache,
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.c.CacheCreateWithConfiguration(tt.args.cc); (err != nil) != tt.wantErr {
				t.Errorf("client.CacheCreateWithConfiguration() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}

	// clear test data
	c.CacheDestroy(cache)
}

func Test_client_CacheGetOrCreateWithConfiguration(t *testing.T) {
	// get test data
	c, err := getTestClient()
	if err != nil {
		t.Fatalf("failed to open test connection: %s", err.Error())
	}
	defer c.Close()
	cache := "TestCache1"

	type args struct {
		cc *CacheConfigurationRefs
	}
	tests := []struct {
		name    string
		c       *client
		args    args
		wantErr bool
	}{
		{
			name: "success test",
			c:    c,
			args: args{
				cc: &CacheConfigurationRefs{
					Name: &cache,
				},
			},
		},
		{
			name: "success test",
			c:    c,
			args: args{
				cc: &CacheConfigurationRefs{
					Name: &cache,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.c.CacheGetOrCreateWithConfiguration(tt.args.cc); (err != nil) != tt.wantErr {
				t.Errorf("client.CacheGetOrCreateWithConfiguration() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}

	// clear test data
	c.CacheDestroy(cache)
}

func Test_client_CacheDestroy(t *testing.T) {
	// get test data
	c, err := getTestClient()
	if err != nil {
		t.Fatalf("failed to open test connection: %s", err.Error())
	}
	defer c.Close()
	if err = c.CacheCreateWithName("TestCache1"); err != nil {
		t.Fatalf("failed to create test cache: %s", err.Error())
	}

	type args struct {
		cache string
	}
	tests := []struct {
		name    string
		c       *client
		args    args
		wantErr bool
	}{
		{
			name: "success test",
			c:    c,
			args: args{
				cache: "TestCache1",
			},
		},
		{
			name: "failed test",
			c:    c,
			args: args{
				cache: "TestCache1",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.c.CacheDestroy(tt.args.cache); (err != nil) != tt.wantErr {
				t.Errorf("client.CacheDestroy() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
