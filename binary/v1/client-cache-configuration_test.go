package ignite

import (
	"context"
	"reflect"
	"testing"
)

func Test_client_CacheCreateWithName(t *testing.T) {
	c, err := Connect(context.Background(), "tcp", "localhost", 10800, 1, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	type args struct {
		cache string
	}
	tests := []struct {
		name    string
		c       Client
		args    args
		wantErr bool
	}{
		{
			name: "1",
			c:    c,
			args: args{
				cache: "CacheCreateWithName",
			},
		},
		{
			name: "2",
			c:    c,
			args: args{
				cache: "CacheCreateWithName",
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
}

func Test_client_CacheGetOrCreateWithName(t *testing.T) {
	c, err := Connect(context.Background(), "tcp", "localhost", 10800, 1, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	type args struct {
		cache string
	}
	tests := []struct {
		name    string
		c       Client
		args    args
		wantErr bool
	}{
		{
			name: "1",
			c:    c,
			args: args{
				cache: "CacheGetOrCreateWithName",
			},
		},
		{
			name: "2",
			c:    c,
			args: args{
				cache: "CacheGetOrCreateWithName",
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
}

func Test_client_CacheGetNames(t *testing.T) {
	c, err := Connect(context.Background(), "tcp", "localhost", 10800, 1, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	tests := []struct {
		name    string
		c       Client
		want    string
		wantErr bool
	}{
		{
			name: "1",
			c:    c,
			want: "CacheGetNames",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.c.CacheGetNames()
			if (err != nil) != tt.wantErr {
				t.Errorf("client.CacheGetNames() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			var found bool
			for _, v := range got {
				if v == tt.want {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("client.CacheGetNames() , want \"%v\", but not found", tt.want)
			}
		})
	}
}

func Test_client_CacheGetConfiguration(t *testing.T) {
	c, err := Connect(context.Background(), "tcp", "localhost", 10800, 1, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	type args struct {
		cache string
		flag  byte
	}
	tests := []struct {
		name    string
		c       Client
		args    args
		want    *CacheConfiguration
		wantErr bool
	}{
		{
			name: "1",
			c:    c,
			args: args{
				cache: "CacheGetConfiguration",
			},
			want: &CacheConfiguration{
				AtomicityMode:                 0,
				Backups:                       0,
				CacheMode:                     2,
				CopyOnRead:                    true,
				DataRegionName:                "",
				EagerTTL:                      true,
				EnableStatistics:              false,
				GroupName:                     "",
				LockTimeout:                   0,
				MaxConcurrentAsyncOperations:  500,
				MaxQueryIterators:             1024,
				Name:                          "CacheGetConfiguration",
				OnheapCacheEnabled:            false,
				PartitionLossPolicy:           4,
				QueryDetailMetricsSize:        0,
				QueryParellelism:              1,
				ReadFromBackup:                true,
				RebalanceBatchSize:            524288,
				RebalanceBatchesPrefetchCount: 2,
				RebalanceDelay:                0,
				RebalanceMode:                 1,
				RebalanceOrder:                0,
				RebalanceThrottle:             0,
				RebalanceTimeout:              10000,
				SQLEscapeAll:                  false,
				SQLIndexInlineMaxSize:         -1,
				SQLSchema:                     "",
				WriteSynchronizationMode:      0,
				CacheKeyConfigurations:        []CacheKeyConfiguration{},
				QueryEntities:                 []QueryEntity{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.c.CacheGetConfiguration(tt.args.cache, tt.args.flag)
			if (err != nil) != tt.wantErr {
				t.Errorf("client.CacheGetConfiguration() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("client.CacheGetConfiguration() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func Test_client_CacheCreateWithConfiguration(t *testing.T) {
	c, err := Connect(context.Background(), "tcp", "localhost", 10800, 1, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	cache := "CacheCreateWithConfiguration"

	type args struct {
		cc *CacheConfigurationRefs
	}
	tests := []struct {
		name    string
		c       Client
		args    args
		wantErr bool
	}{
		{
			name: "1",
			c:    c,
			args: args{
				cc: &CacheConfigurationRefs{
					Name: &cache,
				},
			},
		},
		{
			name: "2",
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
}

func Test_client_CacheGetOrCreateWithConfiguration(t *testing.T) {
	c, err := Connect(context.Background(), "tcp", "localhost", 10800, 1, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	cache := "CacheGetOrCreateWithConfiguration"

	type args struct {
		cc *CacheConfigurationRefs
	}
	tests := []struct {
		name    string
		c       Client
		args    args
		wantErr bool
	}{
		{
			name: "1",
			c:    c,
			args: args{
				cc: &CacheConfigurationRefs{
					Name: &cache,
				},
			},
		},
		{
			name: "2",
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
}

func Test_client_CacheDestroy(t *testing.T) {
	c, err := Connect(context.Background(), "tcp", "localhost", 10800, 1, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	type args struct {
		cache string
	}
	tests := []struct {
		name    string
		c       Client
		args    args
		wantErr bool
	}{
		{
			name: "1",
			c:    c,
			args: args{
				cache: "CacheDestroy",
			},
		},
		{
			name: "1",
			c:    c,
			args: args{
				cache: "CacheDestroy",
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
