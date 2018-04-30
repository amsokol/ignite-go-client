package ignite

import (
	"context"
	"reflect"
	"testing"
)

func Test_client_CacheGet(t *testing.T) {
	c, err := Connect(context.Background(), "tcp", "localhost", 10800, 1, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.CachePut("CacheGet", false, "byte", byte(123))
	c.CachePut("CacheGet", false, "short", int16(12345))
	c.CachePut("CacheGet", false, "int", int32(1234567890))
	c.CachePut("CacheGet", false, "long", int64(1234567890123456789))
	c.CachePut("CacheGet", false, "float", float32(123456.789))
	c.CachePut("CacheGet", false, "double", float64(123456789.12345))
	c.CachePut("CacheGet", false, "char", Char('A'))
	c.CachePut("CacheGet", false, "bool", true)
	c.CachePut("CacheGet", false, "string", "test string")

	type args struct {
		cache  string
		binary bool
		key    interface{}
	}
	tests := []struct {
		name    string
		c       Client
		args    args
		want    interface{}
		wantErr bool
	}{
		{
			name: "byte",
			c:    c,
			args: args{
				cache: "CacheGet",
				key:   "byte",
			},
			want: byte(123),
		},
		{
			name: "short",
			c:    c,
			args: args{
				cache: "CacheGet",
				key:   "short",
			},
			want: int16(12345),
		},
		{
			name: "int",
			c:    c,
			args: args{
				cache: "CacheGet",
				key:   "int",
			},
			want: int32(1234567890),
		},
		{
			name: "long",
			c:    c,
			args: args{
				cache: "CacheGet",
				key:   "long",
			},
			want: int64(1234567890123456789),
		},
		{
			name: "float",
			c:    c,
			args: args{
				cache: "CacheGet",
				key:   "float",
			},
			want: float32(123456.789),
		},
		{
			name: "double",
			c:    c,
			args: args{
				cache: "CacheGet",
				key:   "double",
			},
			want: float64(123456789.12345),
		},
		{
			name: "char",
			c:    c,
			args: args{
				cache: "CacheGet",
				key:   "char",
			},
			want: Char('A'),
		},
		{
			name: "bool",
			c:    c,
			args: args{
				cache: "CacheGet",
				key:   "bool",
			},
			want: true,
		},
		{
			name: "string",
			c:    c,
			args: args{
				cache: "CacheGet",
				key:   "string",
			},
			want: "test string",
		},
		{
			name: "NULL",
			c:    c,
			args: args{
				cache: "CacheGet",
				key:   "NULL",
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.c.CacheGet(tt.args.cache, tt.args.binary, tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("client.CacheGet() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("client.CacheGet() = %v, want %v", got, tt.want)
			}
		})
	}
}
