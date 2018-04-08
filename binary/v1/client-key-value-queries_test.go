package ignite

import (
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
)

func Test_client_CacheGet(t *testing.T) {
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

	// put test values
	testClientCachePut(t, c)
	uid, _ := uuid.Parse("d6589da7-f8b1-4687-b5bd-2ddc7362a4a4")
	tm := time.Date(2018, 4, 3, 14, 25, 32, int(time.Millisecond*123+time.Microsecond*456+789), time.UTC)

	type args struct {
		cache  string
		binary bool
		key    interface{}
	}
	tests := []struct {
		name    string
		c       *client
		args    args
		want    interface{}
		wantErr bool
	}{
		{
			name: "success test 1",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key1",
			},
			want: byte(123),
		},
		{
			name: "success test 2",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key2",
			},
			want: int16(1234),
		},
		{
			name: "success test 3",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key3",
			},
			want: int32(1234),
		},
		{
			name: "success test 4",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key4",
			},
			want: int64(123456789),
		},
		{
			name: "success test 5",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key5",
			},
			want: float32(1.123),
		},
		{
			name: "success test 6",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key6",
			},
			want: float64(1.123456),
		},
		{
			name: "success test 7",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key7",
			},
			want: Char('W'),
		},
		{
			name: "success test 8",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key8",
			},
			want: true,
		},
		{
			name: "success test 9",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key9",
			},
			want: "value",
		},
		{
			name: "success test 10",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key10",
			},
			want: uid,
		},
		{
			name: "success test 11",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key11",
			},
			want: NativeTime2Date(tm),
		},
		{
			name: "success test 12",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key12",
			},
			want: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		},
		{
			name: "success test 13",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key13",
			},
			want: []int16{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		},
		{
			name: "success test 14",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key14",
			},
			want: []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		},
		{
			name: "success test 15",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key15",
			},
			want: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		},
		{
			name: "success test 16",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key16",
			},
			want: []float32{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0},
		},
		{
			name: "success test 17",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key17",
			},
			want: []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0},
		},
		{
			name: "success test 18",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key18",
			},
			want: []Char{'a', 'b', 'c'},
		},
		{
			name: "success test 19",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key19",
			},
			want: []bool{true, false},
		},
		{
			name: "success test 20",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key20",
			},
			want: []string{"abc", "def"},
		},
		/*
			{
				name: "success test 21",
				c:    c,
				args: args{
					cache:  "TestCache1",
					binary: false,
					key:    "key21",
				},
				want: []uuid.UUID{uid1, uid2},
			},
		*/
		/*
			{
				name: "success test 22",
				c:    c,
				args: args{
					cache:  "TestCache1",
					binary: false,
					key:    "key22",
				},
				want: []Date{12345, 67890},
			},
		*/
		{
			name: "success test 33",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key33",
			},
			want: tm,
		},
		{
			name: "success test 36",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key36",
			},
			want: NativeTime2Time(tm),
		},
		{
			name: "success test NULL",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key-does-not-exist",
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

func Test_client_CacheGetAll(t *testing.T) {
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

	testClientCachePutAll(t, c)

	type args struct {
		cache  string
		binary bool
		keys   []interface{}
	}
	tests := []struct {
		name    string
		c       *client
		args    args
		want    map[interface{}]interface{}
		wantErr bool
	}{
		{
			name: "success test 1",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				keys: []interface{}{
					byte(123),
					float64(678),
					"key",
				},
			},
			want: map[interface{}]interface{}{
				byte(123):    "test",
				float64(678): Char('w'),
				"key":        int64(128),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.c.CacheGetAll(tt.args.cache, tt.args.binary, tt.args.keys)
			if (err != nil) != tt.wantErr {
				t.Errorf("client.CacheGetAll() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("client.CacheGetAll() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_client_CachePut(t *testing.T) {
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

	testClientCachePut(t, c)
}

func testClientCachePut(t *testing.T, c *client) {
	uid, _ := uuid.Parse("d6589da7-f8b1-4687-b5bd-2ddc7362a4a4")
	tm := time.Date(2018, 4, 3, 14, 25, 32, int(time.Millisecond*123+time.Microsecond*456+789), time.UTC)

	type args struct {
		cache  string
		binary bool
		key    interface{}
		value  interface{}
	}
	tests := []struct {
		name    string
		c       *client
		args    args
		wantErr bool
	}{
		{
			name: "success test 1",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key1",
				value:  byte(123),
			},
		},
		{
			name: "success test 2",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key2",
				value:  int16(1234),
			},
		},
		{
			name: "success test 3",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key3",
				value:  int32(1234),
			},
		},
		{
			name: "success test 4",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key4",
				value:  int64(123456789),
			},
		},
		{
			name: "success test 5",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key5",
				value:  float32(1.123),
			},
		},
		{
			name: "success test 6",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key6",
				value:  float64(1.123456),
			},
		},
		{
			name: "success test 7",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key7",
				value:  Char('W'),
			},
		},
		{
			name: "success test 8",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key8",
				value:  true,
			},
		},
		{
			name: "success test 9",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key9",
				value:  "value",
			},
		},
		{
			name: "success test 10",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key10",
				value:  uid,
			},
		},
		{
			name: "success test 11",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key11",
				value:  NativeTime2Date(tm),
			},
		},
		{
			name: "success test 12",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key12",
				value:  []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			},
		},
		{
			name: "success test 13",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key13",
				value:  []int16{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			},
		},
		{
			name: "success test 14",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key14",
				value:  []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			},
		},
		{
			name: "success test 15",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key15",
				value:  []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			},
		},
		{
			name: "success test 16",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key16",
				value:  []float32{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0},
			},
		},
		{
			name: "success test 17",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key17",
				value:  []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0},
			},
		},
		{
			name: "success test 18",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key18",
				value:  []Char{'a', 'b', 'c'},
			},
		},
		{
			name: "success test 19",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key19",
				value:  []bool{true, false},
			},
		},
		{
			name: "success test 20",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key20",
				value:  []string{"abc", "def"},
			},
		},
		/*
			{
				name: "success test 21",
				c:    c,
				args: args{
					cache:  "TestCache1",
					binary: false,
					key:    "key21",
					value:  []uuid.UUID{uid1, uid2},
					},
			},
		*/
		/*
			{
				name: "success test 22",
				c:    c,
				args: args{
					cache:  "TestCache1",
					binary: false,
					key:    "key22",
					value:  []Date{12345, 67890},
					},
			},
		*/
		{
			name: "success test 33",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key33",
				value:  tm,
			},
		},
		{
			name: "success test 36",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key36",
				value:  NativeTime2Time(tm),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.c.CachePut(tt.args.cache, tt.args.binary, tt.args.key, tt.args.value); (err != nil) != tt.wantErr {
				t.Errorf("client.CachePut() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_client_CachePutAll(t *testing.T) {
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

	testClientCachePutAll(t, c)
}

func testClientCachePutAll(t *testing.T, c *client) {
	type args struct {
		cache  string
		binary bool
		data   map[interface{}]interface{}
	}
	tests := []struct {
		name    string
		c       *client
		args    args
		wantErr bool
	}{
		{
			name: "success test 1",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				data: map[interface{}]interface{}{
					byte(123):    "test",
					float64(678): Char('w'),
					"key":        int64(128),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.c.CachePutAll(tt.args.cache, tt.args.binary, tt.args.data); (err != nil) != tt.wantErr {
				t.Errorf("client.CachePutAll() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_client_CacheContainsKey(t *testing.T) {
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

	// put test values
	if err = c.CachePut("TestCache1", false, "key", "value"); err != nil {
		t.Fatalf("failed to put test pair: %s", err.Error())
	}

	type args struct {
		cache  string
		binary bool
		key    interface{}
	}
	tests := []struct {
		name    string
		c       *client
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "success test 1",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key",
			},
			want: true,
		},
		{
			name: "success test 2",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key-not-found",
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.c.CacheContainsKey(tt.args.cache, tt.args.binary, tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("client.CacheContainsKey() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("client.CacheContainsKey() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_client_CacheContainsKeys(t *testing.T) {
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

	// put test values
	if err = c.CachePut("TestCache1", false, "key1", "value1"); err != nil {
		t.Fatalf("failed to put test pair: %s", err.Error())
	}
	if err = c.CachePut("TestCache1", false, "key2", "value1"); err != nil {
		t.Fatalf("failed to put test pair: %s", err.Error())
	}

	type args struct {
		cache  string
		binary bool
		keys   []interface{}
	}
	tests := []struct {
		name    string
		c       *client
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "success test 1",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				keys:   []interface{}{"key1", "key2"},
			},
			want: true,
		},
		{
			name: "success test 2",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				keys:   []interface{}{"key1", "key-not-found"},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.c.CacheContainsKeys(tt.args.cache, tt.args.binary, tt.args.keys)
			if (err != nil) != tt.wantErr {
				t.Errorf("client.CacheContainsKeys() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("client.CacheContainsKeys() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_client_CacheGetAndPut(t *testing.T) {
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

	// put test values
	if err = c.CachePut("TestCache1", false, "key", "value 1"); err != nil {
		t.Fatalf("failed to put test pair: %s", err.Error())
	}

	type args struct {
		cache  string
		binary bool
		key    interface{}
		value  interface{}
	}
	tests := []struct {
		name    string
		c       *client
		args    args
		want    interface{}
		wantErr bool
	}{
		{
			name: "success test 1",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key",
				value:  "value 2",
			},
			want: "value 1",
		},
		{
			name: "success test 2",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key-not-exist",
				value:  "value",
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.c.CacheGetAndPut(tt.args.cache, tt.args.binary, tt.args.key, tt.args.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("client.CacheGetAndPut() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("client.CacheGetAndPut() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_client_CacheGetAndReplace(t *testing.T) {
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

	// put test values
	if err = c.CachePut("TestCache1", false, "key", "value 1"); err != nil {
		t.Fatalf("failed to put test pair: %s", err.Error())
	}

	type args struct {
		cache  string
		binary bool
		key    interface{}
		value  interface{}
	}
	tests := []struct {
		name    string
		c       *client
		args    args
		want    interface{}
		wantErr bool
	}{
		{
			name: "success test 1",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key",
				value:  "value 2",
			},
			want: "value 1",
		},
		{
			name: "success test 2",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key-not-exist",
				value:  "value",
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.c.CacheGetAndReplace(tt.args.cache, tt.args.binary, tt.args.key, tt.args.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("client.CacheGetAndReplace() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("client.CacheGetAndReplace() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_client_CacheGetAndRemove(t *testing.T) {
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

	// put test values
	if err = c.CachePut("TestCache1", false, "key", "value 1"); err != nil {
		t.Fatalf("failed to put test pair: %s", err.Error())
	}

	type args struct {
		cache  string
		binary bool
		key    interface{}
	}
	tests := []struct {
		name    string
		c       *client
		args    args
		want    interface{}
		wantErr bool
	}{
		{
			name: "success test 1",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key",
			},
			want: "value 1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.c.CacheGetAndRemove(tt.args.cache, tt.args.binary, tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("client.CacheGetAndRemove() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("client.CacheGetAndRemove() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_client_CachePutIfAbsent(t *testing.T) {
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

	// put test values
	if err = c.CachePut("TestCache1", false, "key2", "value 2"); err != nil {
		t.Fatalf("failed to put test pair: %s", err.Error())
	}

	type args struct {
		cache  string
		binary bool
		key    interface{}
		value  interface{}
	}
	tests := []struct {
		name    string
		c       *client
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "success test 1",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key1",
				value:  byte(123),
			},
			want: true,
		},
		{
			name: "success test 2",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key2",
				value:  byte(45),
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.c.CachePutIfAbsent(tt.args.cache, tt.args.binary, tt.args.key, tt.args.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("client.CachePutIfAbsent() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("client.CachePutIfAbsent() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_client_CacheReplace(t *testing.T) {
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

	// put test values
	if err = c.CachePut("TestCache1", false, "key", "value 1"); err != nil {
		t.Fatalf("failed to put test pair: %s", err.Error())
	}

	type args struct {
		cache  string
		binary bool
		key    interface{}
		value  interface{}
	}
	tests := []struct {
		name    string
		c       *client
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "success test 1",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key",
				value:  "value 2",
			},
			want: true,
		},
		{
			name: "success test 2",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key-not-found",
				value:  "value 3",
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.c.CacheReplace(tt.args.cache, tt.args.binary, tt.args.key, tt.args.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("client.CacheReplace() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("client.CacheReplace() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_client_CacheReplaceIfEquals(t *testing.T) {
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

	// put test values
	if err = c.CachePut("TestCache1", false, "key", "value 1"); err != nil {
		t.Fatalf("failed to put test pair: %s", err.Error())
	}

	type args struct {
		cache        string
		binary       bool
		key          interface{}
		valueCompare interface{}
		valueNew     interface{}
	}
	tests := []struct {
		name    string
		c       *client
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "success test 1",
			c:    c,
			args: args{
				cache:        "TestCache1",
				binary:       false,
				key:          "key",
				valueCompare: "value 1",
				valueNew:     "value 2",
			},
			want: true,
		},
		{
			name: "success test 1",
			c:    c,
			args: args{
				cache:        "TestCache1",
				binary:       false,
				key:          "key",
				valueCompare: "value 1",
				valueNew:     "value 3",
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.c.CacheReplaceIfEquals(tt.args.cache, tt.args.binary, tt.args.key, tt.args.valueCompare, tt.args.valueNew)
			if (err != nil) != tt.wantErr {
				t.Errorf("client.CacheReplaceIfEquals() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("client.CacheReplaceIfEquals() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_client_CacheGetAndPutIfAbsent(t *testing.T) {
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

	// put test values
	if err = c.CachePut("TestCache1", false, "key", "value 1"); err != nil {
		t.Fatalf("failed to put test pair: %s", err.Error())
	}

	type args struct {
		cache  string
		binary bool
		key    interface{}
		value  interface{}
	}
	tests := []struct {
		name    string
		c       *client
		args    args
		want    interface{}
		wantErr bool
	}{
		{
			name: "success test 1",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key",
				value:  "value 2",
			},
			want: "value 1",
		},
		{
			name: "success test 2",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key-not-exist",
				value:  "value",
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.c.CacheGetAndPutIfAbsent(tt.args.cache, tt.args.binary, tt.args.key, tt.args.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("client.CacheGetAndPutIfAbsent() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("client.CacheGetAndPutIfAbsent() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_client_CacheClear(t *testing.T) {
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

	// put test values
	if err = c.CachePut("TestCache1", false, "key", "value"); err != nil {
		t.Fatalf("failed to put test pair: %s", err.Error())
	}

	type args struct {
		cache  string
		binary bool
	}
	tests := []struct {
		name    string
		c       *client
		args    args
		wantErr bool
	}{
		{
			name: "success test 1",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.c.CacheClear(tt.args.cache, tt.args.binary); (err != nil) != tt.wantErr {
				t.Errorf("client.CacheClear() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_client_CacheClearKey(t *testing.T) {
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

	// put test values
	if err = c.CachePut("TestCache1", false, "key", "value"); err != nil {
		t.Fatalf("failed to put test pair: %s", err.Error())
	}

	type args struct {
		cache  string
		binary bool
		key    interface{}
	}
	tests := []struct {
		name    string
		c       *client
		args    args
		wantErr bool
	}{
		{
			name: "success test 1",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.c.CacheClearKey(tt.args.cache, tt.args.binary, tt.args.key); (err != nil) != tt.wantErr {
				t.Errorf("client.CacheClearKey() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_client_CacheClearKeys(t *testing.T) {
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

	// put test values
	if err = c.CachePut("TestCache1", false, "key1", "value1"); err != nil {
		t.Fatalf("failed to put test pair: %s", err.Error())
	}
	if err = c.CachePut("TestCache1", false, "key2", "value1"); err != nil {
		t.Fatalf("failed to put test pair: %s", err.Error())
	}

	type args struct {
		cache  string
		binary bool
		keys   []interface{}
	}
	tests := []struct {
		name    string
		c       *client
		args    args
		wantErr bool
	}{
		{
			name: "success test 1",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				keys:   []interface{}{"key1", "key2"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.c.CacheClearKeys(tt.args.cache, tt.args.binary, tt.args.keys); (err != nil) != tt.wantErr {
				t.Errorf("client.CacheClearKeys() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_client_CacheRemoveKey(t *testing.T) {
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

	// put test values
	if err = c.CachePut("TestCache1", false, "key", "value"); err != nil {
		t.Fatalf("failed to put test pair: %s", err.Error())
	}

	type args struct {
		cache  string
		binary bool
		key    interface{}
	}
	tests := []struct {
		name    string
		c       *client
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "success test 1",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key",
			},
			want: true,
		},
		{
			name: "success test 1",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key",
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.c.CacheRemoveKey(tt.args.cache, tt.args.binary, tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("client.CacheRemoveKey() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("client.CacheRemoveKey() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_client_CacheRemoveIfEquals(t *testing.T) {
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

	// put test values
	if err = c.CachePut("TestCache1", false, "key", "value"); err != nil {
		t.Fatalf("failed to put test pair: %s", err.Error())
	}

	type args struct {
		cache  string
		binary bool
		key    interface{}
		value  interface{}
	}
	tests := []struct {
		name    string
		c       *client
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "success test 1",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key",
				value:  "invalid value",
			},
			want: false,
		},
		{
			name: "success test 1",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key",
				value:  "value",
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.c.CacheRemoveIfEquals(tt.args.cache, tt.args.binary, tt.args.key, tt.args.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("client.CacheRemoveIfEquals() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("client.CacheRemoveIfEquals() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_client_CacheGetSize(t *testing.T) {
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

	// put test values
	if err = c.CachePut("TestCache1", false, "key", "value"); err != nil {
		t.Fatalf("failed to put test pair: %s", err.Error())
	}

	type args struct {
		cache  string
		binary bool
		count  int
		modes  []byte
	}
	tests := []struct {
		name    string
		c       *client
		args    args
		want    int64
		wantErr bool
	}{
		{
			name: "success test 1",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				count:  0,
				modes:  []byte{0},
			},
			want: 1,
		},
		{
			name: "success test 1",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				count:  0,
				modes:  nil,
			},
			want: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.c.CacheGetSize(tt.args.cache, tt.args.binary, tt.args.count, tt.args.modes)
			if (err != nil) != tt.wantErr {
				t.Errorf("client.CacheGetSize() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("client.CacheGetSize() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_client_CacheRemoveKeys(t *testing.T) {
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

	// put test values
	if err = c.CachePut("TestCache1", false, "key1", "value1"); err != nil {
		t.Fatalf("failed to put test pair: %s", err.Error())
	}
	if err = c.CachePut("TestCache1", false, "key2", "value1"); err != nil {
		t.Fatalf("failed to put test pair: %s", err.Error())
	}

	type args struct {
		cache  string
		binary bool
		keys   []interface{}
	}
	tests := []struct {
		name    string
		c       *client
		args    args
		wantErr bool
	}{
		{
			name: "success test 1",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				keys:   []interface{}{"key1", "key2"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.c.CacheRemoveKeys(tt.args.cache, tt.args.binary, tt.args.keys); (err != nil) != tt.wantErr {
				t.Errorf("client.CacheRemoveKeys() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_client_CacheRemoveAll(t *testing.T) {
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

	// put test values
	if err = c.CachePut("TestCache1", false, "key1", "value1"); err != nil {
		t.Fatalf("failed to put test pair: %s", err.Error())
	}
	if err = c.CachePut("TestCache1", false, "key2", "value1"); err != nil {
		t.Fatalf("failed to put test pair: %s", err.Error())
	}

	type args struct {
		cache  string
		binary bool
	}
	tests := []struct {
		name    string
		c       *client
		args    args
		wantErr bool
	}{
		{
			name: "success test 1",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.c.CacheRemoveAll(tt.args.cache, tt.args.binary); (err != nil) != tt.wantErr {
				t.Errorf("client.CacheRemoveAll() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
