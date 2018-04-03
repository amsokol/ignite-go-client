package ignite

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func Test_client_CachePut(t *testing.T) {
	// get test data
	c, err := getTestClient()
	if err != nil {
		t.Fatalf("failed to open test connection: %s", err.Error())
	}
	defer c.Close()
	if err = c.CacheCreateWithName("TestCache1", nil); err != nil {
		t.Fatalf("failed to create test cache: %s", err.Error())
	}
	defer c.CacheDestroy("TestCache1", nil)

	testClientCachePut(t, c)
}

func testClientCachePut(t *testing.T, c *client) {
	var status int32
	uid, _ := uuid.Parse("d6589da7-f8b1-4687-b5bd-2ddc7362a4a4")
	tm := time.Date(2018, 4, 3, 14, 25, 32, int(time.Millisecond*123+time.Microsecond*456+789), time.UTC)

	type args struct {
		cache  string
		binary bool
		key    interface{}
		value  interface{}
		status *int32
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
				status: &status,
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
				status: &status,
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
				status: &status,
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
				status: &status,
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
				status: &status,
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
				status: &status,
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
				status: &status,
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
				status: &status,
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
				status: &status,
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
				status: &status,
			},
		},
		{
			name: "success test 11",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key11",
				value:  Time2IgniteDate(tm),
				status: &status,
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
				status: &status,
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
				status: &status,
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
				status: &status,
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
				status: &status,
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
				status: &status,
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
				status: &status,
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
				status: &status,
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
				status: &status,
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
				status: &status,
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
					status: &status,
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
					status: &status,
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
				status: &status,
			},
		},
		{
			name: "success test 36",
			c:    c,
			args: args{
				cache:  "TestCache1",
				binary: false,
				key:    "key36",
				value:  Time2IgniteTime(tm),
				status: &status,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.c.CachePut(tt.args.cache, tt.args.binary, tt.args.key, tt.args.value, tt.args.status); (err != nil) != tt.wantErr {
				t.Errorf("client.CachePut() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
