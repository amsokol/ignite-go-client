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
	if err = c.CacheCreateWithName("TestCache1", nil); err != nil {
		t.Fatalf("failed to create test cache: %s", err.Error())
	}
	defer c.CacheDestroy("TestCache1", nil)
	var status int32

	// put test values
	testClientCachePut(t, c)
	uid, _ := uuid.Parse("d6589da7-f8b1-4687-b5bd-2ddc7362a4a4")
	tm := time.Date(2018, 4, 3, 14, 25, 32, int(time.Millisecond*123+time.Microsecond*456+789), time.UTC)

	type args struct {
		cache  string
		binary bool
		key    interface{}
		status *int32
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
				status: &status,
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
				status: &status,
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
				status: &status,
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
				status: &status,
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
				status: &status,
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
				status: &status,
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
				status: &status,
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
				status: &status,
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
				status: &status,
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
				status: &status,
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
				status: &status,
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
				status: &status,
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
				status: &status,
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
				status: &status,
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
				status: &status,
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
				status: &status,
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
				status: &status,
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
				status: &status,
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
				status: &status,
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
				status: &status,
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
					status: &status,
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
					status: &status,
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
				status: &status,
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
				status: &status,
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
				status: &status,
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.c.CacheGet(tt.args.cache, tt.args.binary, tt.args.key, tt.args.status)
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
