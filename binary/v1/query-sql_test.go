package ignite

/*
import (
	"reflect"
	"testing"
	"time"
)

func Test_client_QuerySQL(t *testing.T) {
	// get test data
	c, err := getTestClient()
	if err != nil {
		t.Fatalf("failed to open test connection: %s", err.Error())
	}
	defer c.Close()
	var status int32
	// insert test values
	tm := time.Date(2018, 4, 3, 14, 25, 32, int(time.Millisecond*123+time.Microsecond*456+789), time.UTC)
	_, err = c.QuerySQLFields("TestDB", false, QuerySQLFieldsData{
		PageSize: 10,
		Query: "INSERT INTO Organization(_key, name, foundDateTime) VALUES" +
			"(?, ?, ?)," +
			"(?, ?, ?)," +
			"(?, ?, ?)",
		QueryArgs: []interface{}{
			int64(1), "Org 1", tm,
			int64(2), "Org 2", tm,
			int64(3), "Org 3", tm},
	}, &status)
	if err != nil {
		t.Fatalf("failed to insert test data: %s", err.Error())
	}
	defer c.CacheRemoveAll("TestDB", false, nil)

	type args struct {
		cache  string
		binary bool
		data   QuerySQLData
		status *int32
	}
	tests := []struct {
		name    string
		c       *client
		args    args
		want    QuerySQLResult
		wantErr bool
	}{
		{
			name: "success test 1",
			c:    c,
			args: args{
				cache: "TestDB",
				data: QuerySQLData{
					Table:    "Organization",
					Query:    `SELECT * FROM Organization ORDER BY name ASC`,
					PageSize: 10,
					Timeout:  10000,
				},
				status: &status,
			},
			want: QuerySQLResult{
				Keys:   []interface{}{},
				Values: []interface{}{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.c.QuerySQL(tt.args.cache, tt.args.binary, tt.args.data, tt.args.status)
			if (err != nil) != tt.wantErr {
				t.Errorf("client.QuerySQL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got.Keys, tt.want.Keys) ||
				!reflect.DeepEqual(got.Values, tt.want.Values) ||
				!reflect.DeepEqual(got.HasMore, tt.want.HasMore) {
				t.Errorf("client.QuerySQL() = %v, want %v", got, tt.want)
			}
		})
	}
}
*/
