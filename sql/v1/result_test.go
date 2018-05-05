package v1

import (
	"database/sql/driver"
	"reflect"
	"testing"
)

func Test_newResult(t *testing.T) {
	type args struct {
		ra int64
	}
	tests := []struct {
		name    string
		args    args
		want    driver.Result
		wantErr bool
	}{
		{
			name: "success test 1",
			args: args{
				ra: 1,
			},
			want: &result{ra: 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := newResult(tt.args.ra)
			if (err != nil) != tt.wantErr {
				t.Errorf("newResult() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newResult() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_result_RowsAffected(t *testing.T) {
	ri, _ := newResult(1)
	r, _ := ri.(*result)

	tests := []struct {
		name    string
		r       *result
		want    int64
		wantErr bool
	}{
		{
			name: "success test 1",
			r:    r,
			want: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.r.RowsAffected()
			if (err != nil) != tt.wantErr {
				t.Errorf("result.RowsAffected() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("result.RowsAffected() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_result_LastInsertId(t *testing.T) {
	ri, _ := newResult(1)
	r, _ := ri.(*result)

	tests := []struct {
		name    string
		r       *result
		want    int64
		wantErr bool
	}{
		{
			name:    "failed test 1",
			r:       r,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.r.LastInsertId()
			if (err != nil) != tt.wantErr {
				t.Errorf("result.LastInsertId() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("result.LastInsertId() = %v, want %v", got, tt.want)
			}
		})
	}
}
