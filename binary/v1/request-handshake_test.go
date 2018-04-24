package ignite

import (
	"bytes"
	"reflect"
	"testing"
)

func Test_requestHandshake_WriteTo(t *testing.T) {
	ri, _ := NewRequestHandshake(1, 0, 0)
	r, _ := ri.(*requestHandshake)

	tests := []struct {
		name    string
		r       *requestHandshake
		want    int64
		wantW   []byte
		wantErr bool
	}{
		{
			name:  "1",
			r:     r,
			want:  12,
			wantW: []byte{8, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 2},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &bytes.Buffer{}
			got, err := tt.r.WriteTo(w)
			if (err != nil) != tt.wantErr {
				t.Errorf("requestHandshake.WriteTo() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("requestHandshake.WriteTo() = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(w.Bytes(), tt.wantW) {
				t.Errorf("requestHandshake.WriteTo() = %#v, want %#v", w.Bytes(), tt.wantW)
			}
		})
	}
}

func TestNewRequestHandshake(t *testing.T) {
	type args struct {
		major int
		minor int
		patch int
	}
	tests := []struct {
		name    string
		args    args
		want    RequestHandshake
		wantErr bool
	}{
		{
			name: "1",
			args: args{
				major: 1,
				minor: 0,
				patch: 0,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewRequestHandshake(tt.args.major, tt.args.minor, tt.args.patch)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewRequestHandshake() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}
