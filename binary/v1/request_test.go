package ignite

import (
	"bytes"
	"reflect"
	"testing"
)

func Test_request_WriteTo(t *testing.T) {
	r := &request{payload: &bytes.Buffer{}}
	_ = WriteInt(r, 1234567890)

	tests := []struct {
		name    string
		r       *request
		want    int64
		wantW   []byte
		wantErr bool
	}{
		{
			name:  "1",
			r:     r,
			want:  4,
			wantW: []byte{0xD2, 0x02, 0x96, 0x49},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &bytes.Buffer{}
			got, err := tt.r.WriteTo(w)
			if (err != nil) != tt.wantErr {
				t.Errorf("request.WriteTo() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("request.WriteTo() = %v, want %v", got, tt.want)
			}
			if gotW := w.Bytes(); !reflect.DeepEqual(w.Bytes(), tt.wantW) {
				t.Errorf("request.WriteTo() = %#v, want %#v", gotW, tt.wantW)
			}
		})
	}
}
