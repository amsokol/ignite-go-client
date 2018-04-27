package ignite

import (
	"bytes"
	"reflect"
	"testing"
)

func TestRequestHandshake_WriteTo(t *testing.T) {
	tests := []struct {
		name    string
		r       *RequestHandshake
		want    int64
		wantW   []byte
		wantErr bool
	}{
		{
			name:  "1",
			r:     NewRequestHandshake(1, 0, 0),
			want:  4 + 8,
			wantW: []byte{0x8, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &bytes.Buffer{}
			got, err := tt.r.WriteTo(w)
			if (err != nil) != tt.wantErr {
				t.Errorf("RequestHandshake.WriteTo() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("RequestHandshake.WriteTo() = %v, want %v", got, tt.want)
			}
			if gotW := w.Bytes(); !reflect.DeepEqual(w.Bytes(), tt.wantW) {
				t.Errorf("request.WriteTo() = %#v, want %#v", gotW, tt.wantW)
			}
		})
	}
}
