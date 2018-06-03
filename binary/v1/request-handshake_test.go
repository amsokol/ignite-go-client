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
			name: "1",
			r:    NewRequestHandshake(1, 0, 0, "ignite", "ignite"),
			want: 4 + 8 + 22,
			wantW: []byte{0x1e, 0x0, 0x0, 0x0, 0x1, 0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2,
				0x9, 0x6, 0x0, 0x0, 0x0, 0x69, 0x67, 0x6e, 0x69, 0x74, 0x65, 0x9, 0x6,
				0x0, 0x0, 0x0, 0x69, 0x67, 0x6e, 0x69, 0x74, 0x65},
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
				t.Errorf("RequestHandshake.WriteTo() = %#v, want %#v", gotW, tt.wantW)
			}
		})
	}
}
