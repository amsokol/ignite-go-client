package ignite

import (
	"bytes"
	"testing"
)

func TestRequestOperation_WriteTo(t *testing.T) {
	tests := []struct {
		name    string
		r       *RequestOperation
		want    int64
		wantErr bool
	}{
		{
			name: "1",
			r:    NewRequestOperation(0),
			want: 4 + 2 + 8,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &bytes.Buffer{}
			got, err := tt.r.WriteTo(w)
			if (err != nil) != tt.wantErr {
				t.Errorf("RequestOperation.WriteTo() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("RequestOperation.WriteTo() = %v, want %v", got, tt.want)
			}
		})
	}
}
