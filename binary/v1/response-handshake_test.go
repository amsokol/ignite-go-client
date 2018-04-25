package ignite

import (
	"bytes"
	"io"
	"testing"
)

func TestResponseHandshake_ReadFrom(t *testing.T) {
	rr1 := bytes.NewBuffer(
		[]byte{1, 0, 0, 0, 1})
	rr2 := bytes.NewBuffer(
		[]byte{23, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0,
			9, 0x0B, 0, 0, 0, 0x74, 0x65, 0x73, 0x74, 0x20, 0x73, 0x74, 0x72, 0x69, 0x6e, 0x67})

	r1 := &ResponseHandshake{}
	r2 := &ResponseHandshake{}

	type args struct {
		rr io.Reader
	}
	tests := []struct {
		name                            string
		r                               *ResponseHandshake
		args                            args
		want                            int64
		wantSuccess                     bool
		wantMajor, wantMinor, wantPatch int
		wantMessage                     string
		wantErr                         bool
	}{
		{
			name: "1",
			r:    r1,
			args: args{
				rr: rr1,
			},
			want:        1,
			wantSuccess: true,
		},
		{
			name: "2",
			r:    r2,
			args: args{
				rr: rr2,
			},
			want:        23,
			wantSuccess: false,
			wantMajor:   1,
			wantMinor:   0,
			wantPatch:   0,
			wantMessage: "test string",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.r.ReadFrom(tt.args.rr)
			if (err != nil) != tt.wantErr {
				t.Errorf("ResponseHandshake.ReadFrom() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ResponseHandshake.ReadFrom() = %v, want %v", got, tt.want)
			}
			if tt.r.Success != tt.wantSuccess {
				t.Errorf("NewResponseHandshake() success = %v, want %v", tt.r.Success, tt.wantSuccess)
			}
			if tt.r.Major != tt.wantMajor {
				t.Errorf("NewResponseHandshake() major = %v, want %v", tt.r.Major, tt.wantMajor)
			}
			if tt.r.Minor != tt.wantMinor {
				t.Errorf("NewResponseHandshake() minor = %v, want %v", tt.r.Minor, tt.wantMinor)
			}
			if tt.r.Patch != tt.wantPatch {
				t.Errorf("NewResponseHandshake() patch = %v, want %v", tt.r.Patch, tt.wantPatch)
			}
			if tt.r.Message != tt.wantMessage {
				t.Errorf("NewResponseHandshake() message = %v, want %v", tt.r.Message, tt.wantMessage)
			}
		})
	}
}
