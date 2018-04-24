package ignite

import (
	"bytes"
	"io"
	"testing"
)

func TestNewResponseHandshake(t *testing.T) {
	r1 := bytes.NewBuffer(
		[]byte{1, 0, 0, 0, 1})
	r2 := bytes.NewBuffer(
		[]byte{23, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0,
			9, 0x0B, 0, 0, 0, 0x74, 0x65, 0x73, 0x74, 0x20, 0x73, 0x74, 0x72, 0x69, 0x6e, 0x67})

	type args struct {
		r io.Reader
	}
	tests := []struct {
		name                            string
		args                            args
		wantSuccess                     bool
		wantMajor, wantMinor, wantPatch int
		wantMessage                     string
		wantErr                         bool
	}{
		{
			name: "1",
			args: args{
				r: r1,
			},
			wantSuccess: true,
		},
		{
			name: "2",
			args: args{
				r: r2,
			},
			wantSuccess: false,
			wantMajor:   1,
			wantMinor:   0,
			wantPatch:   0,
			wantMessage: "test string",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewResponseHandshake(tt.args.r)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewResponseHandshake() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got.Success() != tt.wantSuccess {
				t.Errorf("NewResponseHandshake() success = %v, want %v", got.Success(), tt.wantSuccess)
			}
			if got.Major() != tt.wantMajor {
				t.Errorf("NewResponseHandshake() major = %v, want %v", got.Major(), tt.wantMajor)
			}
			if got.Minor() != tt.wantMinor {
				t.Errorf("NewResponseHandshake() minor = %v, want %v", got.Minor(), tt.wantMinor)
			}
			if got.Patch() != tt.wantPatch {
				t.Errorf("NewResponseHandshake() patch = %v, want %v", got.Patch(), tt.wantPatch)
			}
			if got.Message() != tt.wantMessage {
				t.Errorf("NewResponseHandshake() message = %v, want %v", got.Message(), tt.wantMessage)
			}
		})
	}
}
