package ignite

import (
	"bytes"
	"io"
	"testing"
)

func TestResponseOperation_ReadFrom(t *testing.T) {
	rr1 := bytes.NewBuffer(
		[]byte{12, 0, 0, 0,
			1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	rr2 := bytes.NewBuffer(
		[]byte{28, 0, 0, 0,
			2, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0,
			9, 0x0B, 0, 0, 0, 0x74, 0x65, 0x73, 0x74, 0x20, 0x73, 0x74, 0x72, 0x69, 0x6e, 0x67})
	rr3 := bytes.NewBuffer(
		[]byte{12, 0, 0, 0,
			3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})

	r1 := NewResponseOperation(1)
	r2 := NewResponseOperation(2)
	r3 := NewResponseOperation(0)

	type args struct {
		rr io.Reader
	}
	tests := []struct {
		name        string
		r           *ResponseOperation
		args        args
		want        int64
		wantUID     int64
		wantStatus  int32
		wantMessage string
		wantErr     bool
	}{
		{
			name: "1",
			r:    r1,
			args: args{
				rr: rr1,
			},
			want:       4 + 12,
			wantUID:    1,
			wantStatus: 0,
		},
		{
			name: "2",
			r:    r2,
			args: args{
				rr: rr2,
			},
			want:        4 + 28,
			wantUID:     2,
			wantStatus:  1,
			wantMessage: "test string",
		},
		{
			name: "3",
			r:    r3,
			args: args{
				rr: rr3,
			},
			want:    4 + 12,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.r.ReadFrom(tt.args.rr)
			if (err != nil) != tt.wantErr {
				t.Errorf("ResponseOperation.ReadFrom() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ResponseOperation.ReadFrom() = %v, want %v", got, tt.want)
			}
			if tt.r.UID != tt.wantUID {
				t.Errorf("ResponseOperation.ReadFrom() UID = %v, want %v", tt.r.UID, tt.wantUID)
			}
			if tt.r.Status != tt.wantStatus {
				t.Errorf("ResponseOperation.ReadFrom() Status = %v, want %v", tt.r.Status, tt.wantStatus)
			}
			if tt.r.Message != tt.wantMessage {
				t.Errorf("ResponseOperation.ReadFrom() Message = %v, want %v", tt.r.Message, tt.wantMessage)
			}
		})
	}
}
