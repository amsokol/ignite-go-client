package ignite

import (
	"bytes"
	"io"
	"testing"
)

type testResponse struct {
	response
}

// test stub
func (r *testResponse) ReadFrom(io.Reader) (int64, error) {
	return 0, nil
}

func Test_response_ReadByte(t *testing.T) {
	r := &testResponse{response: response{message: bytes.NewBuffer(
		[]byte{123})}}

	tests := []struct {
		name    string
		r       *response
		want    byte
		wantErr bool
	}{
		{
			name: "1",
			r:    &r.response,
			want: 123,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.r.ReadByte()
			if (err != nil) != tt.wantErr {
				t.Errorf("response.ReadByte() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("response.ReadByte() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_response_ReadOByte(t *testing.T) {
	r1 := &testResponse{response: response{message: bytes.NewBuffer(
		[]byte{1, 123})}}
	r2 := &testResponse{response: response{message: bytes.NewBuffer(
		[]byte{0, 123})}}

	tests := []struct {
		name    string
		r       *response
		want    byte
		wantErr bool
	}{
		{
			name: "1",
			r:    &r1.response,
			want: 123,
		},
		{
			name:    "2",
			r:       &r2.response,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.r.ReadOByte()
			if (err != nil) != tt.wantErr {
				t.Errorf("response.ReadOByte() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("response.ReadOByte() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_response_ReadShort(t *testing.T) {
	r := &testResponse{response: response{message: bytes.NewBuffer(
		[]byte{0x39, 0x30})}}

	tests := []struct {
		name    string
		r       *response
		want    int16
		wantErr bool
	}{
		{
			name: "1",
			r:    &r.response,
			want: 12345,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.r.ReadShort()
			if (err != nil) != tt.wantErr {
				t.Errorf("response.ReadShort() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("response.ReadShort() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_response_ReadOShort(t *testing.T) {
	r1 := &testResponse{response: response{message: bytes.NewBuffer(
		[]byte{2, 0x39, 0x30})}}
	r2 := &testResponse{response: response{message: bytes.NewBuffer(
		[]byte{0, 0x39, 0x30})}}

	tests := []struct {
		name    string
		r       *response
		want    int16
		wantErr bool
	}{
		{
			name: "1",
			r:    &r1.response,
			want: 12345,
		},
		{
			name:    "2",
			r:       &r2.response,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.r.ReadOShort()
			if (err != nil) != tt.wantErr {
				t.Errorf("response.ReadOShort() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("response.ReadOShort() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_response_ReadInt(t *testing.T) {
	r := &testResponse{response: response{message: bytes.NewBuffer(
		[]byte{0xD2, 0x02, 0x96, 0x49})}}

	tests := []struct {
		name    string
		r       *response
		want    int32
		wantErr bool
	}{
		{
			name: "1",
			r:    &r.response,
			want: 1234567890,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.r.ReadInt()
			if (err != nil) != tt.wantErr {
				t.Errorf("response.ReadInt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("response.ReadInt() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_response_ReadOInt(t *testing.T) {
	r1 := &testResponse{response: response{message: bytes.NewBuffer(
		[]byte{3, 0xD2, 0x02, 0x96, 0x49})}}
	r2 := &testResponse{response: response{message: bytes.NewBuffer(
		[]byte{0, 0xD2, 0x02, 0x96, 0x49})}}

	tests := []struct {
		name    string
		r       *response
		want    int32
		wantErr bool
	}{
		{
			name: "1",
			r:    &r1.response,
			want: 1234567890,
		},
		{
			name:    "2",
			r:       &r2.response,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.r.ReadOInt()
			if (err != nil) != tt.wantErr {
				t.Errorf("response.ReadOInt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("response.ReadOInt() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_response_ReadBool(t *testing.T) {
	r1 := &testResponse{response: response{message: bytes.NewBuffer(
		[]byte{1})}}
	r2 := &testResponse{response: response{message: bytes.NewBuffer(
		[]byte{0})}}
	r3 := &testResponse{response: response{message: bytes.NewBuffer(
		[]byte{2})}}

	tests := []struct {
		name    string
		r       *response
		want    bool
		wantErr bool
	}{
		{
			name: "1",
			r:    &r1.response,
			want: true,
		},
		{
			name: "2",
			r:    &r2.response,
			want: false,
		},
		{
			name:    "3",
			r:       &r3.response,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.r.ReadBool()
			if (err != nil) != tt.wantErr {
				t.Errorf("response.ReadBool() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("response.ReadBool() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_response_ReadOBool(t *testing.T) {
	r1 := &testResponse{response: response{message: bytes.NewBuffer(
		[]byte{8, 1})}}
	r2 := &testResponse{response: response{message: bytes.NewBuffer(
		[]byte{8, 0})}}
	r3 := &testResponse{response: response{message: bytes.NewBuffer(
		[]byte{8, 2})}}
	r4 := &testResponse{response: response{message: bytes.NewBuffer(
		[]byte{9, 1})}}

	tests := []struct {
		name    string
		r       *response
		want    bool
		wantErr bool
	}{
		{
			name: "1",
			r:    &r1.response,
			want: true,
		},
		{
			name: "2",
			r:    &r2.response,
			want: false,
		},
		{
			name:    "3",
			r:       &r3.response,
			wantErr: true,
		},
		{
			name:    "4",
			r:       &r4.response,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.r.ReadOBool()
			if (err != nil) != tt.wantErr {
				t.Errorf("response.ReadOBool() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("response.ReadOBool() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_response_ReadOString(t *testing.T) {
	r1 := &testResponse{response: response{message: bytes.NewBuffer(
		[]byte{9, 0x0B, 0, 0, 0, 0x74, 0x65, 0x73, 0x74, 0x20, 0x73, 0x74, 0x72, 0x69, 0x6e, 0x67})}}
	r2 := &testResponse{response: response{message: bytes.NewBuffer(
		[]byte{9, 0, 0, 0, 0})}}
	r3 := &testResponse{response: response{message: bytes.NewBuffer(
		[]byte{101})}}
	r4 := &testResponse{response: response{message: bytes.NewBuffer(
		[]byte{0, 0x0B, 0, 0, 0, 0x74, 0x65, 0x73, 0x74, 0x20, 0x73, 0x74, 0x72, 0x69, 0x6e, 0x67})}}

	tests := []struct {
		name    string
		r       *response
		want    string
		want1   bool
		wantErr bool
	}{
		{
			name: "1",
			r:    &r1.response,
			want: "test string",
		},
		{
			name: "2",
			r:    &r2.response,
			want: "",
		},
		{
			name:  "3",
			r:     &r3.response,
			want1: true,
		},
		{
			name:    "4",
			r:       &r4.response,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := tt.r.ReadOString()
			if (err != nil) != tt.wantErr {
				t.Errorf("response.ReadOString() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("response.ReadOString() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("response.ReadOString() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
