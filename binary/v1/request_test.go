package ignite

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"testing"
)

type testRequest struct {
	request
}

func (r *testRequest) WriteTo(io.Writer) (int64, error) {
	return 0, errors.New("stub only")
}

func newTestRequestObject() *request {
	r := &testRequest{request: request{payload: &bytes.Buffer{}}}
	return &r.request
}

func Test_request_WriteByte(t *testing.T) {
	type args struct {
		v byte
	}
	tests := []struct {
		name    string
		r       *request
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "1",
			r:    newTestRequestObject(),
			args: args{
				v: 123,
			},
			want: []byte{123}[:],
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.r.WriteByte(tt.args.v); (err != nil) != tt.wantErr {
				t.Errorf("request.WriteByte() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(tt.r.payload.Bytes(), tt.want) {
				t.Errorf("request.WriteByte() = %#v, want %#v", tt.r.payload.Bytes(), tt.want)
			}
		})
	}
}

func Test_request_WriteOByte(t *testing.T) {
	type args struct {
		v byte
	}
	tests := []struct {
		name    string
		r       *request
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "1",
			r:    newTestRequestObject(),
			args: args{
				v: 123,
			},
			want: []byte{1, 123}[:],
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.r.WriteOByte(tt.args.v); (err != nil) != tt.wantErr {
				t.Errorf("request.WriteOByte() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(tt.r.payload.Bytes(), tt.want) {
				t.Errorf("request.WriteOByte() = %#v, want %#v", tt.r.payload.Bytes(), tt.want)
			}
		})
	}
}

func Test_request_WriteShort(t *testing.T) {
	type args struct {
		v int16
	}
	tests := []struct {
		name    string
		r       *request
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "1",
			r:    newTestRequestObject(),
			args: args{
				v: 12345,
			},
			want: []byte{0x39, 0x30}[:],
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.r.WriteShort(tt.args.v); (err != nil) != tt.wantErr {
				t.Errorf("request.WriteShort() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(tt.r.payload.Bytes(), tt.want) {
				t.Errorf("request.WriteShort() = %#v, want %#v", tt.r.payload.Bytes(), tt.want)
			}
		})
	}
}

func Test_request_WriteOShort(t *testing.T) {
	type args struct {
		v int16
	}
	tests := []struct {
		name    string
		r       *request
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "1",
			r:    newTestRequestObject(),
			args: args{
				v: 12345,
			},
			want: []byte{2, 0x39, 0x30}[:],
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.r.WriteOShort(tt.args.v); (err != nil) != tt.wantErr {
				t.Errorf("request.WriteOShort() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(tt.r.payload.Bytes(), tt.want) {
				t.Errorf("request.WriteOShort() = %#v, want %#v", tt.r.payload.Bytes(), tt.want)
			}
		})
	}
}

func Test_request_WriteInt(t *testing.T) {
	type args struct {
		v int32
	}
	tests := []struct {
		name    string
		r       *request
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "1",
			r:    newTestRequestObject(),
			args: args{
				v: 1234567890,
			},
			want: []byte{0xD2, 0x02, 0x96, 0x49}[:],
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.r.WriteInt(tt.args.v); (err != nil) != tt.wantErr {
				t.Errorf("request.WriteInt() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(tt.r.payload.Bytes(), tt.want) {
				t.Errorf("request.WriteInt() = %#v, want %#v", tt.r.payload.Bytes(), tt.want)
			}
		})
	}
}

func Test_request_WriteOInt(t *testing.T) {
	type args struct {
		v int32
	}
	tests := []struct {
		name    string
		r       *request
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "1",
			r:    newTestRequestObject(),
			args: args{
				v: 1234567890,
			},
			want: []byte{3, 0xD2, 0x02, 0x96, 0x49}[:],
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.r.WriteOInt(tt.args.v); (err != nil) != tt.wantErr {
				t.Errorf("request.WriteOInt() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(tt.r.payload.Bytes(), tt.want) {
				t.Errorf("request.WriteOInt() = %#v, want %#v", tt.r.payload.Bytes(), tt.want)
			}
		})
	}
}

func Test_request_WriteBool(t *testing.T) {
	type args struct {
		v bool
	}
	tests := []struct {
		name    string
		r       *request
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "1",
			r:    newTestRequestObject(),
			args: args{
				v: true,
			},
			want: []byte{1}[:],
		},
		{
			name: "2",
			r:    newTestRequestObject(),
			args: args{
				v: false,
			},
			want: []byte{0}[:],
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.r.WriteBool(tt.args.v); (err != nil) != tt.wantErr {
				t.Errorf("request.WriteBool() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(tt.r.payload.Bytes(), tt.want) {
				t.Errorf("request.WriteByte() = %#v, want %#v", tt.r.payload.Bytes(), tt.want)
			}
		})
	}
}

func Test_request_WriteOBool(t *testing.T) {
	type args struct {
		v bool
	}
	tests := []struct {
		name    string
		r       *request
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "1",
			r:    newTestRequestObject(),
			args: args{
				v: true,
			},
			want: []byte{8, 1}[:],
		},
		{
			name: "2",
			r:    newTestRequestObject(),
			args: args{
				v: false,
			},
			want: []byte{8, 0}[:],
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.r.WriteOBool(tt.args.v); (err != nil) != tt.wantErr {
				t.Errorf("request.WriteOBool() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(tt.r.payload.Bytes(), tt.want) {
				t.Errorf("request.WriteByte() = %#v, want %#v", tt.r.payload.Bytes(), tt.want)
			}
		})
	}
}

func Test_request_WriteOString(t *testing.T) {
	type args struct {
		v string
	}
	tests := []struct {
		name    string
		r       *request
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "1",
			r:    newTestRequestObject(),
			args: args{
				v: "test string",
			},
			want: []byte{9, 0x0B, 0, 0, 0, 0x74, 0x65, 0x73, 0x74, 0x20, 0x73, 0x74, 0x72, 0x69, 0x6e, 0x67}[:],
		},
		{
			name: "2",
			r:    newTestRequestObject(),
			args: args{
				v: "",
			},
			want: []byte{9, 0, 0, 0, 0}[:],
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.r.WriteOString(tt.args.v); (err != nil) != tt.wantErr {
				t.Errorf("request.WriteOString() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(tt.r.payload.Bytes(), tt.want) {
				t.Errorf("request.WriteOString() = %#v, want %#v", tt.r.payload.Bytes(), tt.want)
			}
		})
	}
}

func Test_request_WriteTo(t *testing.T) {
	r := newTestRequestObject()
	_ = r.WriteInt(1234567890)

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
