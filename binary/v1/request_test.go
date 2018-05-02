package ignite

import (
	"bytes"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
)

func Test_request_WriteByte(t *testing.T) {
	r := &request{payload: &bytes.Buffer{}}

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
			r:    r,
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
	r := &request{payload: &bytes.Buffer{}}

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
			r:    r,
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
	r := &request{payload: &bytes.Buffer{}}

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
			r:    r,
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
	r := &request{payload: &bytes.Buffer{}}

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
			r:    r,
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
	r := &request{payload: &bytes.Buffer{}}

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
			r:    r,
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
	r := &request{payload: &bytes.Buffer{}}

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
			r:    r,
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

func Test_request_WriteLong(t *testing.T) {
	r := &request{payload: &bytes.Buffer{}}

	type args struct {
		v int64
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
			r:    r,
			args: args{
				v: 1234567890123456789,
			},
			want: []byte{0x15, 0x81, 0xE9, 0x7D, 0xF4, 0x10, 0x22, 0x11}[:],
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.r.WriteLong(tt.args.v); (err != nil) != tt.wantErr {
				t.Errorf("request.WriteLong() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(tt.r.payload.Bytes(), tt.want) {
				t.Errorf("request.WriteLong() = %#v, want %#v", tt.r.payload.Bytes(), tt.want)
			}
		})
	}
}

func Test_request_WriteOLong(t *testing.T) {
	r := &request{payload: &bytes.Buffer{}}

	type args struct {
		v int64
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
			r:    r,
			args: args{
				v: 1234567890123456789,
			},
			want: []byte{4, 0x15, 0x81, 0xE9, 0x7D, 0xF4, 0x10, 0x22, 0x11}[:],
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.r.WriteOLong(tt.args.v); (err != nil) != tt.wantErr {
				t.Errorf("request.WriteOLong() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(tt.r.payload.Bytes(), tt.want) {
				t.Errorf("request.WriteOLong() = %#v, want %#v", tt.r.payload.Bytes(), tt.want)
			}
		})
	}
}

func Test_request_WriteFloat(t *testing.T) {
	r := &request{payload: &bytes.Buffer{}}

	type args struct {
		v float32
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
			r:    r,
			args: args{
				v: 123456.789,
			},
			want: []byte{0x65, 0x20, 0xf1, 0x47}[:],
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.r.WriteFloat(tt.args.v); (err != nil) != tt.wantErr {
				t.Errorf("request.WriteFloat() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(tt.r.payload.Bytes(), tt.want) {
				t.Errorf("request.WriteFloat() = %#v, want %#v", tt.r.payload.Bytes(), tt.want)
			}
		})
	}
}

func Test_request_WriteOFloat(t *testing.T) {
	r := &request{payload: &bytes.Buffer{}}

	type args struct {
		v float32
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
			r:    r,
			args: args{
				v: 123456.789,
			},
			want: []byte{5, 0x65, 0x20, 0xf1, 0x47}[:],
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.r.WriteOFloat(tt.args.v); (err != nil) != tt.wantErr {
				t.Errorf("request.WriteOFloat() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(tt.r.payload.Bytes(), tt.want) {
				t.Errorf("request.WriteOFloat() = %#v, want %#v", tt.r.payload.Bytes(), tt.want)
			}
		})
	}
}

func Test_request_WriteDouble(t *testing.T) {
	r := &request{payload: &bytes.Buffer{}}

	type args struct {
		v float64
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
			r:    r,
			args: args{
				v: 123456789.12345,
			},
			want: []byte{0xad, 0x69, 0x7e, 0x54, 0x34, 0x6f, 0x9d, 0x41}[:],
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.r.WriteDouble(tt.args.v); (err != nil) != tt.wantErr {
				t.Errorf("request.WriteDouble() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(tt.r.payload.Bytes(), tt.want) {
				t.Errorf("request.WriteDouble() = %#v, want %#v", tt.r.payload.Bytes(), tt.want)
			}
		})
	}
}

func Test_request_WriteODouble(t *testing.T) {
	r := &request{payload: &bytes.Buffer{}}

	type args struct {
		v float64
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
			r:    r,
			args: args{
				v: 123456789.12345,
			},
			want: []byte{6, 0xad, 0x69, 0x7e, 0x54, 0x34, 0x6f, 0x9d, 0x41}[:],
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.r.WriteODouble(tt.args.v); (err != nil) != tt.wantErr {
				t.Errorf("request.WriteODouble() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(tt.r.payload.Bytes(), tt.want) {
				t.Errorf("request.WriteODouble() = %#v, want %#v", tt.r.payload.Bytes(), tt.want)
			}
		})
	}
}

func Test_request_WriteChar(t *testing.T) {
	r := &request{payload: &bytes.Buffer{}}

	type args struct {
		v rune
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
			r:    r,
			args: args{
				v: 'A',
			},
			want: []byte{0x41, 0x0}[:],
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.r.WriteChar(tt.args.v); (err != nil) != tt.wantErr {
				t.Errorf("request.WriteChar() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(tt.r.payload.Bytes(), tt.want) {
				t.Errorf("request.WriteODouble() = %#v, want %#v", tt.r.payload.Bytes(), tt.want)
			}
		})
	}
}

func Test_request_WriteOChar(t *testing.T) {
	r := &request{payload: &bytes.Buffer{}}

	type args struct {
		v Char
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
			r:    r,
			args: args{
				v: 'A',
			},
			want: []byte{7, 0x41, 0x0}[:],
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.r.WriteOChar(tt.args.v); (err != nil) != tt.wantErr {
				t.Errorf("request.WriteOChar() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(tt.r.payload.Bytes(), tt.want) {
				t.Errorf("request.WriteODouble() = %#v, want %#v", tt.r.payload.Bytes(), tt.want)
			}
		})
	}
}

func Test_request_WriteBool(t *testing.T) {
	r1 := &request{payload: &bytes.Buffer{}}
	r2 := &request{payload: &bytes.Buffer{}}

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
			r:    r1,
			args: args{
				v: true,
			},
			want: []byte{1}[:],
		},
		{
			name: "2",
			r:    r2,
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
	r1 := &request{payload: &bytes.Buffer{}}
	r2 := &request{payload: &bytes.Buffer{}}

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
			r:    r1,
			args: args{
				v: true,
			},
			want: []byte{8, 1}[:],
		},
		{
			name: "2",
			r:    r2,
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
	r1 := &request{payload: &bytes.Buffer{}}
	r2 := &request{payload: &bytes.Buffer{}}

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
			r:    r1,
			args: args{
				v: "test string",
			},
			want: []byte{9, 0x0B, 0, 0, 0, 0x74, 0x65, 0x73, 0x74, 0x20, 0x73, 0x74, 0x72, 0x69, 0x6e, 0x67}[:],
		},
		{
			name: "2",
			r:    r2,
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

func Test_request_WriteOUUID(t *testing.T) {
	r1 := &request{payload: &bytes.Buffer{}}
	v, _ := uuid.Parse("d6589da7-f8b1-4687-b5bd-2ddc7362a4a4")

	type args struct {
		v uuid.UUID
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
			r:    r1,
			args: args{
				v: v,
			},
			want: []byte{10, 0xd6, 0x58, 0x9d, 0xa7, 0xf8, 0xb1, 0x46, 0x87, 0xb5,
				0xbd, 0x2d, 0xdc, 0x73, 0x62, 0xa4, 0xa4}[:],
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.r.WriteOUUID(tt.args.v); (err != nil) != tt.wantErr {
				t.Errorf("request.WriteOUUID() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(tt.r.payload.Bytes(), tt.want) {
				t.Errorf("request.WriteOUUID() = %#v, want %#v", tt.r.payload.Bytes(), tt.want)
			}
		})
	}
}

func Test_request_WriteODate(t *testing.T) {
	r1 := &request{payload: &bytes.Buffer{}}
	dm := time.Date(2018, 4, 3, 0, 0, 0, 0, time.UTC)

	type args struct {
		v Date
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
			r:    r1,
			args: args{
				v: DateT(dm),
			},
			want: []byte{11, 0x0, 0xa0, 0xcd, 0x88, 0x62, 0x1, 0x0, 0x0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.r.WriteODate(tt.args.v); (err != nil) != tt.wantErr {
				t.Errorf("request.WriteODate() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(tt.r.payload.Bytes(), tt.want) {
				t.Errorf("request.WriteODate() = %#v, want %#v", tt.r.payload.Bytes(), tt.want)
			}
		})
	}
}

func Test_request_WriteByteArray(t *testing.T) {
	r1 := &request{payload: &bytes.Buffer{}}

	type args struct {
		v []byte
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
			r:    r1,
			args: args{
				v: []byte{1, 2, 3},
			},
			want: []byte{3, 0, 0, 0, 1, 2, 3},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.r.WriteByteArray(tt.args.v); (err != nil) != tt.wantErr {
				t.Errorf("request.WriteByteArray() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(tt.r.payload.Bytes(), tt.want) {
				t.Errorf("request.WriteByteArray() = %#v, want %#v", tt.r.payload.Bytes(), tt.want)
			}
		})
	}
}

func Test_request_WriteOByteArray(t *testing.T) {
	r1 := &request{payload: &bytes.Buffer{}}

	type args struct {
		v []byte
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
			r:    r1,
			args: args{
				v: []byte{1, 2, 3},
			},
			want: []byte{12, 3, 0, 0, 0, 1, 2, 3},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.r.WriteOByteArray(tt.args.v); (err != nil) != tt.wantErr {
				t.Errorf("request.WriteOByteArray() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(tt.r.payload.Bytes(), tt.want) {
				t.Errorf("request.WriteOByteArray() = %#v, want %#v", tt.r.payload.Bytes(), tt.want)
			}
		})
	}
}

func Test_request_WriteShortArray(t *testing.T) {
	r1 := &request{payload: &bytes.Buffer{}}

	type args struct {
		v []int16
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
			r:    r1,
			args: args{
				v: []int16{1, 2, 3},
			},
			want: []byte{3, 0, 0, 0, 1, 0, 2, 0, 3, 0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.r.WriteShortArray(tt.args.v); (err != nil) != tt.wantErr {
				t.Errorf("request.WriteShortArray() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(tt.r.payload.Bytes(), tt.want) {
				t.Errorf("request.WriteShortArray() = %#v, want %#v", tt.r.payload.Bytes(), tt.want)
			}
		})
	}
}

func Test_request_WriteOShortArray(t *testing.T) {
	r1 := &request{payload: &bytes.Buffer{}}

	type args struct {
		v []int16
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
			r:    r1,
			args: args{
				v: []int16{1, 2, 3},
			},
			want: []byte{13, 3, 0, 0, 0, 1, 0, 2, 0, 3, 0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.r.WriteOShortArray(tt.args.v); (err != nil) != tt.wantErr {
				t.Errorf("request.WriteOShortArray() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(tt.r.payload.Bytes(), tt.want) {
				t.Errorf("request.WriteOShortArray() = %#v, want %#v", tt.r.payload.Bytes(), tt.want)
			}
		})
	}
}

func Test_request_WriteIntArray(t *testing.T) {
	r1 := &request{payload: &bytes.Buffer{}}

	type args struct {
		v []int32
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
			r:    r1,
			args: args{
				v: []int32{1, 2, 3},
			},
			want: []byte{3, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.r.WriteIntArray(tt.args.v); (err != nil) != tt.wantErr {
				t.Errorf("request.WriteIntArray() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(tt.r.payload.Bytes(), tt.want) {
				t.Errorf("request.WriteIntArray() = %#v, want %#v", tt.r.payload.Bytes(), tt.want)
			}
		})
	}
}

func Test_request_WriteOIntArray(t *testing.T) {
	r1 := &request{payload: &bytes.Buffer{}}

	type args struct {
		v []int32
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
			r:    r1,
			args: args{
				v: []int32{1, 2, 3},
			},
			want: []byte{14, 3, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.r.WriteOIntArray(tt.args.v); (err != nil) != tt.wantErr {
				t.Errorf("request.WriteOIntArray() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(tt.r.payload.Bytes(), tt.want) {
				t.Errorf("request.WriteOIntArray() = %#v, want %#v", tt.r.payload.Bytes(), tt.want)
			}
		})
	}
}

func Test_request_WriteLongArray(t *testing.T) {
	r1 := &request{payload: &bytes.Buffer{}}

	type args struct {
		v []int64
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
			r:    r1,
			args: args{
				v: []int64{1, 2, 3},
			},
			want: []byte{3, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.r.WriteLongArray(tt.args.v); (err != nil) != tt.wantErr {
				t.Errorf("request.WriteLongArray() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(tt.r.payload.Bytes(), tt.want) {
				t.Errorf("request.WriteLongArray() = %#v, want %#v", tt.r.payload.Bytes(), tt.want)
			}
		})
	}
}

func Test_request_WriteOLongArray(t *testing.T) {
	r1 := &request{payload: &bytes.Buffer{}}

	type args struct {
		v []int64
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
			r:    r1,
			args: args{
				v: []int64{1, 2, 3},
			},
			want: []byte{15, 3, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.r.WriteOLongArray(tt.args.v); (err != nil) != tt.wantErr {
				t.Errorf("request.WriteOLongArray() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(tt.r.payload.Bytes(), tt.want) {
				t.Errorf("request.WriteOLongArray() = %#v, want %#v", tt.r.payload.Bytes(), tt.want)
			}
		})
	}
}

func Test_request_WriteFloatArray(t *testing.T) {
	r1 := &request{payload: &bytes.Buffer{}}

	type args struct {
		v []float32
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
			r:    r1,
			args: args{
				v: []float32{1.1, 2.2, 3.3},
			},
			want: []byte{0x3, 0x0, 0x0, 0x0, 0xcd, 0xcc, 0x8c, 0x3f, 0xcd, 0xcc, 0xc, 0x40, 0x33, 0x33, 0x53, 0x40},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.r.WriteFloatArray(tt.args.v); (err != nil) != tt.wantErr {
				t.Errorf("request.WriteFloatArray() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(tt.r.payload.Bytes(), tt.want) {
				t.Errorf("request.WriteFloatArray() = %#v, want %#v", tt.r.payload.Bytes(), tt.want)
			}
		})
	}
}

func Test_request_WriteOFloatArray(t *testing.T) {
	r1 := &request{payload: &bytes.Buffer{}}

	type args struct {
		v []float32
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
			r:    r1,
			args: args{
				v: []float32{1.1, 2.2, 3.3},
			},
			want: []byte{16, 0x3, 0x0, 0x0, 0x0, 0xcd, 0xcc, 0x8c, 0x3f, 0xcd, 0xcc, 0xc, 0x40, 0x33, 0x33, 0x53, 0x40},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.r.WriteOFloatArray(tt.args.v); (err != nil) != tt.wantErr {
				t.Errorf("request.WriteOFloatArray() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(tt.r.payload.Bytes(), tt.want) {
				t.Errorf("request.WriteOFloatArray() = %#v, want %#v", tt.r.payload.Bytes(), tt.want)
			}
		})
	}
}

func Test_request_WriteOTimestamp(t *testing.T) {
	r1 := &request{payload: &bytes.Buffer{}}
	tm := time.Date(2018, 4, 3, 14, 25, 32, int(time.Millisecond*123+time.Microsecond*456+789), time.UTC)

	type args struct {
		v time.Time
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
			r:    r1,
			args: args{
				v: tm,
			},
			want: []byte{33, 0xdb, 0xb, 0xe6, 0x8b, 0x62, 0x1, 0x0, 0x0, 0x55, 0xf8, 0x6, 0x0}[:],
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.r.WriteOTimestamp(tt.args.v); (err != nil) != tt.wantErr {
				t.Errorf("request.WriteOTimestamp() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(tt.r.payload.Bytes(), tt.want) {
				t.Errorf("request.WriteOString() = %#v, want %#v", tt.r.payload.Bytes(), tt.want)
			}
		})
	}
}

func Test_request_WriteOTime(t *testing.T) {
	r1 := &request{payload: &bytes.Buffer{}}
	tm := time.Date(1, 1, 1, 14, 25, 32, int(time.Millisecond*123+time.Microsecond*456+789), time.UTC)

	type args struct {
		v Time
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
			r:    r1,
			args: args{
				v: TimeT(tm),
			},
			want: []byte{36, 0xdb, 0x6b, 0x18, 0x3, 0x0, 0x0, 0x0, 0x0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.r.WriteOTime(tt.args.v); (err != nil) != tt.wantErr {
				t.Errorf("request.WriteOTime() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(tt.r.payload.Bytes(), tt.want) {
				t.Errorf("request.WriteOTime() = %#v, want %#v", tt.r.payload.Bytes(), tt.want)
			}
		})
	}
}

func Test_request_WriteNull(t *testing.T) {
	r := &request{payload: &bytes.Buffer{}}

	tests := []struct {
		name    string
		r       *request
		want    []byte
		wantErr bool
	}{
		{
			name: "1",
			r:    r,
			want: []byte{101}[:],
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.r.WriteNull(); (err != nil) != tt.wantErr {
				t.Errorf("request.WriteNull() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(tt.r.payload.Bytes(), tt.want) {
				t.Errorf("request.WriteNull() = %#v, want %#v", tt.r.payload.Bytes(), tt.want)
			}
		})
	}
}

func Test_request_WriteObject(t *testing.T) {
	r1 := &request{payload: &bytes.Buffer{}}
	r2 := &request{payload: &bytes.Buffer{}}
	r3 := &request{payload: &bytes.Buffer{}}
	r4 := &request{payload: &bytes.Buffer{}}
	r5 := &request{payload: &bytes.Buffer{}}
	r6 := &request{payload: &bytes.Buffer{}}
	r7 := &request{payload: &bytes.Buffer{}}
	r8 := &request{payload: &bytes.Buffer{}}
	r9 := &request{payload: &bytes.Buffer{}}
	r10 := &request{payload: &bytes.Buffer{}}
	r11 := &request{payload: &bytes.Buffer{}}
	r12 := &request{payload: &bytes.Buffer{}}
	r13 := &request{payload: &bytes.Buffer{}}
	r14 := &request{payload: &bytes.Buffer{}}
	r15 := &request{payload: &bytes.Buffer{}}
	r16 := &request{payload: &bytes.Buffer{}}
	r33 := &request{payload: &bytes.Buffer{}}
	r36 := &request{payload: &bytes.Buffer{}}
	r101 := &request{payload: &bytes.Buffer{}}
	tm := time.Date(2018, 4, 3, 14, 25, 32, int(time.Millisecond*123+time.Microsecond*456+789), time.UTC)
	dm := time.Date(2018, 4, 3, 0, 0, 0, 0, time.UTC)
	uid, _ := uuid.Parse("d6589da7-f8b1-4687-b5bd-2ddc7362a4a4")

	type args struct {
		o interface{}
	}
	tests := []struct {
		name    string
		r       *request
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "byte",
			r:    r1,
			args: args{
				byte(123),
			},
			want: []byte{1, 123}[:],
		},
		{
			name: "short",
			r:    r2,
			args: args{
				int16(12345),
			},
			want: []byte{2, 0x39, 0x30}[:],
		},
		{
			name: "int",
			r:    r3,
			args: args{
				int32(1234567890),
			},
			want: []byte{3, 0xD2, 0x02, 0x96, 0x49}[:],
		},
		{
			name: "long",
			r:    r4,
			args: args{
				int64(1234567890123456789),
			},
			want: []byte{4, 0x15, 0x81, 0xE9, 0x7D, 0xF4, 0x10, 0x22, 0x11}[:],
		},
		{
			name: "float",
			r:    r5,
			args: args{
				float32(123456.789),
			},
			want: []byte{5, 0x65, 0x20, 0xf1, 0x47}[:],
		},
		{
			name: "double",
			r:    r6,
			args: args{
				float64(123456789.12345),
			},
			want: []byte{6, 0xad, 0x69, 0x7e, 0x54, 0x34, 0x6f, 0x9d, 0x41}[:],
		},
		{
			name: "char",
			r:    r7,
			args: args{
				Char('A'),
			},
			want: []byte{7, 0x41, 0x0}[:],
		},
		{
			name: "bool",
			r:    r8,
			args: args{
				true,
			},
			want: []byte{8, 0x1}[:],
		},
		{
			name: "String",
			r:    r9,
			args: args{
				"test string",
			},
			want: []byte{9, 0x0B, 0, 0, 0, 0x74, 0x65, 0x73, 0x74, 0x20, 0x73, 0x74, 0x72, 0x69, 0x6e, 0x67}[:],
		},
		{
			name: "UUID",
			r:    r10,
			args: args{
				uid,
			},
			want: []byte{10, 0xd6, 0x58, 0x9d, 0xa7, 0xf8, 0xb1, 0x46, 0x87, 0xb5,
				0xbd, 0x2d, 0xdc, 0x73, 0x62, 0xa4, 0xa4}[:],
		},
		{
			name: "Date",
			r:    r11,
			args: args{
				DateT(dm),
			},
			want: []byte{11, 0x0, 0xa0, 0xcd, 0x88, 0x62, 0x1, 0x0, 0x0},
		},
		{
			name: "byte array",
			r:    r12,
			args: args{
				[]byte{1, 2, 3},
			},
			want: []byte{12, 3, 0, 0, 0, 1, 2, 3},
		},
		{
			name: "short array",
			r:    r13,
			args: args{
				[]int16{1, 2, 3},
			},
			want: []byte{13, 3, 0, 0, 0, 1, 0, 2, 0, 3, 0},
		},
		{
			name: "int array",
			r:    r14,
			args: args{
				[]int32{1, 2, 3},
			},
			want: []byte{14, 3, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0},
		},
		{
			name: "long array",
			r:    r15,
			args: args{
				[]int64{1, 2, 3},
			},
			want: []byte{15, 3, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			name: "float32 array",
			r:    r16,
			args: args{
				[]float32{1.1, 2.2, 3.3},
			},
			want: []byte{16, 0x3, 0x0, 0x0, 0x0, 0xcd, 0xcc, 0x8c, 0x3f, 0xcd, 0xcc, 0xc, 0x40, 0x33, 0x33, 0x53, 0x40},
		},
		{
			name: "Timestamp",
			r:    r33,
			args: args{
				tm,
			},
			want: []byte{33, 0xdb, 0xb, 0xe6, 0x8b, 0x62, 0x1, 0x0, 0x0, 0x55, 0xf8, 0x6, 0x0}[:],
		},
		{
			name: "Time",
			r:    r36,
			args: args{
				TimeT(tm),
			},
			want: []byte{36, 0xdb, 0x6b, 0x18, 0x3, 0x0, 0x0, 0x0, 0x0}[:],
		},
		{
			name: "NULL",
			r:    r101,
			args: args{
				nil,
			},
			want: []byte{101}[:],
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.r.WriteObject(tt.args.o); (err != nil) != tt.wantErr {
				t.Errorf("request.WriteObject() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(tt.r.payload.Bytes(), tt.want) {
				t.Errorf("request.WriteObject() = %#v, want %#v", tt.r.payload.Bytes(), tt.want)
			}
		})
	}
}

func Test_request_WriteTo(t *testing.T) {
	r := &request{payload: &bytes.Buffer{}}
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
