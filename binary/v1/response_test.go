package ignite

import (
	"bytes"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
)

func Test_response_ReadByte(t *testing.T) {
	r := &response{message: bytes.NewBuffer([]byte{123})}

	tests := []struct {
		name    string
		r       *response
		want    byte
		wantErr bool
	}{
		{
			name: "1",
			r:    r,
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

func Test_response_ReadShort(t *testing.T) {
	r := &response{message: bytes.NewBuffer([]byte{0x39, 0x30})}

	tests := []struct {
		name    string
		r       *response
		want    int16
		wantErr bool
	}{
		{
			name: "1",
			r:    r,
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

func Test_response_ReadInt(t *testing.T) {
	r := &response{message: bytes.NewBuffer([]byte{0xD2, 0x02, 0x96, 0x49})}

	tests := []struct {
		name    string
		r       *response
		want    int32
		wantErr bool
	}{
		{
			name: "1",
			r:    r,
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

func Test_response_ReadLong(t *testing.T) {
	r := &response{message: bytes.NewBuffer([]byte{0x15, 0x81, 0xE9, 0x7D, 0xF4, 0x10, 0x22, 0x11})}

	tests := []struct {
		name    string
		r       *response
		want    int64
		wantErr bool
	}{
		{
			name: "1",
			r:    r,
			want: 1234567890123456789,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.r.ReadLong()
			if (err != nil) != tt.wantErr {
				t.Errorf("response.ReadLong() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("response.ReadLong() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_response_ReadFloat(t *testing.T) {
	r := &response{message: bytes.NewBuffer([]byte{0x65, 0x20, 0xf1, 0x47})}

	tests := []struct {
		name    string
		r       *response
		want    float32
		wantErr bool
	}{
		{
			name: "1",
			r:    r,
			want: 123456.789,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.r.ReadFloat()
			if (err != nil) != tt.wantErr {
				t.Errorf("response.ReadFloat() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("response.ReadFloat() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_response_ReadDouble(t *testing.T) {
	r := &response{message: bytes.NewBuffer([]byte{0xad, 0x69, 0x7e, 0x54, 0x34, 0x6f, 0x9d, 0x41})}

	tests := []struct {
		name    string
		r       *response
		want    float64
		wantErr bool
	}{
		{
			name: "1",
			r:    r,
			want: 123456789.12345,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.r.ReadDouble()
			if (err != nil) != tt.wantErr {
				t.Errorf("response.ReadDouble() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("response.ReadDouble() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_response_ReadChar(t *testing.T) {
	r := &response{message: bytes.NewBuffer([]byte{0x41, 0x0})}

	tests := []struct {
		name    string
		r       *response
		want    Char
		wantErr bool
	}{
		{
			name: "1",
			r:    r,
			want: Char('A'),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.r.ReadChar()
			if (err != nil) != tt.wantErr {
				t.Errorf("response.ReadChar() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("response.ReadChar() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_response_ReadBool(t *testing.T) {
	r1 := &response{message: bytes.NewBuffer([]byte{1})}
	r2 := &response{message: bytes.NewBuffer([]byte{0})}
	r3 := &response{message: bytes.NewBuffer([]byte{2})}

	tests := []struct {
		name    string
		r       *response
		want    bool
		wantErr bool
	}{
		{
			name: "1",
			r:    r1,
			want: true,
		},
		{
			name: "2",
			r:    r2,
			want: false,
		},
		{
			name:    "3",
			r:       r3,
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

func Test_response_ReadOString(t *testing.T) {
	r1 := &response{message: bytes.NewBuffer(
		[]byte{9, 0x0B, 0, 0, 0, 0x74, 0x65, 0x73, 0x74, 0x20, 0x73, 0x74, 0x72, 0x69, 0x6e, 0x67})}
	r2 := &response{message: bytes.NewBuffer(
		[]byte{9, 0, 0, 0, 0})}
	r3 := &response{message: bytes.NewBuffer(
		[]byte{101})}
	r4 := &response{message: bytes.NewBuffer(
		[]byte{0, 0x0B, 0, 0, 0, 0x74, 0x65, 0x73, 0x74, 0x20, 0x73, 0x74, 0x72, 0x69, 0x6e, 0x67})}

	tests := []struct {
		name    string
		r       *response
		want    string
		want1   bool
		wantErr bool
	}{
		{
			name: "1",
			r:    r1,
			want: "test string",
		},
		{
			name: "2",
			r:    r2,
			want: "",
		},
		{
			name:  "3",
			r:     r3,
			want1: true,
		},
		{
			name:    "4",
			r:       r4,
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

func Test_response_ReadUUID(t *testing.T) {
	r1 := &response{message: bytes.NewBuffer(
		[]byte{0xd6, 0x58, 0x9d, 0xa7, 0xf8, 0xb1, 0x46, 0x87, 0xb5,
			0xbd, 0x2d, 0xdc, 0x73, 0x62, 0xa4, 0xa4}[:])}
	v, _ := uuid.Parse("d6589da7-f8b1-4687-b5bd-2ddc7362a4a4")

	tests := []struct {
		name    string
		r       *response
		want    uuid.UUID
		wantErr bool
	}{
		{
			name: "1",
			r:    r1,
			want: v,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.r.ReadUUID()
			if (err != nil) != tt.wantErr {
				t.Errorf("response.ReadUUID() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("response.ReadUUID() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func Test_response_ReadDate(t *testing.T) {
	r1 := &response{message: bytes.NewBuffer([]byte{0x0, 0xa0, 0xcd, 0x88, 0x62, 0x1, 0x0, 0x0})}
	dm := time.Date(2018, 4, 3, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name    string
		r       *response
		want    time.Time
		wantErr bool
	}{
		{
			name: "1",
			r:    r1,
			want: dm,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.r.ReadDate()
			if (err != nil) != tt.wantErr {
				t.Errorf("response.ReadDate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("response.ReadDate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_response_ReadByteArray(t *testing.T) {
	r1 := &response{message: bytes.NewBuffer([]byte{3, 0, 0, 0, 1, 2, 3})}

	tests := []struct {
		name    string
		r       *response
		want    []byte
		wantErr bool
	}{
		{
			name: "1",
			r:    r1,
			want: []byte{1, 2, 3},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.r.ReadByteArray()
			if (err != nil) != tt.wantErr {
				t.Errorf("response.ReadByteArray() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("response.ReadByteArray() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func Test_response_ReadShortArray(t *testing.T) {
	r1 := &response{message: bytes.NewBuffer([]byte{3, 0, 0, 0, 1, 0, 2, 0, 3, 0})}

	tests := []struct {
		name    string
		r       *response
		want    []int16
		wantErr bool
	}{
		{
			name: "1",
			r:    r1,
			want: []int16{1, 2, 3},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.r.ReadShortArray()
			if (err != nil) != tt.wantErr {
				t.Errorf("response.ReadShortArray() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("response.ReadShortArray() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func Test_response_ReadIntArray(t *testing.T) {
	r1 := &response{message: bytes.NewBuffer(
		[]byte{3, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0})}

	tests := []struct {
		name    string
		r       *response
		want    []int32
		wantErr bool
	}{
		{
			name: "1",
			r:    r1,
			want: []int32{1, 2, 3},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.r.ReadIntArray()
			if (err != nil) != tt.wantErr {
				t.Errorf("response.ReadIntArray() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("response.ReadIntArray() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_response_ReadLongArray(t *testing.T) {
	r1 := &response{message: bytes.NewBuffer(
		[]byte{3, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0})}

	tests := []struct {
		name    string
		r       *response
		want    []int64
		wantErr bool
	}{
		{
			name: "1",
			r:    r1,
			want: []int64{1, 2, 3},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.r.ReadLongArray()
			if (err != nil) != tt.wantErr {
				t.Errorf("response.ReadLongArray() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("response.ReadLongArray() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_response_ReadTimestamp(t *testing.T) {
	r1 := &response{message: bytes.NewBuffer(
		[]byte{0xdb, 0xb, 0xe6, 0x8b, 0x62, 0x1, 0x0, 0x0, 0x55, 0xf8, 0x6, 0x0})}
	tm := time.Date(2018, 4, 3, 14, 25, 32, int(time.Millisecond*123+time.Microsecond*456+789), time.UTC)

	tests := []struct {
		name    string
		r       *response
		want    time.Time
		wantErr bool
	}{
		{
			name: "1",
			r:    r1,
			want: tm,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.r.ReadTimestamp()
			if (err != nil) != tt.wantErr {
				t.Errorf("response.ReadTimestamp() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("response.ReadTimestamp() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func Test_response_ReadTime(t *testing.T) {
	r1 := &response{message: bytes.NewBuffer([]byte{0xdb, 0x6b, 0x18, 0x3, 0x0, 0x0, 0x0, 0x0})}
	tm := time.Date(1, 1, 1, 14, 25, 32, int(time.Millisecond*123), time.UTC)

	tests := []struct {
		name    string
		r       *response
		want    time.Time
		wantErr bool
	}{
		{
			name: "1",
			r:    r1,
			want: tm,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.r.ReadTime()
			if (err != nil) != tt.wantErr {
				t.Errorf("response.ReadTime() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("response.ReadTime() = %s, want %s", got.String(), tt.want.String())
				// t.Errorf("response.ReadTime() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func Test_response_ReadObject(t *testing.T) {
	r1 := &response{message: bytes.NewBuffer([]byte{1, 123})}
	r2 := &response{message: bytes.NewBuffer([]byte{2, 0x39, 0x30})}
	r3 := &response{message: bytes.NewBuffer([]byte{3, 0xD2, 0x02, 0x96, 0x49})}
	r4 := &response{message: bytes.NewBuffer([]byte{4, 0x15, 0x81, 0xE9, 0x7D, 0xF4, 0x10, 0x22, 0x11})}
	r5 := &response{message: bytes.NewBuffer([]byte{5, 0x65, 0x20, 0xf1, 0x47})}
	r6 := &response{message: bytes.NewBuffer([]byte{6, 0xad, 0x69, 0x7e, 0x54, 0x34, 0x6f, 0x9d, 0x41})}
	r7 := &response{message: bytes.NewBuffer([]byte{7, 0x41, 0x0})}
	r8 := &response{message: bytes.NewBuffer([]byte{8, 1})}
	r9 := &response{message: bytes.NewBuffer(
		[]byte{9, 0x0B, 0, 0, 0, 0x74, 0x65, 0x73, 0x74, 0x20, 0x73, 0x74, 0x72, 0x69, 0x6e, 0x67})}
	r10 := &response{message: bytes.NewBuffer([]byte{10, 0xd6, 0x58, 0x9d, 0xa7, 0xf8, 0xb1, 0x46, 0x87, 0xb5,
		0xbd, 0x2d, 0xdc, 0x73, 0x62, 0xa4, 0xa4})}
	uid, _ := uuid.Parse("d6589da7-f8b1-4687-b5bd-2ddc7362a4a4")
	r11 := &response{message: bytes.NewBuffer([]byte{11, 0x0, 0xa0, 0xcd, 0x88, 0x62, 0x1, 0x0, 0x0})}
	dm := time.Date(2018, 4, 3, 0, 0, 0, 0, time.UTC)
	r12 := &response{message: bytes.NewBuffer([]byte{12, 3, 0, 0, 0, 1, 2, 3})}
	r13 := &response{message: bytes.NewBuffer([]byte{13, 3, 0, 0, 0, 1, 0, 2, 0, 3, 0})}
	r14 := &response{message: bytes.NewBuffer(
		[]byte{14, 3, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0})}
	r15 := &response{message: bytes.NewBuffer(
		[]byte{15, 3, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0})}
	r33 := &response{message: bytes.NewBuffer([]byte{33, 0xdb, 0xb, 0xe6, 0x8b, 0x62, 0x1, 0x0, 0x0,
		0x55, 0xf8, 0x6, 0x0})}
	tm := time.Date(2018, 4, 3, 14, 25, 32, int(time.Millisecond*123+time.Microsecond*456+789), time.UTC)
	r36 := &response{message: bytes.NewBuffer([]byte{36, 0xdb, 0x6b, 0x18, 0x3, 0x0, 0x0, 0x0, 0x0})}
	tm2 := time.Date(1, 1, 1, 14, 25, 32, int(time.Millisecond*123), time.UTC)
	r101 := &response{message: bytes.NewBuffer([]byte{101})}

	tests := []struct {
		name    string
		r       *response
		want    interface{}
		wantErr bool
	}{
		{
			name: "byte",
			r:    r1,
			want: byte(123),
		},
		{
			name: "short",
			r:    r2,
			want: int16(12345),
		},
		{
			name: "int",
			r:    r3,
			want: int32(1234567890),
		},
		{
			name: "long",
			r:    r4,
			want: int64(1234567890123456789),
		},
		{
			name: "float",
			r:    r5,
			want: float32(123456.789),
		},
		{
			name: "double",
			r:    r6,
			want: float64(123456789.12345),
		},
		{
			name: "char",
			r:    r7,
			want: Char('A'),
		},
		{
			name: "bool",
			r:    r8,
			want: true,
		},
		{
			name: "string",
			r:    r9,
			want: "test string",
		},
		{
			name: "UUID",
			r:    r10,
			want: uid,
		},
		{
			name: "Date",
			r:    r11,
			want: dm,
		},
		{
			name: "byte array",
			r:    r12,
			want: []byte{1, 2, 3},
		},
		{
			name: "short array",
			r:    r13,
			want: []int16{1, 2, 3},
		},
		{
			name: "int array",
			r:    r14,
			want: []int32{1, 2, 3},
		},
		{
			name: "long array",
			r:    r15,
			want: []int64{1, 2, 3},
		},
		{
			name: "Timestamp",
			r:    r33,
			want: tm,
		},
		{
			name: "Time",
			r:    r36,
			want: tm2,
		},
		{
			name: "NULL",
			r:    r101,
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.r.ReadObject()
			if (err != nil) != tt.wantErr {
				t.Errorf("response.ReadObject() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("response.ReadObject() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func Test_response_ReadFrom(t *testing.T) {
	rr := bytes.NewBuffer([]byte{1, 0, 0, 0, 1})

	type args struct {
		rr io.Reader
	}
	tests := []struct {
		name    string
		r       *response
		args    args
		want    int64
		wantErr bool
	}{
		{
			name: "1",
			r:    &response{},
			args: args{
				rr: rr,
			},
			want: 4 + 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.r.ReadFrom(tt.args.rr)
			if (err != nil) != tt.wantErr {
				t.Errorf("response.ReadFrom() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("response.ReadFrom() = %v, want %v", got, tt.want)
			}
		})
	}
}
