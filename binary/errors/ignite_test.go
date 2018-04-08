package errors

import (
	"fmt"
	"testing"
)

func TestNewError(t *testing.T) {
	type args struct {
		status  int32
		message string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "success test",
			args: args{
				status:  1,
				message: "test error message",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := NewError(tt.args.status, tt.args.message); (err != nil) != tt.wantErr {
				t.Errorf("NewError() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestErrorf(t *testing.T) {
	type args struct {
		format string
		a      []interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "success test",
			args: args{
				format: "test error %d",
				a:      []interface{}{1},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := Errorf(tt.args.format, tt.args.a...); (err != nil) != tt.wantErr {
				t.Errorf("Errorf() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestWrapf(t *testing.T) {
	type args struct {
		err    error
		format string
		a      []interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "success test",
			args: args{
				err:    NewError(1, "test message"),
				format: "test error %d",
				a:      []interface{}{1},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := Wrapf(tt.args.err, tt.args.format, tt.args.a...); (err != nil) != tt.wantErr {
				t.Errorf("Wrapf() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestIgniteError_String(t *testing.T) {
	tests := []struct {
		name string
		e    *IgniteError
		want string
	}{
		{
			name: "success test",
			e: &IgniteError{IgniteStatus: 1, IgniteMessage: "test message",
				message: fmt.Sprintf("[%d] %s", 1, "test message")},
			want: "[1] test message",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.e.String(); got != tt.want {
				t.Errorf("IgniteError.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIgniteError_Error(t *testing.T) {
	tests := []struct {
		name string
		e    *IgniteError
		want string
	}{
		{
			name: "success test",
			e: &IgniteError{IgniteStatus: 1, IgniteMessage: "test message",
				message: fmt.Sprintf("[%d] %s", 1, "test message")},
			want: "[1] test message",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.e.Error(); got != tt.want {
				t.Errorf("IgniteError.Error() = %v, want %v", got, tt.want)
			}
		})
	}
}
