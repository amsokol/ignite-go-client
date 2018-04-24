package ignite

import (
	"testing"
)

func TestNewRequestHandshake(t *testing.T) {
	type args struct {
		major int
		minor int
		patch int
	}
	tests := []struct {
		name    string
		args    args
		want    Request
		wantErr bool
	}{
		{
			name: "1",
			args: args{
				major: 1,
				minor: 0,
				patch: 0,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewRequestHandshake(tt.args.major, tt.args.minor, tt.args.patch)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewRequestHandshake() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}
