package ignite

import "testing"

func Test_HashCode(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name string
		args args
		want int32
	}{
		{
			name: "success test",
			args: args{
				s: "T",
			},
			want: 84,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := HashCode(tt.args.s); got != tt.want {
				t.Errorf("hashCode() = %v, want %v", got, tt.want)
			}
		})
	}
}
