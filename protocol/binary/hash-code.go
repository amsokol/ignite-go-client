package binary

func hashCode(s string) int {
	if len(s) == 0 {
		return 1
	}

	h := uint32(0)
	for i := 0; i < len(s); i++ {
		h = 31*h + uint32(s[i])
	}
	return int(int32(h))
}
