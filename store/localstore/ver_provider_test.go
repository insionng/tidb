package localstore

import "testing"

func TestLocalVersionProvider(t *testing.T) {
	l := &localVersionProvider{}
	m := map[uint64]struct{}{}
	for i := 0; i < 100000; i++ {
		ts := l.GetCurrentVersion()
		m[ts.(uint64)] = struct{}{}
	}

	if len(m) != 100000 {
		t.Error("generated same ts")
	}
}
