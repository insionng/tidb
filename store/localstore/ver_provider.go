package localstore

import (
	"sync"
	"time"

	"github.com/pingcap/tidb/kv"
)

var _ kv.VersionProvider = (*localVersionProvider)(nil)

type localVersionProvider struct {
	mu              sync.Mutex
	lastTimeStampTs int64
	n               int64
}

func (l *localVersionProvider) GetCurrentVersion() kv.Version {
	l.mu.Lock()
	defer l.mu.Unlock()
	ts := (time.Now().UnixNano() / int64(time.Millisecond)) << 18
	if l.lastTimeStampTs == ts {
		l.n++
		return uint64(ts + l.n)
	} else {
		l.lastTimeStampTs = ts
		l.n = 0
	}
	return uint64(ts)
}
