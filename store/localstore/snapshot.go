// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package localstore

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/localstore/engine"
)

var (
	_ kv.Snapshot = (*dbSnapshot)(nil)
	_ kv.Iterator = (*dbIter)(nil)
)

// get raw key with version
func getDataKey(key []byte, ver kv.Version) []byte {
	return []byte(fmt.Sprintf("%s_%020d", key, ver))
}

// get metakey
func getMetaKey(key []byte) []byte {
	return []byte(fmt.Sprintf("%s_META", key))
}

type dbSnapshot struct {
	engine.Snapshot
}

func (s *dbSnapshot) getLatestVersionDetail(k []byte) (Version, bool, error) {
	metaKey := getMetaKey(k)
	v, err := s.Snapshot.Get(metaKey)
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	metaData, err := kv.DecodeValue(v)
	if err != nil {
		return nil, false, err
	}
	// value => [latestVersion, isDelete]
	return kv.Version(metaData[0]), metaData[1].(bool), nil
}

func (s *dbSnapshot) Get(k []byte, ver kv.Version) ([]byte, error) {
	// engine Snapshot return nil, nil for value not found,
	// so here we will check nil and return kv.ErrNotExist.
	var currentVer kv.Version
	if ver == kv.LatestVersion {
		currentVer, isDelete, err := s.getLatestVersionDetail(k)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if isDeleted {
			return nil, kv.ErrNotExist
		}
	} else {
		currentVer = ver
	}
	rawKey := getDataKey(k, currentVer)
	v, err := s.Snapshot.Get(rawKey)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, kv.ErrNotExist
	}
	return v, nil
}

func (s *dbSnapshot) NewIterator(param interface{}, ver kv.Version) kv.Iterator {
	startKey, ok := param.([]byte)
	if !ok {
		log.Errorf("leveldb iterator parameter error, %+v", param)
		return nil
	}
	it := s.Snapshot.NewIterator(startKey)

	var currentVer kv.Version
	if ver == kv.LatestVersion {
		currentVer, _, err := s.getLatestVersionDetail(k)
		if err != nil {
			log.Error(err)
			return nil
		}
	} else {
		currentVer = ver
	}
	return newDBIter(it, currentVer)
}

func (s *dbSnapshot) Release() {
	if s.Snapshot != nil {
		s.Snapshot.Release()
		s.Snapshot = nil
	}
}

type dbIter struct {
	engine.Iterator
	ver   kv.Version
	valid bool
}

func newDBIter(it engine.Iterator, ver kv.Version) *dbIter {
	return &dbIter{
		Iterator: it,
		ver:      ver,
		valid:    it.Next(),
	}
}

func (it *dbIter) Next(fn kv.FnKeyCmp) (kv.Iterator, error) {
	it.valid = it.Iterator.Next()
	return it, nil
}

func (it *dbIter) Valid() bool {
	return it.valid
}

func (it *dbIter) Key() string {
	return string(it.Iterator.Key())
}

func (it *dbIter) Value() []byte {
	return it.Iterator.Value()
}

func (it *dbIter) Close() {
	if it.Iterator != nil {
		it.Iterator.Release()
		it.Iterator = nil
	}
}
