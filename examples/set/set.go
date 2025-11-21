package main

import (
	"github.com/karrick/gobptree"
)

const order = 64

type SyncSetInt64 struct {
	tree *gobptree.GenericTree[int64, struct{}]
}

func NewSyncSetInt64() *SyncSetInt64 {
	tree, err := gobptree.NewGenericTree[int64, struct{}](order)
	if err != nil {
		panic(err)
	}
	return &SyncSetInt64{tree: tree}
}

func (s *SyncSetInt64) GetItems() []int64 {
	var keys []int64
	cursor := s.tree.NewScannerAll()
	for cursor.Scan() {
		key, _ := cursor.Pair()
		keys = append(keys, key)
	}
	if err := cursor.Close(); err != nil {
		panic(err)
	}
	return keys
}

func (s *SyncSetInt64) Len() int {
	var l int
	cursor := s.tree.NewScannerAll()
	for cursor.Scan() {
		l++
	}
	if err := cursor.Close(); err != nil {
		panic(err)
	}
	return l
}

func (s *SyncSetInt64) Set(item int64) {
	s.tree.Insert(item, struct{}{})
}

func (s *SyncSetInt64) Exists(item int64) bool {
	_, ok := s.tree.Search(item)
	return ok
}

func (s *SyncSetInt64) Delete(item int64) {
	s.tree.Delete(item)
}
