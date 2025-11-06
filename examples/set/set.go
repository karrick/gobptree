package main

import (
	"github.com/karrick/gobptree"
)

type SyncSetInt64 struct {
	tree *gobptree.Int64Tree
}

func NewSyncSetInt64() *SyncSetInt64 {
	tree, _ := gobptree.NewInt64Tree(64)
	return &SyncSetInt64{tree: tree}
}

func (s *SyncSetInt64) GetItems() []int64 {
	var items []int64
	scanner := s.tree.NewScanner(0)
	for scanner.Scan() {
		item, _ := scanner.Pair()
		items = append(items, item)
	}
	scanner.Close()
	return items
}

func updateTreeCallback(interface{}, bool) interface{} {
	return nil
}

func (s *SyncSetInt64) Set(item int64) {
	s.tree.Insert(item, struct{}{})
	// s.tree.Update(item, updateTreeCallback)
}

func (s *SyncSetInt64) Exists(item int64) bool {
	_, ok := s.tree.Search(item)
	return ok
}

func (s *SyncSetInt64) Delete(item int64) {
	s.tree.Delete(item)
}

func (s *SyncSetInt64) Len() int {
	var l int
	scanner := s.tree.NewScanner(0)
	for scanner.Scan() {
		l++
	}
	scanner.Close()
	return l
}
