package gobptree

import (
	"cmp"
	"fmt"
	"testing"
)

////////////////////////////////////////
// test helpers to ensure two nodes match
////////////////////////////////////////

func ensureItems[K cmp.Ordered](t *testing.T, tree *GenericTree[K], want []any) {
	t.Helper()

	var got []any

	scanner := tree.NewScannerAll()
	for scanner.Scan() {
		item, _ := scanner.Pair()
		got = append(got, item)
	}

	ensureError(t, scanner.Close())
	ensureSame(t, got, want)
}

func ensureInternalNodesMatch[K cmp.Ordered](t *testing.T, got, want *internalNode[K]) {
	t.Helper()

	if got == nil {
		if want != nil {
			t.Errorf("GOT: %#v; WANT: %#v", got, want)
		}
		return
	} else if want == nil {
		t.Errorf("GOT: %#v; WANT: %#v", got, want)
	}

	t.Run("Runts", func(t *testing.T) {
		t.Helper()
		ensureSame(t, got.Runts, want.Runts)
	})

	t.Run("Children", func(t *testing.T) {
		if g, w := len(got.Children), len(want.Children); g != w {
			t.Errorf("length(Children) GOT: %v; WANT: %v", g, w)
		}
		for i := 0; i < len(got.Children); i++ {
			ensureNodesMatch(t, got.Children[i], want.Children[i])
		}
	})
}

func ensureLeafNodesMatch[K cmp.Ordered](t *testing.T, got, want *leafNode[K]) {
	t.Helper()

	if got == nil {
		if want != nil {
			t.Errorf("GOT: %#v; WANT: %#v", got, want)
		}
		return
	} else if want == nil {
		t.Errorf("GOT: %#v; WANT: %#v", got, want)
	}

	t.Run("Runts", func(t *testing.T) {
		ensureSame(t, got.Runts, want.Runts)
	})

	t.Run("Values", func(t *testing.T) {
		ensureSame(t, got.Values, want.Values)
	})

	t.Run("Next", func(t *testing.T) {
		t.Skip("FIXME")
		ensureLeafNodesMatch(t, got.Next, want.Next)
	})
}

func ensureNodesMatch[K cmp.Ordered](t *testing.T, got, want node[K]) {
	t.Helper()

	switch e := want.(type) {
	case *internalNode[K]:
		a, ok := got.(*internalNode[K])
		if !ok {
			t.Errorf("GOT: %T; WANT: %T", got, want)
		}
		ensureInternalNodesMatch(t, a, e)
	case *leafNode[K]:
		a, ok := got.(*leafNode[K])
		if !ok {
			t.Errorf("GOT: %T; WANT: %T", got, want)
		}
		ensureLeafNodesMatch(t, a, e)
	default:
		t.Errorf("GOT: %T; WANT: node", got)
	}
}

////////////////////////////////////////
// test helpers to create new internal and leaf nodes
////////////////////////////////////////

func newInternalFrom[K cmp.Ordered](items ...node[K]) *internalNode[K] {
	n := &internalNode[K]{
		Runts:    make([]K, len(items)),
		Children: make([]node[K], len(items)),
	}
	for i := 0; i < len(items); i++ {
		n.Runts[i] = items[i].smallest()
		n.Children[i] = items[i]
	}
	return n
}

func newLeafFrom[K cmp.Ordered](next *leafNode[K], items ...K) *leafNode[K] {
	n := &leafNode[K]{
		Runts:  make([]K, len(items)),
		Values: make([]any, len(items)),
		Next:   next,
	}
	for i := 0; i < len(items); i++ {
		n.Runts[i] = items[i]
		n.Values[i] = items[i]
	}
	return n
}

////////////////////////////////////////
// tests
////////////////////////////////////////

func TestGenericBinarySearch(t *testing.T) {
	t.Run("skip Values", func(t *testing.T) {
		Values := []int64{1, 3, 5, 7, 9, 11, 13}

		if got, want := searchGreaterThanOrEqualTo(0, Values), 0; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := searchGreaterThanOrEqualTo(1, Values), 0; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := searchGreaterThanOrEqualTo(2, Values), 1; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := searchGreaterThanOrEqualTo(3, Values), 1; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := searchGreaterThanOrEqualTo(4, Values), 2; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := searchGreaterThanOrEqualTo(5, Values), 2; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := searchGreaterThanOrEqualTo(6, Values), 3; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := searchGreaterThanOrEqualTo(7, Values), 3; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := searchGreaterThanOrEqualTo(8, Values), 4; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := searchGreaterThanOrEqualTo(9, Values), 4; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := searchGreaterThanOrEqualTo(10, Values), 5; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := searchGreaterThanOrEqualTo(11, Values), 5; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := searchGreaterThanOrEqualTo(12, Values), 6; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := searchGreaterThanOrEqualTo(13, Values), 6; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := searchGreaterThanOrEqualTo(14, Values), 6; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
	})
	t.Run("greater than or equal to", func(t *testing.T) {
		t.Run("empty list", func(t *testing.T) {
			i := searchGreaterThanOrEqualTo(1, nil)
			if got, want := i, 0; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
		})
		t.Run("single item list", func(t *testing.T) {
			t.Run("key before", func(t *testing.T) {
				i := searchGreaterThanOrEqualTo(1, []int64{2})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match", func(t *testing.T) {
				i := searchGreaterThanOrEqualTo(2, []int64{2})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key after", func(t *testing.T) {
				i := searchGreaterThanOrEqualTo(3, []int64{2})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
		})
		t.Run("multiple item list", func(t *testing.T) {
			t.Run("key before first", func(t *testing.T) {
				i := searchGreaterThanOrEqualTo(1, []int64{2, 4, 6})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match first", func(t *testing.T) {
				i := searchGreaterThanOrEqualTo(2, []int64{2, 4, 6})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key between first and second", func(t *testing.T) {
				i := searchGreaterThanOrEqualTo(3, []int64{2, 4, 6})
				if got, want := i, 1; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match second", func(t *testing.T) {
				i := searchGreaterThanOrEqualTo(4, []int64{2, 4, 6})
				if got, want := i, 1; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key between second and third", func(t *testing.T) {
				i := searchGreaterThanOrEqualTo(5, []int64{2, 4, 6})
				if got, want := i, 2; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match third", func(t *testing.T) {
				i := searchGreaterThanOrEqualTo(6, []int64{2, 4, 6})
				if got, want := i, 2; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key after third", func(t *testing.T) {
				i := searchGreaterThanOrEqualTo(7, []int64{2, 4, 6})
				if got, want := i, 2; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
		})
	})
	t.Run("less than or equal to", func(t *testing.T) {
		t.Run("empty list", func(t *testing.T) {
			i := searchLessThanOrEqualTo(1, nil)
			if got, want := i, 0; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
		})
		t.Run("single item list", func(t *testing.T) {
			t.Run("key before", func(t *testing.T) {
				i := searchLessThanOrEqualTo(1, []int64{2})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match", func(t *testing.T) {
				i := searchLessThanOrEqualTo(int64(2), []int64{2})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key after", func(t *testing.T) {
				i := searchLessThanOrEqualTo(3, []int64{2})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
		})
		t.Run("multiple item list", func(t *testing.T) {
			t.Run("key before first", func(t *testing.T) {
				i := searchLessThanOrEqualTo(1, []int64{2, 4, 6})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match first", func(t *testing.T) {
				i := searchLessThanOrEqualTo(int64(2), []int64{2, 4, 6})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key between first and second", func(t *testing.T) {
				i := searchLessThanOrEqualTo(3, []int64{2, 4, 6})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match second", func(t *testing.T) {
				i := searchLessThanOrEqualTo(4, []int64{2, 4, 6})
				if got, want := i, 1; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key between second and third", func(t *testing.T) {
				i := searchLessThanOrEqualTo(5, []int64{2, 4, 6})
				if got, want := i, 1; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match third", func(t *testing.T) {
				i := searchLessThanOrEqualTo(6, []int64{2, 4, 6})
				if got, want := i, 2; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key after third", func(t *testing.T) {
				i := searchLessThanOrEqualTo(7, []int64{2, 4, 6})
				if got, want := i, 2; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
		})
	})
}

func TestNewGenericTreeReturnsErrorWhenInvalidOrder(t *testing.T) {
	for _, v := range []int{0, -1, 1, 3, 11} {
		_, err := NewGenericTree[int](v)
		if err == nil {
			ensureError(t, err, fmt.Sprintf("multiple of 2: %d", v))
		}
	}
}

func TestGenericInternalNodeMaybeSplit(t *testing.T) {
	leafD := newLeafFrom(nil, 40, 41, 42, 43)
	leafC := newLeafFrom(leafD, 30, 31, 32, 33)
	leafB := newLeafFrom(leafC, 20, 21, 22, 23)
	leafA := newLeafFrom(leafB, 10, 11, 12, 13)

	internal := newInternalFrom(leafA, leafB, leafC, leafD)

	t.Run("does nothing when not full", func(t *testing.T) {
		_, right := internal.maybeSplit(6)
		if right != nil {
			t.Errorf("GOT: %v; WANT: %v", right, nil)
		}
	})

	t.Run("splits when full", func(t *testing.T) {
		wantLeft := newInternalFrom(leafA, leafB)
		wantRight := newInternalFrom(leafC, leafD)

		gotLeft, gotRight := internal.maybeSplit(4)

		ensureNodesMatch(t, gotLeft, wantLeft)
		ensureNodesMatch(t, gotRight, wantRight)
	})
}

func TestGenericInternalNodeInsertSmallerKey(t *testing.T) {
	leafB := newLeafFrom(nil, 21, 22)
	leafA := newLeafFrom(leafB, 12, 13)

	internal := newInternalFrom(leafA, leafB)

	tree := &GenericTree[int]{root: internal, order: 4}

	tree.Insert(11, 11)

	if got, want := internal.Runts[0], 11; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
}

func TestGenericInsertOrder2(t *testing.T) {
	tree, err := NewGenericTree[int](2)
	ensureError(t, err)

	t.Run("1", func(t *testing.T) {
		tree.Insert(1, 1)

		t.Run("contents", func(t *testing.T) {
			ensureItems(t, tree, []any{1})
		})

		t.Run("structure", func(t *testing.T) {
			ensureNodesMatch(t, tree.root, newLeafFrom(nil, 1))
		})
	})

	t.Run("2", func(t *testing.T) {
		tree.Insert(2, 2)

		t.Run("contents", func(t *testing.T) {
			ensureItems(t, tree, []any{1, 2})
		})

		t.Run("structure", func(t *testing.T) {
			ensureNodesMatch(t, tree.root, newLeafFrom(nil, 1, 2))
		})
	})

	t.Run("3", func(t *testing.T) {
		tree.Insert(3, 3)

		t.Run("contents", func(t *testing.T) {
			ensureItems(t, tree, []any{1, 2, 3})
		})

		t.Run("structure", func(t *testing.T) {
			// internalA
			//   |
			//   + leafA
			//   |   |
			//   |   + 1
			//   |
			//   + leafB
			//       |
			//       + 2
			//       + 3
			leafB := newLeafFrom(nil, 2, 3)
			leafA := newLeafFrom(leafB, 1)
			internalA := newInternalFrom(leafA, leafB)
			ensureNodesMatch(t, tree.root, internalA)
		})
	})

	t.Run("4", func(t *testing.T) {
		tree.Insert(4, 4)

		t.Run("contents", func(t *testing.T) {
			ensureItems(t, tree, []any{1, 2, 3, 4})
		})

		t.Run("structure", func(t *testing.T) {
			// internalA
			//   |
			//   + internalB
			//   |   |
			//   |   + leafA
			//   |       |
			//   |       + 1
			//   |
			//   + internalC
			//       |
			//       + leafB
			//       |   |
			//       |   + 2
			//       |
			//       + leafC
			//           |
			//           + 3
			//           + 4
			leafC := newLeafFrom(nil, 3, 4)
			leafB := newLeafFrom(leafC, 2)
			leafA := newLeafFrom(leafB, 1)
			internalC := newInternalFrom(leafB, leafC)
			internalB := newInternalFrom(leafA)
			internalA := newInternalFrom(internalB, internalC)
			ensureNodesMatch(t, tree.root, internalA)
		})
	})

	t.Run("5", func(t *testing.T) {
		t.Skip("FIXME")
		tree.Insert(5, 5)

		t.Run("contents", func(t *testing.T) {
			ensureItems(t, tree, []any{1, 2, 3, 4, 5})
		})

		t.Run("structure", func(t *testing.T) {
			// internalA
			//   |
			//   + internalB
			//   |   |
			//   |   + leafA
			//   |       |
			//   |       + 1
			//   |
			//   + internalC
			//       |
			//       + internalD
			//       |   |
			//       |   + leafB
			//       |       |
			//       |       + 2
			//       |
			//       + internalE
			//           |
			//           + leafC
			//           |   |
			//           |   + 3
			//           |
			//           + leafD
			//               |
			//               + 4
			//               + 5
			leafD := newLeafFrom(nil, 4, 5)
			leafC := newLeafFrom(leafD, 3)
			leafB := newLeafFrom(leafC, 2)
			leafA := newLeafFrom(leafB, 1)
			internalE := newInternalFrom(leafC, leafD)
			internalD := newInternalFrom(leafB)
			internalC := newInternalFrom(internalD, internalE)
			internalB := newInternalFrom(leafA)
			internalA := newInternalFrom(internalB, internalC)
			ensureNodesMatch(t, tree.root, internalA)
		})
	})

	// t.Run("5", func(t *testing.T) {
	// 	tree.Insert(5, 5)

	// 	leafB := newLeafFrom(nil, 3, 4, 5)
	// 	leafA := newLeafFrom(leafB, 1, 2)
	// 	ensureNodesMatch(t, tree.root, newInternalFrom(leafA, leafB))
	// })

	// t.Run("6", func(t *testing.T) {
	// 	tree.Insert(6, 6)

	// 	leafB := newLeafFrom(nil, 3, 4, 5, 6)
	// 	leafA := newLeafFrom(leafB, 1, 2)
	// 	ensureNodesMatch(t, tree.root, newInternalFrom(leafA, leafB))
	// })

	// t.Run("7", func(t *testing.T) {
	// 	tree.Insert(7, 7)

	// 	leafC := newLeafFrom(nil, 5, 6, 7)
	// 	leafB := newLeafFrom(leafC, 3, 4)
	// 	leafA := newLeafFrom(leafB, 1, 2)
	// 	ensureNodesMatch(t, tree.root, newInternalFrom(leafA, leafB, leafC))
	// })

	// t.Run("8", func(t *testing.T) {
	// 	tree.Insert(8, 8)

	// 	leafC := newLeafFrom(nil, 5, 6, 7, 8)
	// 	leafB := newLeafFrom(leafC, 3, 4)
	// 	leafA := newLeafFrom(leafB, 1, 2)
	// 	ensureNodesMatch(t, tree.root, newInternalFrom(leafA, leafB, leafC))
	// })

	// t.Run("9", func(t *testing.T) {
	// 	tree.Insert(9, 9)

	// 	leafD := newLeafFrom(nil, 7, 8, 9)
	// 	leafC := newLeafFrom(leafD, 5, 6)
	// 	leafB := newLeafFrom(leafC, 3, 4)
	// 	leafA := newLeafFrom(leafB, 1, 2)
	// 	ensureNodesMatch(t, tree.root, newInternalFrom(leafA, leafB, leafC, leafD))
	// })

	// t.Run("10", func(t *testing.T) {
	// 	tree.Insert(10, 10)

	// 	// root -> internal A
	// 	// internal A -> internal B, internal C
	// 	// internal B -> leaf A, leaf B
	// 	// internal C -> leaf C, leaf D

	// 	leafD := newLeafFrom(nil, 7, 8, 9, 10)
	// 	leafC := newLeafFrom(leafD, 5, 6)
	// 	leafB := newLeafFrom(leafC, 3, 4)
	// 	leafA := newLeafFrom(leafB, 1, 2)

	// 	internalC := newInternalFrom(leafC, leafD)
	// 	internalB := newInternalFrom(leafA, leafB)
	// 	internalA := newInternalFrom(internalB, internalC)
	// 	ensureNodesMatch(t, tree.root, internalA)
	// })

	// t.Run("11", func(t *testing.T) {
	// 	tree.Insert(11, 11)

	// 	// root -> internal A
	// 	// internal A -> internal B, internal C
	// 	// internal B -> leaf A, leaf B
	// 	// internal C -> leaf C, leaf D, leaf E

	// 	leafE := newLeafFrom(nil, 9, 10, 11)
	// 	leafD := newLeafFrom(leafE, 7, 8)
	// 	leafC := newLeafFrom(leafD, 5, 6)
	// 	leafB := newLeafFrom(leafC, 3, 4)
	// 	leafA := newLeafFrom(leafB, 1, 2)

	// 	internalC := newInternalFrom(leafC, leafD, leafE)
	// 	internalB := newInternalFrom(leafA, leafB)
	// 	internalA := newInternalFrom(internalB, internalC)
	// 	ensureNodesMatch(t, tree.root, internalA)
	// })

	// t.Run("12", func(t *testing.T) {
	// 	tree.Insert(12, 12)

	// 	// root -> internal A
	// 	// internal A -> internal B, internal C
	// 	// internal B -> leaf A, leaf B
	// 	// internal C -> leaf C, leaf D, leaf E

	// 	leafE := newLeafFrom(nil, 9, 10, 11, 12)
	// 	leafD := newLeafFrom(leafE, 7, 8)
	// 	leafC := newLeafFrom(leafD, 5, 6)
	// 	leafB := newLeafFrom(leafC, 3, 4)
	// 	leafA := newLeafFrom(leafB, 1, 2)

	// 	internalC := newInternalFrom(leafC, leafD, leafE)
	// 	internalB := newInternalFrom(leafA, leafB)
	// 	internalA := newInternalFrom(internalB, internalC)
	// 	ensureNodesMatch(t, tree.root, internalA)
	// })

	// t.Run("13", func(t *testing.T) {
	// 	tree.Insert(13, 13)

	// 	// root -> internal A
	// 	// internal A -> internal B, internal C
	// 	// internal B -> leaf A, leaf B
	// 	// internal C -> leaf C, leaf D, leaf E, leaf F

	// 	leafF := newLeafFrom(nil, 11, 12, 13)
	// 	leafE := newLeafFrom(leafF, 9, 10)
	// 	leafD := newLeafFrom(leafE, 7, 8)
	// 	leafC := newLeafFrom(leafD, 5, 6)
	// 	leafB := newLeafFrom(leafC, 3, 4)
	// 	leafA := newLeafFrom(leafB, 1, 2)

	// 	internalC := newInternalFrom(leafC, leafD, leafE, leafF)
	// 	internalB := newInternalFrom(leafA, leafB)
	// 	internalA := newInternalFrom(internalB, internalC)
	// 	ensureNodesMatch(t, tree.root, internalA)
	// })

	// t.Run("14", func(t *testing.T) {
	// 	tree.Insert(14, 14)

	// 	// root -> internal A
	// 	// internal A -> internal B, internal C
	// 	// internal B -> leaf A, leaf B
	// 	// internal C -> leaf C, leaf D, leaf E, leaf F

	// 	leafF := newLeafFrom(nil, 11, 12, 13, 14)
	// 	leafE := newLeafFrom(leafF, 9, 10)
	// 	leafD := newLeafFrom(leafE, 7, 8)
	// 	leafC := newLeafFrom(leafD, 5, 6)
	// 	leafB := newLeafFrom(leafC, 3, 4)
	// 	leafA := newLeafFrom(leafB, 1, 2)

	// 	internalC := newInternalFrom(leafC, leafD, leafE, leafF)
	// 	internalB := newInternalFrom(leafA, leafB)
	// 	internalA := newInternalFrom(internalB, internalC)
	// 	ensureNodesMatch(t, tree.root, internalA)
	// })

	// t.Run("15", func(t *testing.T) {
	// 	tree.Insert(15, 15)

	// 	// root -> internal A
	// 	// internal A -> internal B, internal C, internal D
	// 	// internal B -> leaf A, leaf B
	// 	// internal C -> leaf C, leaf D
	// 	// internal D -> leaf E, leaf F, leaf G

	// 	leafG := newLeafFrom(nil, 13, 14, 15)
	// 	leafF := newLeafFrom(leafG, 11, 12)
	// 	leafE := newLeafFrom(leafF, 9, 10)
	// 	leafD := newLeafFrom(leafE, 7, 8)
	// 	leafC := newLeafFrom(leafD, 5, 6)
	// 	leafB := newLeafFrom(leafC, 3, 4)
	// 	leafA := newLeafFrom(leafB, 1, 2)

	// 	internalD := newInternalFrom(leafE, leafF, leafG)
	// 	internalC := newInternalFrom(leafC, leafD)
	// 	internalB := newInternalFrom(leafA, leafB)
	// 	internalA := newInternalFrom(internalB, internalC, internalD)
	// 	ensureNodesMatch(t, tree.root, internalA)
	// })
}

func TestGenericInsertOrder4(t *testing.T) {
	tree, err := NewGenericTree[int](4)
	ensureError(t, err)

	t.Run("1", func(t *testing.T) {
		tree.Insert(1, 1)
		ensureNodesMatch(t, tree.root, newLeafFrom(nil, 1))
	})

	t.Run("2", func(t *testing.T) {
		tree.Insert(2, 2)
		ensureNodesMatch(t, tree.root, newLeafFrom(nil, 1, 2))
	})

	t.Run("3", func(t *testing.T) {
		tree.Insert(3, 3)
		ensureNodesMatch(t, tree.root, newLeafFrom(nil, 1, 2, 3))
	})

	t.Run("4", func(t *testing.T) {
		tree.Insert(4, 4)
		ensureNodesMatch(t, tree.root, newLeafFrom(nil, 1, 2, 3, 4))
	})

	t.Run("5", func(t *testing.T) {
		tree.Insert(5, 5)

		leafB := newLeafFrom(nil, 3, 4, 5)
		leafA := newLeafFrom(leafB, 1, 2)
		ensureNodesMatch(t, tree.root, newInternalFrom(leafA, leafB))
	})

	t.Run("6", func(t *testing.T) {
		tree.Insert(6, 6)

		leafB := newLeafFrom(nil, 3, 4, 5, 6)
		leafA := newLeafFrom(leafB, 1, 2)
		ensureNodesMatch(t, tree.root, newInternalFrom(leafA, leafB))
	})

	t.Run("7", func(t *testing.T) {
		tree.Insert(7, 7)

		leafC := newLeafFrom(nil, 5, 6, 7)
		leafB := newLeafFrom(leafC, 3, 4)
		leafA := newLeafFrom(leafB, 1, 2)
		ensureNodesMatch(t, tree.root, newInternalFrom(leafA, leafB, leafC))
	})

	t.Run("8", func(t *testing.T) {
		tree.Insert(8, 8)

		leafC := newLeafFrom(nil, 5, 6, 7, 8)
		leafB := newLeafFrom(leafC, 3, 4)
		leafA := newLeafFrom(leafB, 1, 2)
		ensureNodesMatch(t, tree.root, newInternalFrom(leafA, leafB, leafC))
	})

	t.Run("9", func(t *testing.T) {
		tree.Insert(9, 9)

		leafD := newLeafFrom(nil, 7, 8, 9)
		leafC := newLeafFrom(leafD, 5, 6)
		leafB := newLeafFrom(leafC, 3, 4)
		leafA := newLeafFrom(leafB, 1, 2)
		ensureNodesMatch(t, tree.root, newInternalFrom(leafA, leafB, leafC, leafD))
	})

	t.Run("10", func(t *testing.T) {
		tree.Insert(10, 10)

		// root -> internal A
		// internal A -> internal B, internal C
		// internal B -> leaf A, leaf B
		// internal C -> leaf C, leaf D

		leafD := newLeafFrom(nil, 7, 8, 9, 10)
		leafC := newLeafFrom(leafD, 5, 6)
		leafB := newLeafFrom(leafC, 3, 4)
		leafA := newLeafFrom(leafB, 1, 2)

		internalC := newInternalFrom(leafC, leafD)
		internalB := newInternalFrom(leafA, leafB)
		internalA := newInternalFrom(internalB, internalC)
		ensureNodesMatch(t, tree.root, internalA)
	})

	t.Run("11", func(t *testing.T) {
		tree.Insert(11, 11)

		// root -> internal A
		// internal A -> internal B, internal C
		// internal B -> leaf A, leaf B
		// internal C -> leaf C, leaf D, leaf E

		leafE := newLeafFrom(nil, 9, 10, 11)
		leafD := newLeafFrom(leafE, 7, 8)
		leafC := newLeafFrom(leafD, 5, 6)
		leafB := newLeafFrom(leafC, 3, 4)
		leafA := newLeafFrom(leafB, 1, 2)

		internalC := newInternalFrom(leafC, leafD, leafE)
		internalB := newInternalFrom(leafA, leafB)
		internalA := newInternalFrom(internalB, internalC)
		ensureNodesMatch(t, tree.root, internalA)
	})

	t.Run("12", func(t *testing.T) {
		tree.Insert(12, 12)

		// root -> internal A
		// internal A -> internal B, internal C
		// internal B -> leaf A, leaf B
		// internal C -> leaf C, leaf D, leaf E

		leafE := newLeafFrom(nil, 9, 10, 11, 12)
		leafD := newLeafFrom(leafE, 7, 8)
		leafC := newLeafFrom(leafD, 5, 6)
		leafB := newLeafFrom(leafC, 3, 4)
		leafA := newLeafFrom(leafB, 1, 2)

		internalC := newInternalFrom(leafC, leafD, leafE)
		internalB := newInternalFrom(leafA, leafB)
		internalA := newInternalFrom(internalB, internalC)
		ensureNodesMatch(t, tree.root, internalA)
	})

	t.Run("13", func(t *testing.T) {
		tree.Insert(13, 13)

		// root -> internal A
		// internal A -> internal B, internal C
		// internal B -> leaf A, leaf B
		// internal C -> leaf C, leaf D, leaf E, leaf F

		leafF := newLeafFrom(nil, 11, 12, 13)
		leafE := newLeafFrom(leafF, 9, 10)
		leafD := newLeafFrom(leafE, 7, 8)
		leafC := newLeafFrom(leafD, 5, 6)
		leafB := newLeafFrom(leafC, 3, 4)
		leafA := newLeafFrom(leafB, 1, 2)

		internalC := newInternalFrom(leafC, leafD, leafE, leafF)
		internalB := newInternalFrom(leafA, leafB)
		internalA := newInternalFrom(internalB, internalC)
		ensureNodesMatch(t, tree.root, internalA)
	})

	// t.Run("14", func(t *testing.T) {
	// 	tree.Insert(14, 14)

	// 	// root -> internal A
	// 	// internal A -> internal B, internal C
	// 	// internal B -> leaf A, leaf B
	// 	// internal C -> leaf C, leaf D, leaf E, leaf F

	// 	leafF := newLeafFrom(nil, 11, 12, 13, 14)
	// 	leafE := newLeafFrom(leafF, 9, 10)
	// 	leafD := newLeafFrom(leafE, 7, 8)
	// 	leafC := newLeafFrom(leafD, 5, 6)
	// 	leafB := newLeafFrom(leafC, 3, 4)
	// 	leafA := newLeafFrom(leafB, 1, 2)

	// 	internalC := newInternalFrom(leafC, leafD, leafE, leafF)
	// 	internalB := newInternalFrom(leafA, leafB)
	// 	internalA := newInternalFrom(internalB, internalC)
	// 	ensureNodesMatch(t, tree.root, internalA)
	// })

	// t.Run("15", func(t *testing.T) {
	// 	tree.Insert(15, 15)

	// 	// root -> internal A
	// 	// internal A -> internal B, internal C, internal D
	// 	// internal B -> leaf A, leaf B
	// 	// internal C -> leaf C, leaf D
	// 	// internal D -> leaf E, leaf F, leaf G

	// 	leafG := newLeafFrom(nil, 13, 14, 15)
	// 	leafF := newLeafFrom(leafG, 11, 12)
	// 	leafE := newLeafFrom(leafF, 9, 10)
	// 	leafD := newLeafFrom(leafE, 7, 8)
	// 	leafC := newLeafFrom(leafD, 5, 6)
	// 	leafB := newLeafFrom(leafC, 3, 4)
	// 	leafA := newLeafFrom(leafB, 1, 2)

	// 	internalD := newInternalFrom(leafE, leafF, leafG)
	// 	internalC := newInternalFrom(leafC, leafD)
	// 	internalB := newInternalFrom(leafA, leafB)
	// 	internalA := newInternalFrom(internalB, internalC, internalD)
	// 	ensureNodesMatch(t, tree.root, internalA)
	// })
}

func TestGenericLeafNodeMaybeSplit(t *testing.T) {
	gimme := func() (*leafNode[int], *leafNode[int]) {
		leafB := newLeafFrom(nil, 21, 22, 23, 24)
		leafA := newLeafFrom(leafB, 11, 12, 13, 14)
		return leafA, leafB
	}

	t.Run("when not full does nothing", func(t *testing.T) {
		_, leafB := gimme()
		_, right := leafB.maybeSplit(6)
		if right != nil {
			t.Errorf("GOT: %v; WANT: %v", right, nil)
		}
	})

	t.Run("splits non-right edge when full", func(t *testing.T) {
		leafA, leafB := gimme()
		leftNode, rightNode := leafA.maybeSplit(4)
		ensureNodesMatch(t, leftNode, newLeafFrom(rightNode.(*leafNode[int]), 11, 12))
		ensureNodesMatch(t, rightNode, newLeafFrom(leafB, 13, 14))
	})
	t.Run("splits right edge when full", func(t *testing.T) {
		leafA, leafB := gimme()
		leftNode, rightNode := leafB.maybeSplit(4)
		if got, want := leafA.Next, leftNode; got != want {
			t.Fatalf("GOT: %v; WANT: %v", got, want)
		}
		ensureNodesMatch(t, leftNode, newLeafFrom(rightNode.(*leafNode[int]), 21, 22))
		ensureNodesMatch(t, rightNode, newLeafFrom(nil, 23, 24))
	})
}

func TestInsertIntoSingleLeafGenericTree(t *testing.T) {
	t.Run("when fewer than order elements", func(t *testing.T) {
		t.Run("when empty", func(t *testing.T) {
			d, _ := NewGenericTree[int](4)
			nl, ok := d.root.(*leafNode[int])
			if !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, false)
			}
			d.Insert(30, 30)
			ensureLeafNodesMatch(t, nl, newLeafFrom(nil, 30))
		})
		t.Run("when less than first runt", func(t *testing.T) {
			d, _ := NewGenericTree[int](4)
			nl, ok := d.root.(*leafNode[int])
			if !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, false)
			}
			d.Insert(30, 30)
			d.Insert(10, 10)
			ensureNodesMatch(t, nl, newLeafFrom(nil, 10, 30))
		})
		t.Run("when update value", func(t *testing.T) {
			d, _ := NewGenericTree[int](4)
			nl, ok := d.root.(*leafNode[int])
			if !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, false)
			}
			d.Insert(30, 30)
			d.Insert(10, 10)
			d.Insert(30, 333)
			ensureNodesMatch(t, nl, &leafNode[int]{
				Runts:  []int{10, 30},
				Values: []any{10, 333},
			})
		})
		t.Run("when between first and final runt", func(t *testing.T) {
			d, _ := NewGenericTree[int](4)
			nl, ok := d.root.(*leafNode[int])
			if !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, false)
			}
			d.Insert(30, 30)
			d.Insert(10, 10)
			d.Insert(20, 20)
			ensureNodesMatch(t, nl, newLeafFrom(nil, 10, 20, 30))
		})
		t.Run("when after final runt", func(t *testing.T) {
			d, _ := NewGenericTree[int](4)
			nl, ok := d.root.(*leafNode[int])
			if !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, false)
			}
			d.Insert(30, 30)
			d.Insert(10, 10)
			d.Insert(20, 20)
			d.Insert(40, 40)
			ensureNodesMatch(t, nl, newLeafFrom(nil, 10, 20, 30, 40))
		})
	})

	t.Run("when insertion splits single leaf node", func(t *testing.T) {
		gimme := func() *GenericTree[int] {
			d, _ := NewGenericTree[int](4)
			for _, v := range []int{10, 20, 30, 40} {
				d.Insert(v, v)
			}
			return d
		}
		t.Run("when new key will be first node in left leaf", func(t *testing.T) {
			d := gimme()
			d.Insert(0, 0)

			root, ok := d.root.(*internalNode[int])
			if !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, true)
			}
			// root should have two Runts and two leaf nodes for Children
			if got, want := len(root.Runts), 2; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := len(root.Children), 2; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			// ensure Children nodes are as expected for this case
			if got, want := root.Runts[0], 0; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			ensureNodesMatch(t, root.Children[0], newLeafFrom(root.Children[1].(*leafNode[int]), 0, 10, 20))

			if got, want := root.Runts[1], 30; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			ensureNodesMatch(t, root.Children[1], newLeafFrom(nil, 30, 40))
		})
		t.Run("when new key is in middle", func(t *testing.T) {
			d := gimme()
			d.Insert(25, 25)
			root, ok := d.root.(*internalNode[int])
			if !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, true)
			}
			// root should have two Runts and two leaf nodes for Children
			if got, want := len(root.Runts), 2; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := len(root.Children), 2; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			// ensure Children nodes are as expected for this case
			if got, want := root.Runts[0], 10; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			ensureNodesMatch(t, root.Children[0], newLeafFrom(root.Children[1].(*leafNode[int]), 10, 20, 25))

			if got, want := root.Runts[1], 30; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			ensureNodesMatch(t, root.Children[1], newLeafFrom(nil, 30, 40))
		})
		t.Run("when new key will be final node in right leaf", func(t *testing.T) {
			d := gimme()
			d.Insert(50, 50)
			root, ok := d.root.(*internalNode[int])
			if !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, true)
			}
			// root should have two Runts and two leaf nodes for Children
			if got, want := len(root.Runts), 2; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := len(root.Children), 2; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			// ensure Children nodes are as expected for this case
			if got, want := root.Runts[0], 10; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			ensureNodesMatch(t, root.Children[0], newLeafFrom(root.Children[1].(*leafNode[int]), 10, 20))

			if got, want := root.Runts[1], 30; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			ensureNodesMatch(t, root.Children[1], newLeafFrom(nil, 30, 40, 50))
		})
	})
}

func TestGenericTreeSearch(t *testing.T) {
	t.Run("empty tree", func(t *testing.T) {
		d, _ := NewGenericTree[int](16)

		_, ok := d.Search(13)
		if got, want := ok, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
	})
	t.Run("single-leaf tree", func(t *testing.T) {
		t.Run("missing value", func(t *testing.T) {
			d, _ := NewGenericTree[int](16)
			for i := 0; i < 15; i++ {
				if i != 13 {
					d.Insert(i, i)
				}
			}

			_, ok := d.Search(13)
			if got, want := ok, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
		})
		t.Run("existing value", func(t *testing.T) {
			d, _ := NewGenericTree[int](16)
			for i := 0; i < 15; i++ {
				if i != 13 {
					d.Insert(i, i)
				}
			}

			value, ok := d.Search(8)
			if got, want := ok, true; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := value, 8; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
		})
	})
	t.Run("multi-leaf tree", func(t *testing.T) {
		t.Run("missing value", func(t *testing.T) {
			d, _ := NewGenericTree[int](4)
			for i := 0; i < 15; i++ {
				if i != 13 {
					d.Insert(i, i)
				}
			}

			_, ok := d.Search(13)
			if got, want := ok, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
		})
		t.Run("existing value", func(t *testing.T) {
			d, _ := NewGenericTree[int](4)
			for i := 0; i < 15; i++ {
				if i != 13 {
					d.Insert(i, i)
				}
			}

			value, ok := d.Search(8)
			if got, want := ok, true; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := value, 8; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
		})
	})
}

func TestGenericTreeCursor(t *testing.T) {
	t.Run("empty tree", func(t *testing.T) {
		var count int

		d, _ := NewGenericTree[int](4)
		c := d.NewScanner(0)
		for c.Scan() {
			count++
		}

		if got, want := count, 0; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
	})
	t.Run("single-leaf tree", func(t *testing.T) {
		t.Run("scan for zero-value element", func(t *testing.T) {
			var Values []any

			d, _ := NewGenericTree[int](16)
			for i := 0; i < 15; i++ {
				d.Insert(i, i)
			}

			c := d.NewScanner(0)
			for c.Scan() {
				_, v := c.Pair()
				Values = append(Values, v)
			}

			expected := []any{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}

			ensureSame(t, Values, expected)
		})
		t.Run("scan for missing element", func(t *testing.T) {
			var Values []any

			d, _ := NewGenericTree[int](16)
			for i := 0; i < 15; i++ {
				if i != 13 {
					d.Insert(i, i)
				}
			}

			c := d.NewScanner(13)
			for c.Scan() {
				_, v := c.Pair()
				Values = append(Values, v)
			}

			expected := []any{14} // , 2, 3, 4, 5, 6, 7, 8, 9}

			ensureSame(t, Values, expected)
		})
		t.Run("scan for existing element", func(t *testing.T) {
			var Values []any

			d, _ := NewGenericTree[int](16)
			for i := 0; i < 15; i++ {
				d.Insert(i, i)
			}

			c := d.NewScanner(13)
			for c.Scan() {
				_, v := c.Pair()
				Values = append(Values, v)
			}

			expected := []any{13, 14} // , 2, 3, 4, 5, 6, 7, 8, 9}

			ensureSame(t, Values, expected)
		})
	})
	t.Run("multi-leaf tree", func(t *testing.T) {
		var Values []any

		d, _ := NewGenericTree[int](4)
		for i := 0; i < 15; i++ {
			d.Insert(i, i)
		}

		c := d.NewScanner(0)
		for c.Scan() {
			_, v := c.Pair()
			Values = append(Values, v)
		}

		expected := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}

		for i := 0; i < len(Values) && i < len(expected); i++ {
			if got, want := Values[i], expected[i]; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
		}
	})
}

func TestGenericTreeUpdate(t *testing.T) {
	d, _ := NewGenericTree[int](8)
	d.Update(1, func(value interface{}, ok bool) interface{} {
		if got, want := ok, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := value, error(nil); got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		return "first"
	})
	d.Update(1, func(value interface{}, ok bool) interface{} {
		if got, want := ok, true; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := value, "first"; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		return "second"
	})
	value, ok := d.Search(1)
	if got, want := ok, true; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := value, "second"; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	d.Insert(3, 3)
	d.Update(2, func(value interface{}, ok bool) interface{} {
		if got, want := ok, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := value, error(nil); got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		return "fourth"
	})
	value, ok = d.Search(2)
	if got, want := ok, true; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := value, "fourth"; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
}

func TestGenericLeafNodeDelete(t *testing.T) {
	t.Run("still big enough", func(t *testing.T) {
		t.Run("key is missing", func(t *testing.T) {
			l := &leafNode[int]{
				Runts:  []int{11, 21, 31},
				Values: []any{11, 21, 31},
			}
			bigEnough := l.deleteKey(2, 42)
			if got, want := bigEnough, true; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureNodesMatch(t, l, &leafNode[int]{
				Runts:  []int{11, 21, 31},
				Values: []any{11, 21, 31},
			})
		})
		t.Run("key is first", func(t *testing.T) {
			l := &leafNode[int]{
				Runts:  []int{11, 21, 31},
				Values: []any{11, 21, 31},
			}
			bigEnough := l.deleteKey(2, 11)
			if got, want := bigEnough, true; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureNodesMatch(t, l, &leafNode[int]{
				Runts:  []int{21, 31},
				Values: []any{21, 31},
			})
		})
		t.Run("key is middle", func(t *testing.T) {
			l := &leafNode[int]{
				Runts:  []int{11, 21, 31},
				Values: []any{11, 21, 31},
			}
			bigEnough := l.deleteKey(2, 21)
			if got, want := bigEnough, true; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureNodesMatch(t, l, &leafNode[int]{
				Runts:  []int{11, 31},
				Values: []any{11, 31},
			})
		})
		t.Run("key is last", func(t *testing.T) {
			l := &leafNode[int]{
				Runts:  []int{11, 21, 31},
				Values: []any{11, 21, 31},
			}
			bigEnough := l.deleteKey(2, 31)
			if got, want := bigEnough, true; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureNodesMatch(t, l, &leafNode[int]{
				Runts:  []int{11, 21},
				Values: []any{11, 21},
			})
		})
	})
	t.Run("will be too small", func(t *testing.T) {
		l := newLeafFrom(nil, 11, 21, 31, 41)
		bigEnough := l.deleteKey(4, 21)
		if got, want := bigEnough, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		ensureNodesMatch(t, l, newLeafFrom(nil, 11, 31, 41))
	})
}

func TestGenericLeafNodeAdoptFrom(t *testing.T) {
	t.Run("left", func(t *testing.T) {
		r := newLeafFrom(nil, 5, 6, 7)
		l := newLeafFrom(r, 0, 1, 2, 3, 4)

		r.adoptFromLeft(l)

		ensureNodesMatch(t, l, newLeafFrom(r, 0, 1, 2, 3))
		ensureNodesMatch(t, r, newLeafFrom(nil, 4, 5, 6, 7))
	})
	t.Run("right", func(t *testing.T) {
		r := newLeafFrom(nil, 3, 4, 5, 6, 7)
		l := newLeafFrom(r, 0, 1, 2)

		l.adoptFromRight(r)

		ensureNodesMatch(t, l, newLeafFrom(r, 0, 1, 2, 3))
		ensureNodesMatch(t, r, newLeafFrom(nil, 4, 5, 6, 7))
	})
}

func TestGenericInternalNodeAdoptFrom(t *testing.T) {
	t.Run("left", func(t *testing.T) {
		leafI := newLeafFrom(nil, 90, 92, 94, 96, 98)
		leafH := newLeafFrom(leafI, 80, 82, 84, 86, 88)
		leafG := newLeafFrom(leafH, 70, 72, 74, 76, 78)
		leafF := newLeafFrom(leafG, 60, 62, 64, 66, 68)
		leafE := newLeafFrom(leafF, 50, 52, 54, 56, 58)
		leafD := newLeafFrom(leafE, 40, 42, 44, 46, 48)
		leafC := newLeafFrom(leafD, 30, 32, 34, 36, 38)
		leafB := newLeafFrom(leafC, 20, 22, 24, 26, 28)
		leafA := newLeafFrom(leafB, 10, 12, 14, 16, 18)

		left := newInternalFrom(leafA, leafB, leafC, leafD, leafE, leafF)
		right := newInternalFrom(leafG, leafH, leafI)

		right.adoptFromLeft(left)

		ensureInternalNodesMatch(t, left, newInternalFrom(leafA, leafB, leafC, leafD, leafE))
		ensureInternalNodesMatch(t, right, newInternalFrom(leafF, leafG, leafH, leafI))
	})
	t.Run("right", func(t *testing.T) {
		leafI := newLeafFrom(nil, 90, 92, 94, 96, 98)
		leafH := newLeafFrom(leafI, 80, 82, 84, 86, 88)
		leafG := newLeafFrom(leafH, 70, 72, 74, 76, 78)
		leafF := newLeafFrom(leafG, 60, 62, 64, 66, 68)
		leafE := newLeafFrom(leafF, 50, 52, 54, 56, 58)
		leafD := newLeafFrom(leafE, 40, 42, 44, 46, 48)
		leafC := newLeafFrom(leafD, 30, 32, 34, 36, 38)
		leafB := newLeafFrom(leafC, 20, 22, 24, 26, 28)
		leafA := newLeafFrom(leafB, 10, 12, 14, 16, 18)

		left := newInternalFrom(leafA, leafB, leafC)
		right := newInternalFrom(leafD, leafE, leafF, leafG, leafH, leafI)

		left.adoptFromRight(right)

		ensureInternalNodesMatch(t, left, newInternalFrom(leafA, leafB, leafC, leafD))
		ensureInternalNodesMatch(t, right, newInternalFrom(leafE, leafF, leafG, leafH, leafI))
	})
}

func TestGenericLeafNodeMergeWithRight(t *testing.T) {
	leafC := newLeafFrom(nil, 6, 7, 8, 9)
	leafB := newLeafFrom(leafC, 3, 4, 5)
	leafA := newLeafFrom(leafB, 0, 1, 2)

	leafA.absorbRight(leafB)

	ensureNodesMatch(t, leafA, newLeafFrom(leafC, 0, 1, 2, 3, 4, 5))

	if got, want := len(leafB.Runts), 0; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := len(leafB.Values), 0; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := leafB.Next, (*leafNode[int])(nil); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
}

func TestGenericInternalNodeMergeWithRight(t *testing.T) {
	leafI := newLeafFrom(nil, 90, 92, 94, 96, 98)
	leafH := newLeafFrom(leafI, 80, 82, 84, 86, 88)
	leafG := newLeafFrom(leafH, 70, 72, 74, 76, 78)
	leafF := newLeafFrom(leafG, 60, 62, 64, 66, 68)
	leafE := newLeafFrom(leafF, 50, 52, 54, 56, 58)
	leafD := newLeafFrom(leafE, 40, 42, 44, 46, 48)
	leafC := newLeafFrom(leafD, 30, 32, 34, 36, 38)
	leafB := newLeafFrom(leafC, 20, 22, 24, 26, 28)
	leafA := newLeafFrom(leafB, 10, 12, 14, 16, 18)

	left := newInternalFrom(leafA, leafB, leafC)
	right := newInternalFrom(leafD, leafE, leafF, leafG)

	left.absorbRight(right)

	ensureInternalNodesMatch(t, left, newInternalFrom(leafA, leafB, leafC, leafD, leafE, leafF, leafG))

	if got, want := len(right.Runts), 0; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := len(right.Children), 0; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
}

func TestGenericInternalNodeDeleteKey(t *testing.T) {
	t.Run("not too small", func(t *testing.T) {
		leafE := newLeafFrom(nil, 50, 52, 54, 56, 58)
		leafD := newLeafFrom(leafE, 40, 42, 44, 46, 48)
		leafC := newLeafFrom(leafD, 30, 32, 34, 36, 38)
		leafB := newLeafFrom(leafC, 20, 22, 24, 26, 28)
		leafA := newLeafFrom(leafB, 10, 12, 14, 16, 18)

		internal := newInternalFrom(leafA, leafB, leafC, leafD)

		bigEnough := internal.deleteKey(4, 22)
		if got, want := bigEnough, true; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
	})
	t.Run("internal node absorbs right when no left and skinny right", func(t *testing.T) {
		t.Run("internal node not too small", func(t *testing.T) {
			leafE := newLeafFrom(nil, 50, 52, 54, 56, 58)
			leafD := newLeafFrom(leafE, 40, 42, 44, 46, 48)
			leafC := newLeafFrom(leafD, 30, 32, 34, 36, 38)
			leafB := newLeafFrom(leafC, 20, 22, 24, 26)
			leafA := newLeafFrom(leafB, 10, 12, 14, 16)

			internal := newInternalFrom(leafA, leafB, leafC, leafD, leafE)

			// NOTE: When leaf A starts with 4 elements, and test deletes 12
			// from it, it will no longer have enough elements, and its
			// remaining elements will be moved to other nodes. However, the
			// internal node at the top will still have enough elements, and
			// its return value will be true.
			bigEnough := internal.deleteKey(4, 12)
			if got, want := bigEnough, true; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}

			// Leaf A will adopt all elements from leaf B, and its Next field
			// will point to leaf C.
			ensureLeafNodesMatch(t, leafA, newLeafFrom(leafC, 10, 14, 16, 20, 22, 24, 26))

			// Leaf B will have no remaining elements.
			if got, want := len(leafB.Runts), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := len(leafB.Values), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}

			// The other leaf nodes should be untouched.
			ensureLeafNodesMatch(t, leafC, newLeafFrom(leafD, 30, 32, 34, 36, 38))
			ensureLeafNodesMatch(t, leafD, newLeafFrom(leafE, 40, 42, 44, 46, 48))
			ensureLeafNodesMatch(t, leafE, newLeafFrom(nil, 50, 52, 54, 56, 58))
		})
		t.Run("internal node too small", func(t *testing.T) {
			leafD := newLeafFrom(nil, 40, 42, 44, 46, 48)
			leafC := newLeafFrom(leafD, 30, 32, 34, 36, 38)
			leafB := newLeafFrom(leafC, 20, 22, 24, 26)
			leafA := newLeafFrom(leafB, 10, 12, 14, 16)

			child := newInternalFrom(leafA, leafB, leafC, leafD)

			bigEnough := child.deleteKey(4, 12)
			if got, want := bigEnough, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}

			ensureLeafNodesMatch(t, leafA, newLeafFrom(leafC, 10, 14, 16, 20, 22, 24, 26))
			if got, want := len(leafB.Runts), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := len(leafB.Values), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureLeafNodesMatch(t, leafC, newLeafFrom(leafD, 30, 32, 34, 36, 38))
			ensureLeafNodesMatch(t, leafD, newLeafFrom(nil, 40, 42, 44, 46, 48))
		})
	})
	t.Run("child adopts from right when no left and fat right", func(t *testing.T) {
		leafE := newLeafFrom(nil, 50, 52, 54, 56, 58)
		leafD := newLeafFrom(leafE, 40, 42, 44, 46, 48)
		leafC := newLeafFrom(leafD, 30, 32, 34, 36, 38)
		leafB := newLeafFrom(leafC, 20, 22, 24, 26, 28)
		leafA := newLeafFrom(leafB, 10, 12, 14, 16)

		child := newInternalFrom(leafA, leafB, leafC, leafD, leafE)

		bigEnough := child.deleteKey(4, 12)
		if got, want := bigEnough, true; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}

		ensureLeafNodesMatch(t, leafA, newLeafFrom(leafB, 10, 14, 16, 20))
		ensureLeafNodesMatch(t, leafB, newLeafFrom(leafC, 22, 24, 26, 28))
		ensureLeafNodesMatch(t, leafC, newLeafFrom(leafD, 30, 32, 34, 36, 38))
		ensureLeafNodesMatch(t, leafD, newLeafFrom(leafE, 40, 42, 44, 46, 48))
		ensureLeafNodesMatch(t, leafE, newLeafFrom(nil, 50, 52, 54, 56, 58))
	})
	t.Run("left absorbs child when skinny left and no right", func(t *testing.T) {
		t.Run("too small", func(t *testing.T) {
			leafD := newLeafFrom(nil, 40, 42, 44, 46)
			leafC := newLeafFrom(leafD, 30, 32, 34, 36)
			leafB := newLeafFrom(leafC, 20, 22, 24, 26)
			leafA := newLeafFrom(leafB, 10, 12, 14, 16)

			child := newInternalFrom(leafA, leafB, leafC, leafD)

			bigEnough := child.deleteKey(4, 42)
			if got, want := bigEnough, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}

			ensureLeafNodesMatch(t, leafA, newLeafFrom(leafB, 10, 12, 14, 16))
			ensureLeafNodesMatch(t, leafB, newLeafFrom(leafC, 20, 22, 24, 26))
			ensureLeafNodesMatch(t, leafC, newLeafFrom(nil, 30, 32, 34, 36, 40, 44, 46))
			if got, want := len(leafD.Runts), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := len(leafD.Values), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
		})
		t.Run("not too small", func(t *testing.T) {
			leafE := newLeafFrom(nil, 50, 52, 54, 56)
			leafD := newLeafFrom(leafE, 40, 42, 44, 46)
			leafC := newLeafFrom(leafD, 30, 32, 34, 36)
			leafB := newLeafFrom(leafC, 20, 22, 24, 26)
			leafA := newLeafFrom(leafB, 10, 12, 14, 16)

			child := newInternalFrom(leafA, leafB, leafC, leafD, leafE)

			bigEnough := child.deleteKey(4, 52)
			if got, want := bigEnough, true; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}

			ensureLeafNodesMatch(t, leafA, newLeafFrom(leafB, 10, 12, 14, 16))
			ensureLeafNodesMatch(t, leafB, newLeafFrom(leafC, 20, 22, 24, 26))
			ensureLeafNodesMatch(t, leafC, newLeafFrom(leafD, 30, 32, 34, 36))
			ensureLeafNodesMatch(t, leafD, newLeafFrom(nil, 40, 42, 44, 46, 50, 54, 56))
			if got, want := len(leafE.Runts), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := len(leafE.Values), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
		})
	})
	t.Run("left absorbs child when skinny left and skinny right", func(t *testing.T) {
		t.Run("too small", func(t *testing.T) {
			leafC := newLeafFrom(nil, 30, 32, 34, 36)
			leafB := newLeafFrom(leafC, 20, 22, 24, 26)
			leafA := newLeafFrom(leafB, 10, 12, 14, 16)

			child := newInternalFrom(leafA, leafB, leafC)

			bigEnough := child.deleteKey(4, 22)
			if got, want := bigEnough, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}

			ensureLeafNodesMatch(t, leafA, newLeafFrom(leafC, 10, 12, 14, 16, 20, 24, 26))
			if got, want := len(leafB.Runts), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := len(leafB.Values), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureLeafNodesMatch(t, leafC, newLeafFrom(nil, 30, 32, 34, 36))
		})
		t.Run("not too small", func(t *testing.T) {
			leafE := newLeafFrom(nil, 50, 52, 54, 56)
			leafD := newLeafFrom(leafE, 40, 42, 44, 46)
			leafC := newLeafFrom(leafD, 30, 32, 34, 36)
			leafB := newLeafFrom(leafC, 20, 22, 24, 26)
			leafA := newLeafFrom(leafB, 10, 12, 14, 16)

			child := newInternalFrom(leafA, leafB, leafC, leafD, leafE)

			bigEnough := child.deleteKey(4, 22)
			if got, want := bigEnough, true; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}

			ensureLeafNodesMatch(t, leafA, newLeafFrom(leafC, 10, 12, 14, 16, 20, 24, 26))
			if got, want := len(leafB.Runts), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := len(leafB.Values), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureLeafNodesMatch(t, leafC, newLeafFrom(leafD, 30, 32, 34, 36))
			ensureLeafNodesMatch(t, leafD, newLeafFrom(leafE, 40, 42, 44, 46))
		})
	})
	t.Run("child adopts from right when skinny left and fat right", func(t *testing.T) {
		leafE := newLeafFrom(nil, 50, 52, 54, 56, 58)
		leafD := newLeafFrom(leafE, 40, 42, 44, 46, 48)
		leafC := newLeafFrom(leafD, 30, 32, 34, 36, 38)
		leafB := newLeafFrom(leafC, 20, 22, 24, 26)
		leafA := newLeafFrom(leafB, 10, 12, 14, 16)

		child := newInternalFrom(leafA, leafB, leafC, leafD, leafE)

		bigEnough := child.deleteKey(4, 22)
		if got, want := bigEnough, true; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}

		ensureLeafNodesMatch(t, leafA, newLeafFrom(leafB, 10, 12, 14, 16))
		ensureLeafNodesMatch(t, leafB, newLeafFrom(leafC, 20, 24, 26, 30))
		ensureLeafNodesMatch(t, leafC, newLeafFrom(leafD, 32, 34, 36, 38))
		ensureLeafNodesMatch(t, leafD, newLeafFrom(leafE, 40, 42, 44, 46, 48))
		ensureLeafNodesMatch(t, leafE, newLeafFrom(nil, 50, 52, 54, 56, 58))
	})
	t.Run("child adopts from left when fat left and no right", func(t *testing.T) {
		leafE := newLeafFrom(nil, 50, 52, 54, 56)
		leafD := newLeafFrom(leafE, 40, 42, 44, 46, 48)
		leafC := newLeafFrom(leafD, 30, 32, 34, 36)
		leafB := newLeafFrom(leafC, 20, 22, 24, 26)
		leafA := newLeafFrom(leafB, 10, 12, 14, 16)

		child := newInternalFrom(leafA, leafB, leafC, leafD, leafE)

		bigEnough := child.deleteKey(4, 52)
		if got, want := bigEnough, true; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}

		ensureLeafNodesMatch(t, leafA, newLeafFrom(leafB, 10, 12, 14, 16))
		ensureLeafNodesMatch(t, leafB, newLeafFrom(leafC, 20, 22, 24, 26))
		ensureLeafNodesMatch(t, leafC, newLeafFrom(leafD, 30, 32, 34, 36))
		ensureLeafNodesMatch(t, leafD, newLeafFrom(leafE, 40, 42, 44, 46))
		ensureLeafNodesMatch(t, leafE, newLeafFrom(nil, 48, 50, 54, 56))
	})
	t.Run("child adopts from left when fat left and skinny right", func(t *testing.T) {
		leafE := newLeafFrom(nil, 50, 52, 54, 56)
		leafD := newLeafFrom(leafE, 40, 42, 44, 46)
		leafC := newLeafFrom(leafD, 30, 32, 34, 36)
		leafB := newLeafFrom(leafC, 20, 22, 24, 26, 28)
		leafA := newLeafFrom(leafB, 10, 12, 14, 16)

		child := newInternalFrom(leafA, leafB, leafC, leafD, leafE)

		bigEnough := child.deleteKey(4, 32)
		if got, want := bigEnough, true; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}

		ensureLeafNodesMatch(t, leafA, newLeafFrom(leafB, 10, 12, 14, 16))
		ensureLeafNodesMatch(t, leafB, newLeafFrom(leafC, 20, 22, 24, 26))
		ensureLeafNodesMatch(t, leafC, newLeafFrom(leafD, 28, 30, 34, 36))
		ensureLeafNodesMatch(t, leafD, newLeafFrom(leafE, 40, 42, 44, 46))
		ensureLeafNodesMatch(t, leafE, newLeafFrom(nil, 50, 52, 54, 56))
	})
	t.Run("child adopts from right when fat left and fat right", func(t *testing.T) {
		leafE := newLeafFrom(nil, 50, 52, 54, 56, 58)
		leafD := newLeafFrom(leafE, 40, 42, 44, 46, 48)
		leafC := newLeafFrom(leafD, 30, 32, 34, 36)
		leafB := newLeafFrom(leafC, 20, 22, 24, 26, 28)
		leafA := newLeafFrom(leafB, 10, 12, 14, 16, 18)

		child := newInternalFrom(leafA, leafB, leafC, leafD, leafE)

		bigEnough := child.deleteKey(4, 32)
		if got, want := bigEnough, true; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}

		ensureLeafNodesMatch(t, leafA, newLeafFrom(leafB, 10, 12, 14, 16, 18))
		ensureLeafNodesMatch(t, leafB, newLeafFrom(leafC, 20, 22, 24, 26, 28))
		ensureLeafNodesMatch(t, leafC, newLeafFrom(leafD, 30, 34, 36, 40))
		ensureLeafNodesMatch(t, leafD, newLeafFrom(leafE, 42, 44, 46, 48))
		ensureLeafNodesMatch(t, leafE, newLeafFrom(nil, 50, 52, 54, 56, 58))
	})
}

func TestGenericDelete(t *testing.T) {
	const order = 32

	d, err := NewGenericTree[int](order)
	if err != nil {
		t.Fatal(err)
	}

	for _, v := range randomizedValues {
		d.Insert(v, v)
	}

	for _, v := range randomizedValues {
		if _, ok := d.Search(v); !ok {
			t.Fatalf("GOT: %v; WANT: %v", ok, true)
		}
	}

	for _, v := range randomizedValues {
		d.Delete(v)
	}

	t.Run("empty", func(t *testing.T) {
		d.Delete(13)
	})
}
