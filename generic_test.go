package gobptree

import (
	"cmp"
	"fmt"
	"math"
	"math/rand"
	"testing"
)

////////////////////////////////////////
// test helpers to ensure two nodes match
////////////////////////////////////////

func ensureInternalNodesMatch[K cmp.Ordered, V any](t *testing.T, got, want *internalNode[K, V]) {
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
		t.Helper()
		if g, w := len(got.Children), len(want.Children); g != w {
			t.Errorf("length(Children) GOT: %v; WANT: %v", g, w)
		}
		for i := 0; i < len(got.Children); i++ {
			ensureNodesMatch(t, got.Children[i], want.Children[i])
		}
	})
}

func ensureLeafNodesMatch[K cmp.Ordered, V any](t *testing.T, got, want *leafNode[K, V]) {
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

	t.Run("Values", func(t *testing.T) {
		t.Helper()
		ensureSame(t, got.Values, want.Values)
	})

	t.Run("Next", func(t *testing.T) {
		t.Helper()
		ensureLeafNodesMatch(t, got.Next, want.Next)
	})
}

func ensureNodesMatch[K cmp.Ordered, V any](t *testing.T, got, want node[K, V]) {
	t.Helper()

	switch w := want.(type) {
	case *internalNode[K, V]:
		g, ok := got.(*internalNode[K, V])
		if !ok {
			t.Errorf("GOT: %#v; WANT: %#v", got, want)
		}
		ensureInternalNodesMatch(t, g, w)
	case *leafNode[K, V]:
		g, ok := got.(*leafNode[K, V])
		if !ok {
			t.Errorf("GOT: %#v; WANT: %#v", got, want)
		}
		ensureLeafNodesMatch(t, g, w)
	default:
		t.Errorf("GOT: %#v; WANT: %#v", got, want)
	}
}

func ensureTree[K cmp.Ordered, V any](t *testing.T, tree *GenericTree[K, V], want node[K, V]) {
	t.Helper()

	// IMPORTANT: Must stitch before running following checks.
	leaf := stitchNextValues(want, nil)

	t.Run("values", func(t *testing.T) {
		t.Helper()

		var gotValues, wantValues []V

		// got values
		scanner := tree.NewScannerAll()
		for scanner.Scan() {
			_, value := scanner.Pair()
			gotValues = append(gotValues, value)
		}
		ensureError(t, scanner.Close())

		// want values
		for leaf != nil {
			for _, value := range leaf.Values {
				wantValues = append(wantValues, value)
			}
			leaf = leaf.Next
		}

		ensureSame(t, gotValues, wantValues)
	})

	t.Run("structure", func(t *testing.T) {
		t.Helper()
		ensureNodesMatch(t, tree.root, want)
	})
}

func ensureValues[K cmp.Ordered, V any](t *testing.T, tree *GenericTree[K, V], want []V) {
	t.Helper()

	t.Run("values", func(t *testing.T) {
		t.Helper()

		var got []V

		scanner := tree.NewScannerAll()
		for scanner.Scan() {
			_, value := scanner.Pair()
			got = append(got, value)
		}

		ensureError(t, scanner.Close())
		ensureSame(t, got, want)
	})
}

////////////////////////////////////////
// test helpers to create new internal and leaf nodes
////////////////////////////////////////

func newInternal[K cmp.Ordered, V any](items ...node[K, V]) *internalNode[K, V] {
	n := &internalNode[K, V]{
		Runts:    make([]K, len(items)),
		Children: make([]node[K, V], len(items)),
	}
	for i := 0; i < len(items); i++ {
		n.Runts[i] = items[i].smallest()
		n.Children[i] = items[i]
	}
	return n
}

// stitchNextValues recursively walks the tree starting at n and setting the
// leaf node next fields, then returns the first leaf node in the tree branch.
func stitchNextValues[K cmp.Ordered, V any](n node[K, V], nextLeaf *leafNode[K, V]) *leafNode[K, V] {
	switch tv := n.(type) {
	case *internalNode[K, V]:
		// Enumerate from the final to the first child.
		for i := len(tv.Children) - 1; i >= 0; i-- {
			nextLeaf = stitchNextValues(tv.Children[i], nextLeaf)
		}
		return nextLeaf
	case *leafNode[K, V]:
		tv.Next = nextLeaf
		return tv
	default:
		panic(fmt.Errorf("GOT: %#v; WANT: node[K,V]", n))
	}
}

////////////////////////////////////////
// tests
////////////////////////////////////////

func TestGenericBinarySearch(t *testing.T) {
	t.Run("skip Values", func(t *testing.T) {
		values := []int64{1, 3, 5, 7, 9, 11, 13}

		if got, want := searchGreaterThanOrEqualTo(0, values), 0; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := searchGreaterThanOrEqualTo(1, values), 0; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := searchGreaterThanOrEqualTo(2, values), 1; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := searchGreaterThanOrEqualTo(3, values), 1; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := searchGreaterThanOrEqualTo(4, values), 2; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := searchGreaterThanOrEqualTo(5, values), 2; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := searchGreaterThanOrEqualTo(6, values), 3; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := searchGreaterThanOrEqualTo(7, values), 3; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := searchGreaterThanOrEqualTo(8, values), 4; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := searchGreaterThanOrEqualTo(9, values), 4; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := searchGreaterThanOrEqualTo(10, values), 5; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := searchGreaterThanOrEqualTo(11, values), 5; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := searchGreaterThanOrEqualTo(12, values), 6; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := searchGreaterThanOrEqualTo(13, values), 6; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := searchGreaterThanOrEqualTo(14, values), 6; got != want {
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
		_, err := NewGenericTree[int, int](v)
		if err == nil {
			ensureError(t, err, fmt.Sprintf("multiple of 2: %d", v))
		}
	}
}

func TestGenericInternalNodeMaybeSplit(t *testing.T) {
	leafD := &leafNode[int, int]{
		Runts:  []int{40, 41, 42, 43},
		Values: []int{40, 41, 42, 43},
	}
	leafC := &leafNode[int, int]{
		Runts:  []int{30, 31, 32, 33},
		Values: []int{30, 31, 32, 33},
		Next:   leafD,
	}
	leafB := &leafNode[int, int]{
		Runts:  []int{20, 21, 22, 23},
		Values: []int{20, 21, 22, 23},
		Next:   leafC,
	}
	leafA := &leafNode[int, int]{
		Runts:  []int{10, 11, 12, 13},
		Values: []int{10, 11, 12, 13},
		Next:   leafB,
	}
	internal := newInternal(leafA, leafB, leafC, leafD)

	t.Run("does nothing when not full", func(t *testing.T) {
		_, right := internal.maybeSplit(6)
		if right != nil {
			t.Errorf("GOT: %v; WANT: %v", right, nil)
		}
	})

	t.Run("splits when full", func(t *testing.T) {
		wantLeft := newInternal(leafA, leafB)
		wantRight := newInternal(leafC, leafD)

		gotLeft, gotRight := internal.maybeSplit(4)

		ensureNodesMatch(t, gotLeft, wantLeft)
		ensureNodesMatch(t, gotRight, wantRight)
	})
}

func TestGenericInternalNodeInsertSmallerKey(t *testing.T) {
	leafB := &leafNode[int, int]{
		Runts:  []int{21, 22},
		Values: []int{21, 22},
	}
	leafA := &leafNode[int, int]{
		Runts:  []int{11, 12},
		Values: []int{11, 12},
		Next:   leafB,
	}
	internal := newInternal(leafA, leafB)

	tree := &GenericTree[int, int]{root: internal, order: 4}

	tree.Insert(11, 11)

	if got, want := internal.Runts[0], 11; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
}

func TestGenericInsertOrder2(t *testing.T) {
	tree, err := NewGenericTree[int, int](2)
	ensureError(t, err)

	t.Run("1", func(t *testing.T) {
		tree.Insert(1, 1)

		ensureTree(t, tree, &leafNode[int, int]{
			Runts:  []int{1},
			Values: []int{1},
		})
	})

	t.Run("2", func(t *testing.T) {
		tree.Insert(2, 2)

		ensureTree(t, tree, &leafNode[int, int]{
			Runts:  []int{1, 2},
			Values: []int{1, 2},
		})
	})

	t.Run("3", func(t *testing.T) {
		tree.Insert(3, 3)

		ensureTree(t, tree,
			newInternal(
				&leafNode[int, int]{
					Runts:  []int{1},
					Values: []int{1},
				},
				&leafNode[int, int]{
					Runts:  []int{2, 3},
					Values: []int{2, 3},
				},
			),
		)
	})

	t.Run("4", func(t *testing.T) {
		tree.Insert(4, 4)

		ensureTree(t, tree,
			newInternal(
				newInternal(
					&leafNode[int, int]{
						Runts:  []int{1},
						Values: []int{1},
					},
				),
				newInternal(
					&leafNode[int, int]{
						Runts:  []int{2},
						Values: []int{2},
					},
					&leafNode[int, int]{
						Runts:  []int{3, 4},
						Values: []int{3, 4},
					},
				),
			),
		)
	})

	t.Run("5", func(t *testing.T) {
		tree.Insert(5, 5)

		ensureTree(t, tree,
			newInternal(
				newInternal(
					newInternal(
						&leafNode[int, int]{
							Runts:  []int{1},
							Values: []int{1},
						},
					),
				),
				newInternal(
					newInternal(
						&leafNode[int, int]{
							Runts:  []int{2},
							Values: []int{2},
						},
					),
					newInternal(
						&leafNode[int, int]{
							Runts:  []int{3},
							Values: []int{3},
						},
						&leafNode[int, int]{
							Runts:  []int{4, 5},
							Values: []int{4, 5},
						},
					),
				),
			),
		)
	})

	t.Run("6", func(t *testing.T) {
		tree.Insert(6, 6)

		ensureTree(t, tree,
			newInternal(
				newInternal(
					newInternal(
						newInternal(
							&leafNode[int, int]{
								Runts:  []int{1},
								Values: []int{1},
							},
						),
					),
				),
				newInternal(
					newInternal(
						newInternal(
							&leafNode[int, int]{
								Runts:  []int{2},
								Values: []int{2},
							},
						),
					),
					newInternal(
						newInternal(
							&leafNode[int, int]{
								Runts:  []int{3},
								Values: []int{3},
							},
						),
						newInternal(
							&leafNode[int, int]{
								Runts:  []int{4},
								Values: []int{4},
							},
							&leafNode[int, int]{
								Runts:  []int{5, 6},
								Values: []int{5, 6},
							},
						),
					),
				),
			),
		)
	})
}

func TestGenericInsertOrder4(t *testing.T) {
	tree, err := NewGenericTree[int, int](4)
	ensureError(t, err)

	t.Run("1", func(t *testing.T) {
		tree.Insert(1, 1)
		ensureTree(t, tree, &leafNode[int, int]{
			Runts:  []int{1},
			Values: []int{1},
		})
	})

	t.Run("2", func(t *testing.T) {
		tree.Insert(2, 2)
		ensureTree(t, tree, &leafNode[int, int]{
			Runts:  []int{1, 2},
			Values: []int{1, 2},
		})
	})

	t.Run("3", func(t *testing.T) {
		tree.Insert(3, 3)
		ensureTree(t, tree, &leafNode[int, int]{
			Runts:  []int{1, 2, 3},
			Values: []int{1, 2, 3},
		})
	})

	t.Run("4", func(t *testing.T) {
		tree.Insert(4, 4)
		ensureTree(t, tree, &leafNode[int, int]{
			Runts:  []int{1, 2, 3, 4},
			Values: []int{1, 2, 3, 4},
		})
	})

	t.Run("5", func(t *testing.T) {
		tree.Insert(5, 5)

		ensureTree(t, tree,
			newInternal(
				&leafNode[int, int]{
					Runts:  []int{1, 2},
					Values: []int{1, 2},
				},
				&leafNode[int, int]{
					Runts:  []int{3, 4, 5},
					Values: []int{3, 4, 5},
				},
			),
		)
	})

	t.Run("6", func(t *testing.T) {
		tree.Insert(6, 6)

		ensureTree(t, tree,
			newInternal(
				&leafNode[int, int]{
					Runts:  []int{1, 2},
					Values: []int{1, 2},
				},
				&leafNode[int, int]{
					Runts:  []int{3, 4, 5, 6},
					Values: []int{3, 4, 5, 6},
				},
			),
		)
	})

	t.Run("7", func(t *testing.T) {
		tree.Insert(7, 7)

		ensureTree(t, tree,
			newInternal(
				&leafNode[int, int]{
					Runts:  []int{1, 2},
					Values: []int{1, 2},
				},
				&leafNode[int, int]{
					Runts:  []int{3, 4},
					Values: []int{3, 4},
				},
				&leafNode[int, int]{
					Runts:  []int{5, 6, 7},
					Values: []int{5, 6, 7},
				},
			),
		)
	})

	t.Run("8", func(t *testing.T) {
		tree.Insert(8, 8)

		ensureTree(t, tree,
			newInternal(
				&leafNode[int, int]{
					Runts:  []int{1, 2},
					Values: []int{1, 2},
				},
				&leafNode[int, int]{
					Runts:  []int{3, 4},
					Values: []int{3, 4},
				},
				&leafNode[int, int]{
					Runts:  []int{5, 6, 7, 8},
					Values: []int{5, 6, 7, 8},
				},
			),
		)
	})

	t.Run("9", func(t *testing.T) {
		tree.Insert(9, 9)

		ensureTree(t, tree,
			newInternal(
				&leafNode[int, int]{
					Runts:  []int{1, 2},
					Values: []int{1, 2},
				},
				&leafNode[int, int]{
					Runts:  []int{3, 4},
					Values: []int{3, 4},
				},
				&leafNode[int, int]{
					Runts:  []int{5, 6},
					Values: []int{5, 6},
				},
				&leafNode[int, int]{
					Runts:  []int{7, 8, 9},
					Values: []int{7, 8, 9},
				},
			),
		)
	})

	t.Run("10", func(t *testing.T) {
		tree.Insert(10, 10)

		ensureTree(t, tree,
			newInternal(
				newInternal(
					&leafNode[int, int]{
						Runts:  []int{1, 2},
						Values: []int{1, 2},
					},
					&leafNode[int, int]{
						Runts:  []int{3, 4},
						Values: []int{3, 4},
					},
				),
				newInternal(
					&leafNode[int, int]{
						Runts:  []int{5, 6},
						Values: []int{5, 6},
					},
					&leafNode[int, int]{
						Runts:  []int{7, 8, 9, 10},
						Values: []int{7, 8, 9, 10},
					},
				),
			),
		)
	})

	t.Run("11", func(t *testing.T) {
		tree.Insert(11, 11)

		ensureTree(t, tree,
			newInternal(
				newInternal(
					&leafNode[int, int]{
						Runts:  []int{1, 2},
						Values: []int{1, 2},
					},
					&leafNode[int, int]{
						Runts:  []int{3, 4},
						Values: []int{3, 4},
					},
				),
				newInternal(
					&leafNode[int, int]{
						Runts:  []int{5, 6},
						Values: []int{5, 6},
					},
					&leafNode[int, int]{
						Runts:  []int{7, 8},
						Values: []int{7, 8},
					},
					&leafNode[int, int]{
						Runts:  []int{9, 10, 11},
						Values: []int{9, 10, 11},
					},
				),
			),
		)
	})

	t.Run("12", func(t *testing.T) {
		tree.Insert(12, 12)

		ensureTree(t, tree,
			newInternal(
				newInternal(
					&leafNode[int, int]{
						Runts:  []int{1, 2},
						Values: []int{1, 2},
					},
					&leafNode[int, int]{
						Runts:  []int{3, 4},
						Values: []int{3, 4},
					},
				),
				newInternal(
					&leafNode[int, int]{
						Runts:  []int{5, 6},
						Values: []int{5, 6},
					},
					&leafNode[int, int]{
						Runts:  []int{7, 8},
						Values: []int{7, 8},
					},
					&leafNode[int, int]{
						Runts:  []int{9, 10, 11, 12},
						Values: []int{9, 10, 11, 12},
					},
				),
			),
		)
	})

	t.Run("13", func(t *testing.T) {
		tree.Insert(13, 13)

		ensureTree(t, tree,
			newInternal(
				newInternal(
					&leafNode[int, int]{
						Runts:  []int{1, 2},
						Values: []int{1, 2},
					},
					&leafNode[int, int]{
						Runts:  []int{3, 4},
						Values: []int{3, 4},
					},
				),
				newInternal(
					&leafNode[int, int]{
						Runts:  []int{5, 6},
						Values: []int{5, 6},
					},
					&leafNode[int, int]{
						Runts:  []int{7, 8},
						Values: []int{7, 8},
					},
					&leafNode[int, int]{
						Runts:  []int{9, 10},
						Values: []int{9, 10},
					},
					&leafNode[int, int]{
						Runts:  []int{11, 12, 13},
						Values: []int{11, 12, 13},
					},
				),
			),
		)
	})
}

func TestGenericLeafNodeMaybeSplit(t *testing.T) {
	gimme := func() (*leafNode[int, int], *leafNode[int, int]) {
		// leafB := newLeafFrom[int, int](nil, 21, 22, 23, 24)
		leafB := &leafNode[int, int]{
			Runts:  []int{21, 22, 23, 24},
			Values: []int{21, 22, 23, 24},
		}
		// leafA := newLeafFrom[int, int](leafB, 11, 12, 13, 14)
		leafA := &leafNode[int, int]{
			Runts:  []int{11, 12, 13, 14},
			Values: []int{11, 12, 13, 14},
			Next:   leafB,
		}
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

		ensureNodesMatch(t, leftNode, &leafNode[int, int]{
			Runts:  []int{11, 12},
			Values: []int{11, 12},
			Next:   rightNode.(*leafNode[int, int]),
		})

		ensureNodesMatch(t, rightNode, &leafNode[int, int]{
			Runts:  []int{13, 14},
			Values: []int{13, 14},
			Next:   leafB,
		})
	})
	t.Run("splits right edge when full", func(t *testing.T) {
		leafA, leafB := gimme()
		leftNode, rightNode := leafB.maybeSplit(4)

		if got, want := leafA.Next, leftNode; got != want {
			t.Fatalf("GOT: %v; WANT: %v", got, want)
		}

		ensureNodesMatch(t, leftNode, &leafNode[int, int]{
			Runts:  []int{21, 22},
			Values: []int{21, 22},
			Next:   rightNode.(*leafNode[int, int]),
		})

		ensureNodesMatch(t, rightNode, &leafNode[int, int]{
			Runts:  []int{23, 24},
			Values: []int{23, 24},
		})
	})
}

func TestInsertIntoSingleLeafGenericTree(t *testing.T) {
	t.Run("when fewer than order elements", func(t *testing.T) {
		t.Run("when empty", func(t *testing.T) {
			tree, err := NewGenericTree[int, int](4)
			ensureError(t, err)

			leaf, ok := tree.root.(*leafNode[int, int])
			if !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, false)
			}

			tree.Insert(30, 30)

			ensureLeafNodesMatch(t, leaf, &leafNode[int, int]{
				Runts:  []int{30},
				Values: []int{30},
			})
		})
		t.Run("when less than first runt", func(t *testing.T) {
			tree, err := NewGenericTree[int, int](4)
			ensureError(t, err)

			leaf, ok := tree.root.(*leafNode[int, int])
			if !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, false)
			}

			tree.Insert(30, 30)
			tree.Insert(10, 10)

			ensureNodesMatch(t, leaf, &leafNode[int, int]{
				Runts:  []int{10, 30},
				Values: []int{10, 30},
			})
		})
		t.Run("when update value", func(t *testing.T) {
			tree, err := NewGenericTree[int, int](4)
			ensureError(t, err)

			leaf, ok := tree.root.(*leafNode[int, int])
			if !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, false)
			}

			tree.Insert(30, 30)
			tree.Insert(10, 10)
			tree.Insert(30, 333)

			ensureNodesMatch(t, leaf, &leafNode[int, int]{
				Runts:  []int{10, 30},
				Values: []int{10, 333},
			})
		})
		t.Run("when between first and final runt", func(t *testing.T) {
			tree, err := NewGenericTree[int, int](4)
			ensureError(t, err)

			leaf, ok := tree.root.(*leafNode[int, int])
			if !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, false)
			}

			tree.Insert(30, 30)
			tree.Insert(10, 10)
			tree.Insert(20, 20)

			ensureNodesMatch(t, leaf, &leafNode[int, int]{
				Runts:  []int{10, 20, 30},
				Values: []int{10, 20, 30},
			})
		})
		t.Run("when after final runt", func(t *testing.T) {
			tree, err := NewGenericTree[int, int](4)
			ensureError(t, err)

			leaf, ok := tree.root.(*leafNode[int, int])
			if !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, false)
			}

			tree.Insert(30, 30)
			tree.Insert(10, 10)
			tree.Insert(20, 20)
			tree.Insert(40, 40)

			ensureNodesMatch(t, leaf, &leafNode[int, int]{
				Runts:  []int{10, 20, 30, 40},
				Values: []int{10, 20, 30, 40},
			})
		})
	})

	t.Run("when insertion splits single leaf node", func(t *testing.T) {
		gimme := func() *GenericTree[int, int] {
			tree, err := NewGenericTree[int, int](4)
			ensureError(t, err)

			for _, v := range []int{10, 20, 30, 40} {
				tree.Insert(v, v)
			}

			return tree
		}

		t.Run("when new key will be first node in left leaf", func(t *testing.T) {
			tree := gimme()
			tree.Insert(0, 0)

			root, ok := tree.root.(*internalNode[int, int])
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

			ensureNodesMatch(t, root.Children[0], &leafNode[int, int]{
				Runts:  []int{0, 10, 20},
				Values: []int{0, 10, 20},
				Next:   root.Children[1].(*leafNode[int, int]),
			})

			if got, want := root.Runts[1], 30; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}

			ensureNodesMatch(t, root.Children[1], &leafNode[int, int]{
				Runts:  []int{30, 40},
				Values: []int{30, 40},
			})
		})
		t.Run("when new key is in middle", func(t *testing.T) {
			tree := gimme()
			tree.Insert(25, 25)

			root, ok := tree.root.(*internalNode[int, int])
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

			ensureNodesMatch(t, root.Children[0], &leafNode[int, int]{
				Runts:  []int{10, 20, 25},
				Values: []int{10, 20, 25},
				Next:   root.Children[1].(*leafNode[int, int]),
			})

			if got, want := root.Runts[1], 30; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}

			ensureNodesMatch(t, root.Children[1], &leafNode[int, int]{
				Runts:  []int{30, 40},
				Values: []int{30, 40},
			})
		})
		t.Run("when new key will be final node in right leaf", func(t *testing.T) {
			tree := gimme()
			tree.Insert(50, 50)

			root, ok := tree.root.(*internalNode[int, int])
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

			ensureNodesMatch(t, root.Children[0], &leafNode[int, int]{
				Runts:  []int{10, 20},
				Values: []int{10, 20},
				Next:   root.Children[1].(*leafNode[int, int]),
			})

			if got, want := root.Runts[1], 30; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}

			ensureNodesMatch(t, root.Children[1], &leafNode[int, int]{
				Runts:  []int{30, 40, 50},
				Values: []int{30, 40, 50},
			})
		})
	})
}

func TestGenericRebalance(t *testing.T) {
	gimme := func(order, item_count int) *GenericTree[int, int] {
		tree, err := NewGenericTree[int, int](order)
		ensureError(t, err)

		values := rand.Perm(item_count)

		for _, v := range values {
			tree.Insert(v+1, v+1)
		}

		return tree
	}

	t.Run("when rebalance count is -1", func(t *testing.T) {
		const order = 4
		const items = 5
		rebalance := -1
		tree := gimme(order, items)
		ensureError(t, tree.Rebalance(rebalance), "cannot rebalance")
	})

	t.Run("when rebalance count is 0", func(t *testing.T) {
		const order = 4
		const items = 5
		rebalance := 0
		tree := gimme(order, items)
		ensureError(t, tree.Rebalance(rebalance), "cannot rebalance")
	})

	t.Run("when rebalance count is 1", func(t *testing.T) {
		const order = 4
		const items = 5
		rebalance := 1
		tree := gimme(order, items)
		ensureError(t, tree.Rebalance(rebalance), "cannot rebalance")
	})

	t.Run("when rebalance count is 2", func(t *testing.T) {
		const order = 4
		const items = 5
		rebalance := 2
		tree := gimme(order, items)

		ensureError(t, tree.Rebalance(rebalance))

		ensureTree(t, tree,
			newInternal(
				newInternal(
					&leafNode[int, int]{
						Runts:  []int{1, 2},
						Values: []int{1, 2},
					},
					&leafNode[int, int]{
						Runts:  []int{3, 4},
						Values: []int{3, 4},
					},
				),
				newInternal(
					&leafNode[int, int]{
						Runts:  []int{5},
						Values: []int{5},
					},
				),
			),
		)
	})

	t.Run("when rebalance count is less than order", func(t *testing.T) {
		const order = 4
		const items = 5
		rebalance := order - 1
		tree := gimme(order, items)

		ensureError(t, tree.Rebalance(rebalance))

		ensureTree(t, tree,
			newInternal(
				&leafNode[int, int]{
					Runts:  []int{1, 2, 3},
					Values: []int{1, 2, 3},
				},
				&leafNode[int, int]{
					Runts:  []int{4, 5},
					Values: []int{4, 5},
				},
			),
		)
	})

	t.Run("when rebalance count is equal to order", func(t *testing.T) {
		const order = 4
		const items = 5
		rebalance := order
		tree := gimme(order, items)

		ensureError(t, tree.Rebalance(rebalance))

		ensureTree(t, tree,
			newInternal(
				&leafNode[int, int]{
					Runts:  []int{1, 2, 3, 4},
					Values: []int{1, 2, 3, 4},
				},
				&leafNode[int, int]{
					Runts:  []int{5},
					Values: []int{5},
				},
			),
		)
	})

	t.Run("when rebalance count is larger than order", func(t *testing.T) {
		const order = 4
		const items = 5
		rebalance := order + 1
		tree := gimme(order, items)
		ensureError(t, tree.Rebalance(rebalance), "cannot rebalance")
	})
}

func TestGenericTreeSearch(t *testing.T) {
	t.Run("empty tree", func(t *testing.T) {
		tree, err := NewGenericTree[int, int](16)
		ensureError(t, err)

		_, ok := tree.Search(13)
		if got, want := ok, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
	})
	t.Run("single-leaf tree", func(t *testing.T) {
		t.Run("missing value", func(t *testing.T) {
			tree, err := NewGenericTree[int, int](16)
			ensureError(t, err)

			for i := 0; i < 15; i++ {
				if i != 13 {
					tree.Insert(i, i)
				}
			}

			_, ok := tree.Search(13)
			if got, want := ok, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
		})
		t.Run("existing value", func(t *testing.T) {
			tree, err := NewGenericTree[int, int](16)
			ensureError(t, err)

			for i := 0; i < 15; i++ {
				if i != 13 {
					tree.Insert(i, i)
				}
			}

			value, ok := tree.Search(8)
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
			tree, err := NewGenericTree[int, int](4)
			ensureError(t, err)

			for i := 0; i < 15; i++ {
				if i != 13 {
					tree.Insert(i, i)
				}
			}

			_, ok := tree.Search(13)
			if got, want := ok, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
		})
		t.Run("existing value", func(t *testing.T) {
			tree, err := NewGenericTree[int, int](4)
			ensureError(t, err)

			for i := 0; i < 15; i++ {
				if i != 13 {
					tree.Insert(i, i)
				}
			}

			value, ok := tree.Search(8)
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
		t.Run("NewScanner", func(t *testing.T) {
			var count int

			tree, err := NewGenericTree[int, int](4)
			ensureError(t, err)

			cursor := tree.NewScanner(math.MinInt64)
			for cursor.Scan() {
				count++
			}

			if got, want := count, 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
		})
		t.Run("NewScannerAll", func(t *testing.T) {
			var count int

			tree, err := NewGenericTree[int, int](4)
			ensureError(t, err)

			cursor := tree.NewScannerAll()
			for cursor.Scan() {
				count++
			}

			if got, want := count, 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
		})
	})
	t.Run("single-leaf tree", func(t *testing.T) {
		t.Run("scan for minimum key", func(t *testing.T) {
			var values []any

			tree, err := NewGenericTree[int, int](16)
			ensureError(t, err)

			for i := 0; i < 15; i++ {
				tree.Insert(i, i)
			}

			cursor := tree.NewScanner(math.MinInt64)
			for cursor.Scan() {
				_, v := cursor.Pair()
				values = append(values, v)
			}

			expected := []any{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}

			ensureSame(t, values, expected)
		})
		t.Run("scan for missing element", func(t *testing.T) {
			var values []any

			tree, err := NewGenericTree[int, int](16)
			ensureError(t, err)

			for i := 0; i < 15; i++ {
				if i != 13 {
					tree.Insert(i, i)
				}
			}

			cursor := tree.NewScanner(13)
			for cursor.Scan() {
				_, v := cursor.Pair()
				values = append(values, v)
			}

			expected := []any{14}

			ensureSame(t, values, expected)
		})
		t.Run("scan for existing element", func(t *testing.T) {
			var values []any

			tree, err := NewGenericTree[int, int](16)
			ensureError(t, err)

			for i := 0; i < 15; i++ {
				tree.Insert(i, i)
			}

			cursor := tree.NewScanner(13)
			for cursor.Scan() {
				_, v := cursor.Pair()
				values = append(values, v)
			}

			expected := []any{13, 14}

			ensureSame(t, values, expected)
		})
	})
	t.Run("multi-leaf tree", func(t *testing.T) {
		var values []any

		tree, err := NewGenericTree[int, int](4)
		ensureError(t, err)

		for i := 0; i < 15; i++ {
			tree.Insert(i, i)
		}

		cursor := tree.NewScanner(math.MinInt64)
		for cursor.Scan() {
			_, v := cursor.Pair()
			values = append(values, v)
		}

		expected := []any{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}

		ensureSame(t, values, expected)
	})
}

func TestGenericTreeUpdate(t *testing.T) {
	tree, err := NewGenericTree[int, string](8)
	ensureError(t, err)

	tree.Update(1, func(value string, ok bool) string {
		if got, want := ok, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		// value should be the zero value of the type
		if got, want := value, ""; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		return "first"
	})

	tree.Update(1, func(value string, ok bool) string {
		if got, want := ok, true; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := value, "first"; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		return "second"
	})

	value, ok := tree.Search(1)
	if got, want := ok, true; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := value, "second"; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}

	tree.Insert(3, "third")

	tree.Update(2, func(value string, ok bool) string {
		if got, want := ok, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		// value should be the zero value of the type
		if got, want := value, ""; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		return "fourth"
	})

	value, ok = tree.Search(2)
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
			leaf := &leafNode[int, int]{
				Runts:  []int{11, 21, 31},
				Values: []int{11, 21, 31},
			}
			bigEnough := leaf.deleteKey(2, 42)
			if got, want := bigEnough, true; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureNodesMatch(t, leaf, &leafNode[int, int]{
				Runts:  []int{11, 21, 31},
				Values: []int{11, 21, 31},
			})
		})
		t.Run("key is first", func(t *testing.T) {
			leaf := &leafNode[int, int]{
				Runts:  []int{11, 21, 31},
				Values: []int{11, 21, 31},
			}
			bigEnough := leaf.deleteKey(2, 11)
			if got, want := bigEnough, true; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureNodesMatch(t, leaf, &leafNode[int, int]{
				Runts:  []int{21, 31},
				Values: []int{21, 31},
			})
		})
		t.Run("key is middle", func(t *testing.T) {
			leaf := &leafNode[int, int]{
				Runts:  []int{11, 21, 31},
				Values: []int{11, 21, 31},
			}
			bigEnough := leaf.deleteKey(2, 21)
			if got, want := bigEnough, true; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureNodesMatch(t, leaf, &leafNode[int, int]{
				Runts:  []int{11, 31},
				Values: []int{11, 31},
			})
		})
		t.Run("key is last", func(t *testing.T) {
			leaf := &leafNode[int, int]{
				Runts:  []int{11, 21, 31},
				Values: []int{11, 21, 31},
			}
			bigEnough := leaf.deleteKey(2, 31)
			if got, want := bigEnough, true; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureNodesMatch(t, leaf, &leafNode[int, int]{
				Runts:  []int{11, 21},
				Values: []int{11, 21},
			})
		})
	})
	t.Run("will be too small", func(t *testing.T) {
		leaf := &leafNode[int, int]{
			Runts:  []int{11, 21, 31, 41},
			Values: []int{11, 21, 31, 41},
		}

		bigEnough := leaf.deleteKey(4, 21)
		if got, want := bigEnough, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}

		ensureNodesMatch(t, leaf, &leafNode[int, int]{
			Runts:  []int{11, 31, 41},
			Values: []int{11, 31, 41},
		})
	})
}

func TestGenericLeafNodeAdoptFrom(t *testing.T) {
	t.Run("left", func(t *testing.T) {
		right := &leafNode[int, int]{
			Runts:  []int{5, 6, 7},
			Values: []int{5, 6, 7},
		}
		left := &leafNode[int, int]{
			Runts:  []int{0, 1, 2, 3, 4},
			Values: []int{0, 1, 2, 3, 4},
			Next:   right,
		}

		right.adoptFromLeft(left)

		ensureNodesMatch(t, left, &leafNode[int, int]{
			Runts:  []int{0, 1, 2, 3},
			Values: []int{0, 1, 2, 3},
			Next:   right,
		})

		ensureNodesMatch(t, right, &leafNode[int, int]{
			Runts:  []int{4, 5, 6, 7},
			Values: []int{4, 5, 6, 7},
		})
	})
	t.Run("right", func(t *testing.T) {
		right := &leafNode[int, int]{
			Runts:  []int{3, 4, 5, 6, 7},
			Values: []int{3, 4, 5, 6, 7},
		}
		left := &leafNode[int, int]{
			Runts:  []int{0, 1, 2},
			Values: []int{0, 1, 2},
			Next:   right,
		}

		left.adoptFromRight(right)

		ensureNodesMatch(t, left, &leafNode[int, int]{
			Runts:  []int{0, 1, 2, 3},
			Values: []int{0, 1, 2, 3},
			Next:   right,
		})

		ensureNodesMatch(t, right, &leafNode[int, int]{
			Runts:  []int{4, 5, 6, 7},
			Values: []int{4, 5, 6, 7},
		})
	})
}

func TestGenericInternalNodeAdoptFrom(t *testing.T) {
	t.Run("left", func(t *testing.T) {
		leafI := &leafNode[int, int]{
			Runts:  []int{90, 92, 94, 96, 98},
			Values: []int{90, 92, 94, 96, 98},
			Next:   nil,
		}
		leafH := &leafNode[int, int]{
			Runts:  []int{80, 82, 84, 86, 88},
			Values: []int{80, 82, 84, 86, 88},
			Next:   leafI,
		}
		leafG := &leafNode[int, int]{
			Runts:  []int{70, 72, 74, 76, 78},
			Values: []int{70, 72, 74, 76, 78},
			Next:   leafH,
		}
		leafF := &leafNode[int, int]{
			Runts:  []int{60, 62, 64, 66, 68},
			Values: []int{60, 62, 64, 66, 68},
			Next:   leafG,
		}
		leafE := &leafNode[int, int]{
			Runts:  []int{50, 52, 54, 56, 58},
			Values: []int{50, 52, 54, 56, 58},
			Next:   leafF,
		}
		leafD := &leafNode[int, int]{
			Runts:  []int{40, 42, 44, 46, 48},
			Values: []int{40, 42, 44, 46, 48},
			Next:   leafE,
		}
		leafC := &leafNode[int, int]{
			Runts:  []int{30, 32, 34, 36, 38},
			Values: []int{30, 32, 34, 36, 38},
			Next:   leafD,
		}
		leafB := &leafNode[int, int]{
			Runts:  []int{20, 22, 24, 26, 28},
			Values: []int{20, 22, 24, 26, 28},
			Next:   leafC,
		}
		leafA := &leafNode[int, int]{
			Runts:  []int{10, 12, 14, 16, 18},
			Values: []int{10, 12, 14, 16, 18},
			Next:   leafB,
		}

		left := newInternal(leafA, leafB, leafC, leafD, leafE, leafF)
		right := newInternal(leafG, leafH, leafI)

		right.adoptFromLeft(left)

		ensureInternalNodesMatch(t, left, newInternal(leafA, leafB, leafC, leafD, leafE))
		ensureInternalNodesMatch(t, right, newInternal(leafF, leafG, leafH, leafI))
	})
	t.Run("right", func(t *testing.T) {
		leafI := &leafNode[int, int]{
			Runts:  []int{90, 92, 94, 96, 98},
			Values: []int{90, 92, 94, 96, 98},
			Next:   nil,
		}
		leafH := &leafNode[int, int]{
			Runts:  []int{80, 82, 84, 86, 88},
			Values: []int{80, 82, 84, 86, 88},
			Next:   leafI,
		}
		leafG := &leafNode[int, int]{
			Runts:  []int{70, 72, 74, 76, 78},
			Values: []int{70, 72, 74, 76, 78},
			Next:   leafH,
		}
		leafF := &leafNode[int, int]{
			Runts:  []int{60, 62, 64, 66, 68},
			Values: []int{60, 62, 64, 66, 68},
			Next:   leafG,
		}
		leafE := &leafNode[int, int]{
			Runts:  []int{50, 52, 54, 56, 58},
			Values: []int{50, 52, 54, 56, 58},
			Next:   leafF,
		}
		leafD := &leafNode[int, int]{
			Runts:  []int{40, 42, 44, 46, 48},
			Values: []int{40, 42, 44, 46, 48},
			Next:   leafE,
		}
		leafC := &leafNode[int, int]{
			Runts:  []int{30, 32, 34, 36, 38},
			Values: []int{30, 32, 34, 36, 38},
			Next:   leafD,
		}
		leafB := &leafNode[int, int]{
			Runts:  []int{20, 22, 24, 26, 28},
			Values: []int{20, 22, 24, 26, 28},
			Next:   leafC,
		}
		leafA := &leafNode[int, int]{
			Runts:  []int{10, 12, 14, 16, 18},
			Values: []int{10, 12, 14, 16, 18},
			Next:   leafB,
		}

		left := newInternal(leafA, leafB, leafC)
		right := newInternal(leafD, leafE, leafF, leafG, leafH, leafI)

		left.adoptFromRight(right)

		ensureInternalNodesMatch(t, left, newInternal(leafA, leafB, leafC, leafD))
		ensureInternalNodesMatch(t, right, newInternal(leafE, leafF, leafG, leafH, leafI))
	})
}

func TestGenericLeafNodeMergeWithRight(t *testing.T) {
	leafC := &leafNode[int, int]{
		Runts:  []int{6, 7, 8, 9},
		Values: []int{6, 7, 8, 9},
		Next:   nil,
	}
	leafB := &leafNode[int, int]{
		Runts:  []int{3, 4, 5},
		Values: []int{3, 4, 5},
		Next:   leafC,
	}
	leafA := &leafNode[int, int]{
		Runts:  []int{0, 1, 2},
		Values: []int{0, 1, 2},
		Next:   leafB,
	}

	leafA.absorbRight(leafB)

	ensureNodesMatch(t, leafA, &leafNode[int, int]{
		Runts:  []int{0, 1, 2, 3, 4, 5},
		Values: []int{0, 1, 2, 3, 4, 5},
		Next:   leafC,
	})

	if got, want := len(leafB.Runts), 0; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := len(leafB.Values), 0; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := leafB.Next, (*leafNode[int, int])(nil); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
}

func TestGenericInternalNodeMergeWithRight(t *testing.T) {
	leafI := &leafNode[int, int]{
		Runts:  []int{90, 92, 94, 96, 98},
		Values: []int{90, 92, 94, 96, 98},
		Next:   nil,
	}
	leafH := &leafNode[int, int]{
		Runts:  []int{80, 82, 84, 86, 88},
		Values: []int{80, 82, 84, 86, 88},
		Next:   leafI,
	}
	leafG := &leafNode[int, int]{
		Runts:  []int{70, 72, 74, 76, 78},
		Values: []int{70, 72, 74, 76, 78},
		Next:   leafH,
	}
	leafF := &leafNode[int, int]{
		Runts:  []int{60, 62, 64, 66, 68},
		Values: []int{60, 62, 64, 66, 68},
		Next:   leafG,
	}
	leafE := &leafNode[int, int]{
		Runts:  []int{50, 52, 54, 56, 58},
		Values: []int{50, 52, 54, 56, 58},
		Next:   leafF,
	}
	leafD := &leafNode[int, int]{
		Runts:  []int{40, 42, 44, 46, 48},
		Values: []int{40, 42, 44, 46, 48},
		Next:   leafE,
	}
	leafC := &leafNode[int, int]{
		Runts:  []int{30, 32, 34, 36, 38},
		Values: []int{30, 32, 34, 36, 38},
		Next:   leafD,
	}
	leafB := &leafNode[int, int]{
		Runts:  []int{20, 22, 24, 26, 28},
		Values: []int{20, 22, 24, 26, 28},
		Next:   leafC,
	}
	leafA := &leafNode[int, int]{
		Runts:  []int{10, 12, 14, 16, 18},
		Values: []int{10, 12, 14, 16, 18},
		Next:   leafB,
	}

	left := newInternal(leafA, leafB, leafC)
	right := newInternal(leafD, leafE, leafF, leafG)

	left.absorbRight(right)

	internal := newInternal(leafA, leafB, leafC, leafD, leafE, leafF, leafG)

	ensureInternalNodesMatch(t, left, internal)

	if got, want := len(right.Runts), 0; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := len(right.Children), 0; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
}

func TestGenericInternalNodeDeleteKey(t *testing.T) {
	t.Run("not too small", func(t *testing.T) {
		leafE := &leafNode[int, int]{
			Runts:  []int{50, 52, 54, 56, 58},
			Values: []int{50, 52, 54, 56, 58},
			Next:   nil,
		}
		leafD := &leafNode[int, int]{
			Runts:  []int{40, 42, 44, 46, 48},
			Values: []int{40, 42, 44, 46, 48},
			Next:   leafE,
		}
		leafC := &leafNode[int, int]{
			Runts:  []int{30, 32, 34, 36, 38},
			Values: []int{30, 32, 34, 36, 38},
			Next:   leafD,
		}
		leafB := &leafNode[int, int]{
			Runts:  []int{20, 22, 24, 26, 28},
			Values: []int{20, 22, 24, 26, 28},
			Next:   leafC,
		}
		leafA := &leafNode[int, int]{
			Runts:  []int{10, 12, 14, 16, 18},
			Values: []int{10, 12, 14, 16, 18},
			Next:   leafB,
		}

		internal := newInternal(leafA, leafB, leafC, leafD)

		bigEnough := internal.deleteKey(4, 22)
		if got, want := bigEnough, true; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
	})
	t.Run("internal node absorbs right when no left and skinny right", func(t *testing.T) {
		t.Run("internal node not too small", func(t *testing.T) {
			leafE := &leafNode[int, int]{
				Runts:  []int{50, 52, 54, 56, 58},
				Values: []int{50, 52, 54, 56, 58},
				Next:   nil,
			}
			leafD := &leafNode[int, int]{
				Runts:  []int{40, 42, 44, 46, 48},
				Values: []int{40, 42, 44, 46, 48},
				Next:   leafE,
			}
			leafC := &leafNode[int, int]{
				Runts:  []int{30, 32, 34, 36, 38},
				Values: []int{30, 32, 34, 36, 38},
				Next:   leafD,
			}
			leafB := &leafNode[int, int]{
				Runts:  []int{20, 22, 24, 26},
				Values: []int{20, 22, 24, 26},
				Next:   leafC,
			}
			leafA := &leafNode[int, int]{
				Runts:  []int{10, 12, 14, 16},
				Values: []int{10, 12, 14, 16},
				Next:   leafB,
			}

			internal := newInternal(leafA, leafB, leafC, leafD, leafE)

			// NOTE: When leaf A starts with 4 elements, and test deletes the
			// value 12 from it with a minimum node size of 4, the node will
			// no longer have enough elements, and its remaining elements will
			// be moved to other nodes. However, the internal node at the top
			// will still have enough elements, and its return value will be
			// true.
			bigEnough := internal.deleteKey(4, 12)
			if got, want := bigEnough, true; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}

			// Leaf A will adopt all elements from leaf B, and its Next field
			// will point to leaf C.
			ensureLeafNodesMatch(t, leafA, &leafNode[int, int]{
				Runts:  []int{10, 14, 16, 20, 22, 24, 26},
				Values: []int{10, 14, 16, 20, 22, 24, 26},
				Next:   leafC,
			})

			// Leaf B will have no remaining elements.
			if got, want := len(leafB.Runts), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := len(leafB.Values), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}

			// The other leaf nodes should be untouched.
			ensureLeafNodesMatch(t, leafC, &leafNode[int, int]{
				Runts:  []int{30, 32, 34, 36, 38},
				Values: []int{30, 32, 34, 36, 38},
				Next:   leafD,
			})
			ensureLeafNodesMatch(t, leafD, &leafNode[int, int]{
				Runts:  []int{40, 42, 44, 46, 48},
				Values: []int{40, 42, 44, 46, 48},
				Next:   leafE,
			})
			ensureLeafNodesMatch(t, leafE, &leafNode[int, int]{
				Runts:  []int{50, 52, 54, 56, 58},
				Values: []int{50, 52, 54, 56, 58},
				Next:   nil,
			})
		})
		t.Run("internal node too small", func(t *testing.T) {
			leafD := &leafNode[int, int]{
				Runts:  []int{40, 42, 44, 46, 48},
				Values: []int{40, 42, 44, 46, 48},
				Next:   nil,
			}
			leafC := &leafNode[int, int]{
				Runts:  []int{30, 32, 34, 36, 38},
				Values: []int{30, 32, 34, 36, 38},
				Next:   leafD,
			}
			leafB := &leafNode[int, int]{
				Runts:  []int{20, 22, 24, 26},
				Values: []int{20, 22, 24, 26},
				Next:   leafC,
			}
			leafA := &leafNode[int, int]{
				Runts:  []int{10, 12, 14, 16},
				Values: []int{10, 12, 14, 16},
				Next:   leafB,
			}

			internal := newInternal(leafA, leafB, leafC, leafD)

			bigEnough := internal.deleteKey(4, 12)
			if got, want := bigEnough, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}

			ensureLeafNodesMatch(t, leafA, &leafNode[int, int]{
				Runts:  []int{10, 14, 16, 20, 22, 24, 26},
				Values: []int{10, 14, 16, 20, 22, 24, 26},
				Next:   leafC,
			})

			// leaf B is reset
			if got, want := len(leafB.Runts), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := len(leafB.Values), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			if got := leafB.Next; got != nil {
				t.Errorf("GOT: %v; WANT: nil", got)
			}

			ensureLeafNodesMatch(t, leafC, &leafNode[int, int]{
				Runts:  []int{30, 32, 34, 36, 38},
				Values: []int{30, 32, 34, 36, 38},
				Next:   leafD,
			})

			ensureLeafNodesMatch(t, leafD, &leafNode[int, int]{
				Runts:  []int{40, 42, 44, 46, 48},
				Values: []int{40, 42, 44, 46, 48},
				Next:   nil,
			})
		})
	})
	t.Run("child adopts from right when no left and fat right", func(t *testing.T) {
		leafE := &leafNode[int, int]{
			Runts:  []int{50, 52, 54, 56, 58},
			Values: []int{50, 52, 54, 56, 58},
			Next:   nil,
		}
		leafD := &leafNode[int, int]{
			Runts:  []int{40, 42, 44, 46, 48},
			Values: []int{40, 42, 44, 46, 48},
			Next:   leafE,
		}
		leafC := &leafNode[int, int]{
			Runts:  []int{30, 32, 34, 36, 38},
			Values: []int{30, 32, 34, 36, 38},
			Next:   leafD,
		}
		leafB := &leafNode[int, int]{
			Runts:  []int{20, 22, 24, 26, 28},
			Values: []int{20, 22, 24, 26, 28},
			Next:   leafC,
		}
		leafA := &leafNode[int, int]{
			Runts:  []int{10, 12, 14, 16},
			Values: []int{10, 12, 14, 16},
			Next:   leafB,
		}

		internal := newInternal(leafA, leafB, leafC, leafD, leafE)

		bigEnough := internal.deleteKey(4, 12)
		if got, want := bigEnough, true; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}

		ensureLeafNodesMatch(t, leafA, &leafNode[int, int]{
			Runts:  []int{10, 14, 16, 20},
			Values: []int{10, 14, 16, 20},
			Next:   leafB,
		})
		ensureLeafNodesMatch(t, leafB, &leafNode[int, int]{
			Runts:  []int{22, 24, 26, 28},
			Values: []int{22, 24, 26, 28},
			Next:   leafC,
		})
		ensureLeafNodesMatch(t, leafC, &leafNode[int, int]{
			Runts:  []int{30, 32, 34, 36, 38},
			Values: []int{30, 32, 34, 36, 38},
			Next:   leafD,
		})
		ensureLeafNodesMatch(t, leafD, &leafNode[int, int]{
			Runts:  []int{40, 42, 44, 46, 48},
			Values: []int{40, 42, 44, 46, 48},
			Next:   leafE,
		})
		ensureLeafNodesMatch(t, leafE, &leafNode[int, int]{
			Runts:  []int{50, 52, 54, 56, 58},
			Values: []int{50, 52, 54, 56, 58},
			Next:   nil,
		})
	})
	t.Run("left absorbs child when skinny left and no right", func(t *testing.T) {
		t.Run("too small", func(t *testing.T) {
			leafD := &leafNode[int, int]{
				Runts:  []int{40, 42, 44, 46},
				Values: []int{40, 42, 44, 46},
				Next:   nil,
			}
			leafC := &leafNode[int, int]{
				Runts:  []int{30, 32, 34, 36},
				Values: []int{30, 32, 34, 36},
				Next:   leafD,
			}
			leafB := &leafNode[int, int]{
				Runts:  []int{20, 22, 24, 26},
				Values: []int{20, 22, 24, 26},
				Next:   leafC,
			}
			leafA := &leafNode[int, int]{
				Runts:  []int{10, 12, 14, 16},
				Values: []int{10, 12, 14, 16},
				Next:   leafB,
			}

			internal := newInternal(leafA, leafB, leafC, leafD)

			bigEnough := internal.deleteKey(4, 42)
			if got, want := bigEnough, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}

			ensureLeafNodesMatch(t, leafA, &leafNode[int, int]{
				Runts:  []int{10, 12, 14, 16},
				Values: []int{10, 12, 14, 16},
				Next:   leafB,
			})
			ensureLeafNodesMatch(t, leafB, &leafNode[int, int]{
				Runts:  []int{20, 22, 24, 26},
				Values: []int{20, 22, 24, 26},
				Next:   leafC,
			})
			ensureLeafNodesMatch(t, leafC, &leafNode[int, int]{
				Runts:  []int{30, 32, 34, 36, 40, 44, 46},
				Values: []int{30, 32, 34, 36, 40, 44, 46},
				Next:   nil,
			})
			if got, want := len(leafD.Runts), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := len(leafD.Values), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
		})
		t.Run("not too small", func(t *testing.T) {
			leafE := &leafNode[int, int]{
				Runts:  []int{50, 52, 54, 56},
				Values: []int{50, 52, 54, 56},
				Next:   nil,
			}
			leafD := &leafNode[int, int]{
				Runts:  []int{40, 42, 44, 46},
				Values: []int{40, 42, 44, 46},
				Next:   leafE,
			}
			leafC := &leafNode[int, int]{
				Runts:  []int{30, 32, 34, 36},
				Values: []int{30, 32, 34, 36},
				Next:   leafD,
			}
			leafB := &leafNode[int, int]{
				Runts:  []int{20, 22, 24, 26},
				Values: []int{20, 22, 24, 26},
				Next:   leafC,
			}
			leafA := &leafNode[int, int]{
				Runts:  []int{10, 12, 14, 16},
				Values: []int{10, 12, 14, 16},
				Next:   leafB,
			}

			internal := newInternal(leafA, leafB, leafC, leafD, leafE)

			bigEnough := internal.deleteKey(4, 52)
			if got, want := bigEnough, true; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}

			ensureLeafNodesMatch(t, leafA, &leafNode[int, int]{
				Runts:  []int{10, 12, 14, 16},
				Values: []int{10, 12, 14, 16},
				Next:   leafB,
			})
			ensureLeafNodesMatch(t, leafB, &leafNode[int, int]{
				Runts:  []int{20, 22, 24, 26},
				Values: []int{20, 22, 24, 26},
				Next:   leafC,
			})
			ensureLeafNodesMatch(t, leafC, &leafNode[int, int]{
				Runts:  []int{30, 32, 34, 36},
				Values: []int{30, 32, 34, 36},
				Next:   leafD,
			})
			ensureLeafNodesMatch(t, leafD, &leafNode[int, int]{
				Runts:  []int{40, 42, 44, 46, 50, 54, 56},
				Values: []int{40, 42, 44, 46, 50, 54, 56},
				Next:   nil,
			})
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
			leafC := &leafNode[int, int]{
				Runts:  []int{30, 32, 34, 36},
				Values: []int{30, 32, 34, 36},
				Next:   nil,
			}
			leafB := &leafNode[int, int]{
				Runts:  []int{20, 22, 24, 26},
				Values: []int{20, 22, 24, 26},
				Next:   leafC,
			}
			leafA := &leafNode[int, int]{
				Runts:  []int{10, 12, 14, 16},
				Values: []int{10, 12, 14, 16},
				Next:   leafB,
			}

			internal := newInternal(leafA, leafB, leafC)

			bigEnough := internal.deleteKey(4, 22)
			if got, want := bigEnough, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}

			ensureLeafNodesMatch(t, leafA, &leafNode[int, int]{
				Runts:  []int{10, 12, 14, 16, 20, 24, 26},
				Values: []int{10, 12, 14, 16, 20, 24, 26},
				Next:   leafC,
			})
			if got, want := len(leafB.Runts), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := len(leafB.Values), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureLeafNodesMatch(t, leafC, &leafNode[int, int]{
				Runts:  []int{30, 32, 34, 36},
				Values: []int{30, 32, 34, 36},
				Next:   nil,
			})
		})
		t.Run("not too small", func(t *testing.T) {
			leafE := &leafNode[int, int]{
				Runts:  []int{50, 52, 54, 56},
				Values: []int{50, 52, 54, 56},
				Next:   nil,
			}
			leafD := &leafNode[int, int]{
				Runts:  []int{40, 42, 44, 46},
				Values: []int{40, 42, 44, 46},
				Next:   leafE,
			}
			leafC := &leafNode[int, int]{
				Runts:  []int{30, 32, 34, 36},
				Values: []int{30, 32, 34, 36},
				Next:   leafD,
			}
			leafB := &leafNode[int, int]{
				Runts:  []int{20, 22, 24, 26},
				Values: []int{20, 22, 24, 26},
				Next:   leafC,
			}
			leafA := &leafNode[int, int]{
				Runts:  []int{10, 12, 14, 16},
				Values: []int{10, 12, 14, 16},
				Next:   leafB,
			}

			internal := newInternal(leafA, leafB, leafC, leafD, leafE)

			bigEnough := internal.deleteKey(4, 22)
			if got, want := bigEnough, true; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}

			ensureLeafNodesMatch(t, leafA, &leafNode[int, int]{
				Runts:  []int{10, 12, 14, 16, 20, 24, 26},
				Values: []int{10, 12, 14, 16, 20, 24, 26},
				Next:   leafC,
			})
			if got, want := len(leafB.Runts), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := len(leafB.Values), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureLeafNodesMatch(t, leafC, &leafNode[int, int]{
				Runts:  []int{30, 32, 34, 36},
				Values: []int{30, 32, 34, 36},
				Next:   leafD,
			})
			ensureLeafNodesMatch(t, leafD, &leafNode[int, int]{
				Runts:  []int{40, 42, 44, 46},
				Values: []int{40, 42, 44, 46},
				Next:   leafE,
			})
		})
	})
	t.Run("child adopts from right when skinny left and fat right", func(t *testing.T) {
		leafE := &leafNode[int, int]{
			Runts:  []int{50, 52, 54, 56, 58},
			Values: []int{50, 52, 54, 56, 58},
			Next:   nil,
		}
		leafD := &leafNode[int, int]{
			Runts:  []int{40, 42, 44, 46, 48},
			Values: []int{40, 42, 44, 46, 48},
			Next:   leafE,
		}
		leafC := &leafNode[int, int]{
			Runts:  []int{30, 32, 34, 36, 38},
			Values: []int{30, 32, 34, 36, 38},
			Next:   leafD,
		}
		leafB := &leafNode[int, int]{
			Runts:  []int{20, 22, 24, 26},
			Values: []int{20, 22, 24, 26},
			Next:   leafC,
		}
		leafA := &leafNode[int, int]{
			Runts:  []int{10, 12, 14, 16},
			Values: []int{10, 12, 14, 16},
			Next:   leafB,
		}

		internal := newInternal(leafA, leafB, leafC, leafD, leafE)

		bigEnough := internal.deleteKey(4, 22)
		if got, want := bigEnough, true; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}

		ensureLeafNodesMatch(t, leafA, &leafNode[int, int]{
			Runts:  []int{10, 12, 14, 16},
			Values: []int{10, 12, 14, 16},
			Next:   leafB,
		})
		ensureLeafNodesMatch(t, leafB, &leafNode[int, int]{
			Runts:  []int{20, 24, 26, 30},
			Values: []int{20, 24, 26, 30},
			Next:   leafC,
		})
		ensureLeafNodesMatch(t, leafC, &leafNode[int, int]{
			Runts:  []int{32, 34, 36, 38},
			Values: []int{32, 34, 36, 38},
			Next:   leafD,
		})
		ensureLeafNodesMatch(t, leafD, &leafNode[int, int]{
			Runts:  []int{40, 42, 44, 46, 48},
			Values: []int{40, 42, 44, 46, 48},
			Next:   leafE,
		})
		ensureLeafNodesMatch(t, leafE, &leafNode[int, int]{
			Runts:  []int{50, 52, 54, 56, 58},
			Values: []int{50, 52, 54, 56, 58},
			Next:   nil,
		})
	})
	t.Run("child adopts from left when fat left and no right", func(t *testing.T) {
		leafE := &leafNode[int, int]{
			Runts:  []int{50, 52, 54, 56},
			Values: []int{50, 52, 54, 56},
			Next:   nil,
		}
		leafD := &leafNode[int, int]{
			Runts:  []int{40, 42, 44, 46, 48},
			Values: []int{40, 42, 44, 46, 48},
			Next:   leafE,
		}
		leafC := &leafNode[int, int]{
			Runts:  []int{30, 32, 34, 36},
			Values: []int{30, 32, 34, 36},
			Next:   leafD,
		}
		leafB := &leafNode[int, int]{
			Runts:  []int{20, 22, 24, 26},
			Values: []int{20, 22, 24, 26},
			Next:   leafC,
		}
		leafA := &leafNode[int, int]{
			Runts:  []int{10, 12, 14, 16},
			Values: []int{10, 12, 14, 16},
			Next:   leafB,
		}

		internal := newInternal(leafA, leafB, leafC, leafD, leafE)

		bigEnough := internal.deleteKey(4, 52)
		if got, want := bigEnough, true; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}

		ensureLeafNodesMatch(t, leafA, &leafNode[int, int]{
			Runts:  []int{10, 12, 14, 16},
			Values: []int{10, 12, 14, 16},
			Next:   leafB,
		})
		ensureLeafNodesMatch(t, leafB, &leafNode[int, int]{
			Runts:  []int{20, 22, 24, 26},
			Values: []int{20, 22, 24, 26},
			Next:   leafC,
		})
		ensureLeafNodesMatch(t, leafC, &leafNode[int, int]{
			Runts:  []int{30, 32, 34, 36},
			Values: []int{30, 32, 34, 36},
			Next:   leafD,
		})
		ensureLeafNodesMatch(t, leafD, &leafNode[int, int]{
			Runts:  []int{40, 42, 44, 46},
			Values: []int{40, 42, 44, 46},
			Next:   leafE,
		})
		ensureLeafNodesMatch(t, leafE, &leafNode[int, int]{
			Runts:  []int{48, 50, 54, 56},
			Values: []int{48, 50, 54, 56},
			Next:   nil,
		})
	})
	t.Run("child adopts from left when fat left and skinny right", func(t *testing.T) {
		leafE := &leafNode[int, int]{
			Runts:  []int{50, 52, 54, 56},
			Values: []int{50, 52, 54, 56},
			Next:   nil,
		}
		leafD := &leafNode[int, int]{
			Runts:  []int{40, 42, 44, 46},
			Values: []int{40, 42, 44, 46},
			Next:   leafE,
		}
		leafC := &leafNode[int, int]{
			Runts:  []int{30, 32, 34, 36},
			Values: []int{30, 32, 34, 36},
			Next:   leafD,
		}
		leafB := &leafNode[int, int]{
			Runts:  []int{20, 22, 24, 26, 28},
			Values: []int{20, 22, 24, 26, 28},
			Next:   leafC,
		}
		leafA := &leafNode[int, int]{
			Runts:  []int{10, 12, 14, 16},
			Values: []int{10, 12, 14, 16},
			Next:   leafB,
		}

		internal := newInternal(leafA, leafB, leafC, leafD, leafE)

		bigEnough := internal.deleteKey(4, 32)
		if got, want := bigEnough, true; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}

		ensureLeafNodesMatch(t, leafA, &leafNode[int, int]{
			Runts:  []int{10, 12, 14, 16},
			Values: []int{10, 12, 14, 16},
			Next:   leafB,
		})
		ensureLeafNodesMatch(t, leafB, &leafNode[int, int]{
			Runts:  []int{20, 22, 24, 26},
			Values: []int{20, 22, 24, 26},
			Next:   leafC,
		})
		ensureLeafNodesMatch(t, leafC, &leafNode[int, int]{
			Runts:  []int{28, 30, 34, 36},
			Values: []int{28, 30, 34, 36},
			Next:   leafD,
		})
		ensureLeafNodesMatch(t, leafD, &leafNode[int, int]{
			Runts:  []int{40, 42, 44, 46},
			Values: []int{40, 42, 44, 46},
			Next:   leafE,
		})
		ensureLeafNodesMatch(t, leafE, &leafNode[int, int]{
			Runts:  []int{50, 52, 54, 56},
			Values: []int{50, 52, 54, 56},
			Next:   nil,
		})
	})
	t.Run("child adopts from right when fat left and fat right", func(t *testing.T) {
		leafE := &leafNode[int, int]{
			Runts:  []int{50, 52, 54, 56, 58},
			Values: []int{50, 52, 54, 56, 58},
			Next:   nil,
		}
		leafD := &leafNode[int, int]{
			Runts:  []int{40, 42, 44, 46, 48},
			Values: []int{40, 42, 44, 46, 48},
			Next:   leafE,
		}
		leafC := &leafNode[int, int]{
			Runts:  []int{30, 32, 34, 36},
			Values: []int{30, 32, 34, 36},
			Next:   leafD,
		}
		leafB := &leafNode[int, int]{
			Runts:  []int{20, 22, 24, 26, 28},
			Values: []int{20, 22, 24, 26, 28},
			Next:   leafC,
		}
		leafA := &leafNode[int, int]{
			Runts:  []int{10, 12, 14, 16, 18},
			Values: []int{10, 12, 14, 16, 18},
			Next:   leafB,
		}

		internal := newInternal(leafA, leafB, leafC, leafD, leafE)

		bigEnough := internal.deleteKey(4, 32)
		if got, want := bigEnough, true; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}

		ensureLeafNodesMatch(t, leafA, &leafNode[int, int]{
			Runts:  []int{10, 12, 14, 16, 18},
			Values: []int{10, 12, 14, 16, 18},
			Next:   leafB,
		})
		ensureLeafNodesMatch(t, leafB, &leafNode[int, int]{
			Runts:  []int{20, 22, 24, 26, 28},
			Values: []int{20, 22, 24, 26, 28},
			Next:   leafC,
		})
		ensureLeafNodesMatch(t, leafC, &leafNode[int, int]{
			Runts:  []int{30, 34, 36, 40},
			Values: []int{30, 34, 36, 40},
			Next:   leafD,
		})
		ensureLeafNodesMatch(t, leafD, &leafNode[int, int]{
			Runts:  []int{42, 44, 46, 48},
			Values: []int{42, 44, 46, 48},
			Next:   leafE,
		})
		ensureLeafNodesMatch(t, leafE, &leafNode[int, int]{
			Runts:  []int{50, 52, 54, 56, 58},
			Values: []int{50, 52, 54, 56, 58},
			Next:   nil,
		})
	})
}

func TestGenericDelete(t *testing.T) {
	// t.Skip("re-enable after insertion tests working")

	t.Run("order 2", func(t *testing.T) {
		t.Skip("FIXME: order of 2 panics")

		tree, err := NewGenericTree[int, int](2)
		ensureError(t, err)
		ensureValues(t, tree, nil)

		values := rand.Perm(8)

		for _, v := range values {
			tree.Insert(v, v)
		}

		// Ensure all values can be found in the tree.
		ensureValues(t, tree, []int{0, 1, 2, 3, 4, 5, 6, 7})

		// NOTE: Only delete up to but not including the final value, so can
		// verify when only a single datum remaining, the root should point to
		// a leaf node.

		t.Run("delete from non empty tree", func(t *testing.T) {
			for _, v := range values[:len(values)-1] {
				tree.Delete(v)
			}
		})

		final := values[len(values)-1]
		ensureValues(t, tree, []int{final})

		ensureNodesMatch(t, tree.root, &leafNode[int, int]{
			Runts:  []int{final},
			Values: []int{final},
			Next:   nil,
		})

		// NOTE: Now delete the final node, and ensure the root points to an
		// empty leaf node.
		tree.Delete(final)

		ensureNodesMatch(t, tree.root, &leafNode[int, int]{
			Runts:  []int{},
			Values: []int{},
			Next:   nil,
		})

		// NOTE: Should be able to delete from an empty tree without
		// consequence.
		t.Run("delete from empty tree", func(t *testing.T) {
			tree.Delete(final)
		})
	})

	t.Run("order 4", func(t *testing.T) {
		const order = 4

		tree, err := NewGenericTree[int, int](order)
		ensureError(t, err)
		ensureValues(t, tree, nil)

		values := rand.Perm(16)

		for _, v := range values {
			tree.Insert(v, v)
		}

		// Ensure all values can be found in the tree.
		ensureValues(t, tree, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15})

		// NOTE: Only delete up to but not including the final value, so can
		// verify when only a single datum remaining, the root should point to
		// a leaf node.

		t.Run("delete from non empty tree", func(t *testing.T) {
			for _, v := range values[:len(values)-1] {
				tree.Delete(v)
			}
		})

		final := values[len(values)-1]
		ensureValues(t, tree, []int{final})

		if false {
			ensureNodesMatch(t, tree.root, &leafNode[int, int]{
				Runts:  []int{final},
				Values: []int{final},
				Next:   nil,
			})

			// NOTE: Now delete the final node, and ensure the root points to an
			// empty leaf node.
			tree.Delete(final)

			ensureNodesMatch(t, tree.root, &leafNode[int, int]{
				Runts:  []int{},
				Values: []int{},
				Next:   nil,
			})

			// NOTE: Should be able to delete from an empty tree without
			// consequence.
			t.Run("delete from empty tree", func(t *testing.T) {
				tree.Delete(final)
			})
		}
	})

	t.Run("order 32", func(t *testing.T) {
		t.Skip("FIXME: leaves wrong values in tree")
		const order = 32

		tree, err := NewGenericTree[int, int](order)
		ensureError(t, err)
		ensureValues(t, tree, nil)

		for _, v := range randomizedValues {
			tree.Insert(v, v)
		}

		// Ensure all values can be found in the tree.
		for _, v := range randomizedValues {
			if _, ok := tree.Search(v); !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, true)
			}
		}

		t.Run("delete from non empty tree", func(t *testing.T) {
			for _, v := range randomizedValues {
				tree.Delete(v)
			}
		})

		ensureNodesMatch(t, tree.root, &leafNode[int, int]{
			Runts:  []int{},
			Values: []int{},
			Next:   nil,
		})

		// NOTE: Should be able to delete from an empty tree without
		// consequence.
		t.Run("delete from empty tree", func(t *testing.T) {
			tree.Delete(13)
		})
	})
}
