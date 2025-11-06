package gobptree

import (
	"cmp"
	"fmt"
	"testing"
	// gcmp "github.com/google/go-cmp/cmp"
	// "github.com/google/go-cmp/cmp/cmpopts"
)

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

func genericLeafFrom[K cmp.Ordered](next *genericLeafNode[K], items ...K) *genericLeafNode[K] {
	n := &genericLeafNode[K]{
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

func genericInternalFrom[K cmp.Ordered](items ...genericNode[K]) *genericInternalNode[K] {
	n := &genericInternalNode[K]{
		Runts:    make([]K, len(items)),
		Children: make([]genericNode[K], len(items)),
	}
	for i := 0; i < len(items); i++ {
		n.Runts[i] = items[i].smallest()
		n.Children[i] = items[i]
	}
	return n
}

////////////////////////////////////////

func ensureGenericLeaf[K cmp.Ordered](t *testing.T, got, want *genericLeafNode[K]) {
	// t.Helper()

	// if diff := gcmp.Diff(want, got, cmpopts.IgnoreFields(genericLeafNode[K]{}, "mutex")); diff != "" {
	// 	t.Errorf("leaf node (-want; +got)\n%s", diff)
	// }
	// return

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
		ensureGenericLeaf(t, got.Next, want.Next)
	})
}

func ensureGenericInternal[K cmp.Ordered](t *testing.T, got, want *genericInternalNode[K]) {
	t.Helper()

	// if diff := gcmp.Diff(want, got, cmpopts.IgnoreFields(genericInternalNode[K]{}, "mutex")); diff != "" {
	// 	t.Errorf("internal node (-want; +got)\n%s", diff)
	// }
	// return

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
			ensureGenericNode(t, got.Children[i], want.Children[i])
		}
	})
}

func ensureGenericNode[K cmp.Ordered](t *testing.T, got, want genericNode[K]) {
	t.Helper()
	// ensureSame(t, got, want)
	// return

	switch e := want.(type) {
	case *genericLeafNode[K]:
		a, ok := got.(*genericLeafNode[K])
		if got, want := ok, true; got != want {
			t.Errorf("GOT: %T; WANT: %T", got, e)
		}
		ensureGenericLeaf(t, a, e)
	case *genericInternalNode[K]:
		a, ok := got.(*genericInternalNode[K])
		if got, want := ok, true; got != want {
			t.Errorf("GOT: %T; WANT: %T", got, e)
		}
		ensureGenericInternal(t, a, e)
	default:
		t.Errorf("GOT: %T; WANT: genericNode", want)
	}
}

////////////////////////////////////////

func TestGenericInternalNodeMaybeSplit(t *testing.T) {
	leafD := genericLeafFrom(nil, 40, 41, 42, 43)
	leafC := genericLeafFrom(leafD, 30, 31, 32, 33)
	leafB := genericLeafFrom(leafC, 20, 21, 22, 23)
	leafA := genericLeafFrom(leafB, 10, 11, 12, 13)

	ni := genericInternalFrom(leafA, leafB, leafC, leafD)

	t.Run("does nothing when not full", func(t *testing.T) {
		_, right := ni.maybeSplit(6)
		if right != nil {
			t.Errorf("GOT: %v; WANT: %v", right, nil)
		}
	})

	t.Run("splits when full", func(t *testing.T) {
		expectedLeft := genericInternalFrom(leafA, leafB)
		expectedRight := genericInternalFrom(leafC, leafD)

		leftNode, rightNode := ni.maybeSplit(4)

		ensureGenericNode(t, leftNode, expectedLeft)
		ensureGenericNode(t, rightNode, expectedRight)
	})
}

func TestInternalGenericNodeInsertSmallerKey(t *testing.T) {
	gimme := func() (*genericLeafNode[int], *genericLeafNode[int]) {
		leafB := genericLeafFrom(nil, 21, 22)
		leafA := genericLeafFrom(leafB, 12, 13)
		return leafA, leafB
	}

	leafA, leafB := gimme()
	ni := genericInternalFrom(leafA, leafB)

	d := &GenericTree[int]{root: ni, order: 4}

	d.Insert(11, 11)

	if got, want := ni.Runts[0], 11; got != want {
		t.Fatalf("GOT: %v; WANT: %v", got, want)
	}
}

func TestGenericInsert(t *testing.T) {
	// t.Skip("FIXME")

	tree, _ := NewGenericTree[int](4)

	t.Run("0", func(t *testing.T) {
		tree.Insert(0, 0)
		ensureGenericNode(t, tree.root, genericLeafFrom(nil, 0))
	})

	t.Run("1", func(t *testing.T) {
		tree.Insert(1, 1)
		ensureGenericNode(t, tree.root, genericLeafFrom(nil, 0, 1))
	})

	t.Run("2", func(t *testing.T) {
		tree.Insert(2, 2)
		ensureGenericNode(t, tree.root, genericLeafFrom(nil, 0, 1, 2))
	})

	t.Run("3", func(t *testing.T) {
		tree.Insert(3, 3)
		ensureGenericNode(t, tree.root, genericLeafFrom(nil, 0, 1, 2, 3))
	})

	t.Run("4", func(t *testing.T) {
		leafB := genericLeafFrom(nil, 2, 3, 4)
		leafA := genericLeafFrom(leafB, 0, 1)

		tree.Insert(4, 4)
		ensureGenericNode(t, tree.root, genericInternalFrom(leafA, leafB))
	})

	t.Run("5", func(t *testing.T) {
		leafB := genericLeafFrom(nil, 2, 3, 4, 5)
		leafA := genericLeafFrom(leafB, 0, 1)

		tree.Insert(5, 5)
		ensureGenericNode(t, tree.root, genericInternalFrom(leafA, leafB))
	})

	t.Run("6", func(t *testing.T) {
		leafC := genericLeafFrom(nil, 4, 5, 6)
		leafB := genericLeafFrom(leafC, 2, 3)
		leafA := genericLeafFrom(leafB, 0, 1)

		tree.Insert(6, 6)
		ensureGenericNode(t, tree.root, genericInternalFrom(leafA, leafB, leafC))
	})
}

func TestGenericLeafNodeMaybeSplit(t *testing.T) {
	gimme := func() (*genericLeafNode[int], *genericLeafNode[int]) {
		leafB := genericLeafFrom(nil, 21, 22, 23, 24)
		leafA := genericLeafFrom(leafB, 11, 12, 13, 14)
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
		ensureGenericNode(t, leftNode, genericLeafFrom(rightNode.(*genericLeafNode[int]), 11, 12))
		ensureGenericNode(t, rightNode, genericLeafFrom(leafB, 13, 14))
	})
	t.Run("splits right edge when full", func(t *testing.T) {
		leafA, leafB := gimme()
		leftNode, rightNode := leafB.maybeSplit(4)
		if got, want := leafA.Next, leftNode; got != want {
			t.Fatalf("GOT: %v; WANT: %v", got, want)
		}
		ensureGenericNode(t, leftNode, genericLeafFrom(rightNode.(*genericLeafNode[int]), 21, 22))
		ensureGenericNode(t, rightNode, genericLeafFrom(nil, 23, 24))
	})
}

func TestInsertIntoSingleLeafGenericTree(t *testing.T) {
	t.Run("when fewer than order elements", func(t *testing.T) {
		t.Run("when empty", func(t *testing.T) {
			d, _ := NewGenericTree[int](4)
			nl, ok := d.root.(*genericLeafNode[int])
			if !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, false)
			}
			d.Insert(30, 30)
			ensureGenericLeaf(t, nl, genericLeafFrom(nil, 30))
		})
		t.Run("when less than first runt", func(t *testing.T) {
			d, _ := NewGenericTree[int](4)
			nl, ok := d.root.(*genericLeafNode[int])
			if !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, false)
			}
			d.Insert(30, 30)
			d.Insert(10, 10)
			ensureGenericNode(t, nl, genericLeafFrom(nil, 10, 30))
		})
		t.Run("when update value", func(t *testing.T) {
			d, _ := NewGenericTree[int](4)
			nl, ok := d.root.(*genericLeafNode[int])
			if !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, false)
			}
			d.Insert(30, 30)
			d.Insert(10, 10)
			d.Insert(30, 333)
			ensureGenericNode(t, nl, &genericLeafNode[int]{
				Runts:  []int{10, 30},
				Values: []any{10, 333},
			})
		})
		t.Run("when between first and final runt", func(t *testing.T) {
			d, _ := NewGenericTree[int](4)
			nl, ok := d.root.(*genericLeafNode[int])
			if !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, false)
			}
			d.Insert(30, 30)
			d.Insert(10, 10)
			d.Insert(20, 20)
			ensureGenericNode(t, nl, genericLeafFrom(nil, 10, 20, 30))
		})
		t.Run("when after final runt", func(t *testing.T) {
			d, _ := NewGenericTree[int](4)
			nl, ok := d.root.(*genericLeafNode[int])
			if !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, false)
			}
			d.Insert(30, 30)
			d.Insert(10, 10)
			d.Insert(20, 20)
			d.Insert(40, 40)
			ensureGenericNode(t, nl, genericLeafFrom(nil, 10, 20, 30, 40))
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

			root, ok := d.root.(*genericInternalNode[int])
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
			ensureGenericNode(t, root.Children[0], genericLeafFrom(root.Children[1].(*genericLeafNode[int]), 0, 10, 20))

			if got, want := root.Runts[1], 30; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			ensureGenericNode(t, root.Children[1], genericLeafFrom(nil, 30, 40))
		})
		t.Run("when new key is in middle", func(t *testing.T) {
			d := gimme()
			d.Insert(25, 25)
			root, ok := d.root.(*genericInternalNode[int])
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
			ensureGenericNode(t, root.Children[0], genericLeafFrom(root.Children[1].(*genericLeafNode[int]), 10, 20, 25))

			if got, want := root.Runts[1], 30; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			ensureGenericNode(t, root.Children[1], genericLeafFrom(nil, 30, 40))
		})
		t.Run("when new key will be final node in right leaf", func(t *testing.T) {
			d := gimme()
			d.Insert(50, 50)
			root, ok := d.root.(*genericInternalNode[int])
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
			ensureGenericNode(t, root.Children[0], genericLeafFrom(root.Children[1].(*genericLeafNode[int]), 10, 20))

			if got, want := root.Runts[1], 30; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			ensureGenericNode(t, root.Children[1], genericLeafFrom(nil, 30, 40, 50))
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
			l := &genericLeafNode[int]{
				Runts:  []int{11, 21, 31},
				Values: []any{11, 21, 31},
			}
			tooSmall := l.deleteKey(2, 42)
			if got, want := tooSmall, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureGenericNode(t, l, &genericLeafNode[int]{
				Runts:  []int{11, 21, 31},
				Values: []any{11, 21, 31},
			})
		})
		t.Run("key is first", func(t *testing.T) {
			l := &genericLeafNode[int]{
				Runts:  []int{11, 21, 31},
				Values: []any{11, 21, 31},
			}
			tooSmall := l.deleteKey(2, 11)
			if got, want := tooSmall, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureGenericNode(t, l, &genericLeafNode[int]{
				Runts:  []int{21, 31},
				Values: []any{21, 31},
			})
		})
		t.Run("key is middle", func(t *testing.T) {
			l := &genericLeafNode[int]{
				Runts:  []int{11, 21, 31},
				Values: []any{11, 21, 31},
			}
			tooSmall := l.deleteKey(2, 21)
			if got, want := tooSmall, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureGenericNode(t, l, &genericLeafNode[int]{
				Runts:  []int{11, 31},
				Values: []any{11, 31},
			})
		})
		t.Run("key is last", func(t *testing.T) {
			l := &genericLeafNode[int]{
				Runts:  []int{11, 21, 31},
				Values: []any{11, 21, 31},
			}
			tooSmall := l.deleteKey(2, 31)
			if got, want := tooSmall, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureGenericNode(t, l, &genericLeafNode[int]{
				Runts:  []int{11, 21},
				Values: []any{11, 21},
			})
		})
	})
	t.Run("will be too small", func(t *testing.T) {
		l := genericLeafFrom(nil, 11, 21, 31, 41)
		tooSmall := l.deleteKey(4, 21)
		if got, want := tooSmall, true; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		ensureGenericNode(t, l, genericLeafFrom(nil, 11, 31, 41))
	})
}

func TestGenericLeafNodeAdoptFrom(t *testing.T) {
	t.Run("left", func(t *testing.T) {
		r := genericLeafFrom(nil, 5, 6, 7)
		l := genericLeafFrom(r, 0, 1, 2, 3, 4)

		r.adoptFromLeft(l)

		ensureGenericNode(t, l, genericLeafFrom(r, 0, 1, 2, 3))
		ensureGenericNode(t, r, genericLeafFrom(nil, 4, 5, 6, 7))
	})
	t.Run("right", func(t *testing.T) {
		r := genericLeafFrom(nil, 3, 4, 5, 6, 7)
		l := genericLeafFrom(r, 0, 1, 2)

		l.adoptFromRight(r)

		ensureGenericNode(t, l, genericLeafFrom(r, 0, 1, 2, 3))
		ensureGenericNode(t, r, genericLeafFrom(nil, 4, 5, 6, 7))
	})
}

func TestGenericInternalNodeAdoptFrom(t *testing.T) {
	t.Run("left", func(t *testing.T) {
		leafI := genericLeafFrom(nil, 90, 92, 94, 96, 98)
		leafH := genericLeafFrom(leafI, 80, 82, 84, 86, 88)
		leafG := genericLeafFrom(leafH, 70, 72, 74, 76, 78)
		leafF := genericLeafFrom(leafG, 60, 62, 64, 66, 68)
		leafE := genericLeafFrom(leafF, 50, 52, 54, 56, 58)
		leafD := genericLeafFrom(leafE, 40, 42, 44, 46, 48)
		leafC := genericLeafFrom(leafD, 30, 32, 34, 36, 38)
		leafB := genericLeafFrom(leafC, 20, 22, 24, 26, 28)
		leafA := genericLeafFrom(leafB, 10, 12, 14, 16, 18)

		left := genericInternalFrom(leafA, leafB, leafC, leafD, leafE, leafF)
		right := genericInternalFrom(leafG, leafH, leafI)

		right.adoptFromLeft(left)

		ensureGenericInternal(t, left, genericInternalFrom(leafA, leafB, leafC, leafD, leafE))
		ensureGenericInternal(t, right, genericInternalFrom(leafF, leafG, leafH, leafI))
	})
	t.Run("right", func(t *testing.T) {
		leafI := genericLeafFrom(nil, 90, 92, 94, 96, 98)
		leafH := genericLeafFrom(leafI, 80, 82, 84, 86, 88)
		leafG := genericLeafFrom(leafH, 70, 72, 74, 76, 78)
		leafF := genericLeafFrom(leafG, 60, 62, 64, 66, 68)
		leafE := genericLeafFrom(leafF, 50, 52, 54, 56, 58)
		leafD := genericLeafFrom(leafE, 40, 42, 44, 46, 48)
		leafC := genericLeafFrom(leafD, 30, 32, 34, 36, 38)
		leafB := genericLeafFrom(leafC, 20, 22, 24, 26, 28)
		leafA := genericLeafFrom(leafB, 10, 12, 14, 16, 18)

		left := genericInternalFrom(leafA, leafB, leafC)
		right := genericInternalFrom(leafD, leafE, leafF, leafG, leafH, leafI)

		left.adoptFromRight(right)

		ensureGenericInternal(t, left, genericInternalFrom(leafA, leafB, leafC, leafD))
		ensureGenericInternal(t, right, genericInternalFrom(leafE, leafF, leafG, leafH, leafI))
	})
}

func TestGenericLeafNodeMergeWithRight(t *testing.T) {
	leafC := genericLeafFrom(nil, 6, 7, 8, 9)
	leafB := genericLeafFrom(leafC, 3, 4, 5)
	leafA := genericLeafFrom(leafB, 0, 1, 2)

	leafA.absorbRight(leafB)

	ensureGenericNode(t, leafA, genericLeafFrom(leafC, 0, 1, 2, 3, 4, 5))

	if got, want := len(leafB.Runts), 0; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := len(leafB.Values), 0; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := leafB.Next, (*genericLeafNode[int])(nil); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
}

func TestGenericInternalNodeMergeWithRight(t *testing.T) {
	leafI := genericLeafFrom(nil, 90, 92, 94, 96, 98)
	leafH := genericLeafFrom(leafI, 80, 82, 84, 86, 88)
	leafG := genericLeafFrom(leafH, 70, 72, 74, 76, 78)
	leafF := genericLeafFrom(leafG, 60, 62, 64, 66, 68)
	leafE := genericLeafFrom(leafF, 50, 52, 54, 56, 58)
	leafD := genericLeafFrom(leafE, 40, 42, 44, 46, 48)
	leafC := genericLeafFrom(leafD, 30, 32, 34, 36, 38)
	leafB := genericLeafFrom(leafC, 20, 22, 24, 26, 28)
	leafA := genericLeafFrom(leafB, 10, 12, 14, 16, 18)

	left := genericInternalFrom(leafA, leafB, leafC)
	right := genericInternalFrom(leafD, leafE, leafF, leafG)

	left.absorbRight(right)

	ensureGenericInternal(t, left, genericInternalFrom(leafA, leafB, leafC, leafD, leafE, leafF, leafG))

	if got, want := len(right.Runts), 0; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := len(right.Children), 0; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
}

func TestGenericInternalNodeDeleteKey(t *testing.T) {
	t.Run("not too small", func(t *testing.T) {
		leafE := genericLeafFrom(nil, 50, 52, 54, 56, 58)
		leafD := genericLeafFrom(leafE, 40, 42, 44, 46, 48)
		leafC := genericLeafFrom(leafD, 30, 32, 34, 36, 38)
		leafB := genericLeafFrom(leafC, 20, 22, 24, 26, 28)
		leafA := genericLeafFrom(leafB, 10, 12, 14, 16, 18)

		child := genericInternalFrom(leafA, leafB, leafC, leafD)

		if got, want := child.deleteKey(4, 22), false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
	})
	t.Run("child absorbs right when no left and skinny right", func(t *testing.T) {
		t.Run("child not too small", func(t *testing.T) {
			leafE := genericLeafFrom(nil, 50, 52, 54, 56, 58)
			leafD := genericLeafFrom(leafE, 40, 42, 44, 46, 48)
			leafC := genericLeafFrom(leafD, 30, 32, 34, 36, 38)
			leafB := genericLeafFrom(leafC, 20, 22, 24, 26)
			leafA := genericLeafFrom(leafB, 10, 12, 14, 16)

			child := genericInternalFrom(leafA, leafB, leafC, leafD, leafE)

			tooSmall := child.deleteKey(4, 12)
			if got, want := tooSmall, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}

			ensureGenericLeaf(t, leafA, genericLeafFrom(leafC, 10, 14, 16, 20, 22, 24, 26))
			if got, want := len(leafB.Runts), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := len(leafB.Values), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureGenericLeaf(t, leafC, genericLeafFrom(leafD, 30, 32, 34, 36, 38))
			ensureGenericLeaf(t, leafD, genericLeafFrom(leafE, 40, 42, 44, 46, 48))
			ensureGenericLeaf(t, leafE, genericLeafFrom(nil, 50, 52, 54, 56, 58))
		})
		t.Run("child too small", func(t *testing.T) {
			leafD := genericLeafFrom(nil, 40, 42, 44, 46, 48)
			leafC := genericLeafFrom(leafD, 30, 32, 34, 36, 38)
			leafB := genericLeafFrom(leafC, 20, 22, 24, 26)
			leafA := genericLeafFrom(leafB, 10, 12, 14, 16)

			child := genericInternalFrom(leafA, leafB, leafC, leafD)

			tooSmall := child.deleteKey(4, 12)
			if got, want := tooSmall, true; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}

			ensureGenericLeaf(t, leafA, genericLeafFrom(leafC, 10, 14, 16, 20, 22, 24, 26))
			if got, want := len(leafB.Runts), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := len(leafB.Values), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureGenericLeaf(t, leafC, genericLeafFrom(leafD, 30, 32, 34, 36, 38))
			ensureGenericLeaf(t, leafD, genericLeafFrom(nil, 40, 42, 44, 46, 48))
		})
	})
	t.Run("child adopts from right when no left and fat right", func(t *testing.T) {
		leafE := genericLeafFrom(nil, 50, 52, 54, 56, 58)
		leafD := genericLeafFrom(leafE, 40, 42, 44, 46, 48)
		leafC := genericLeafFrom(leafD, 30, 32, 34, 36, 38)
		leafB := genericLeafFrom(leafC, 20, 22, 24, 26, 28)
		leafA := genericLeafFrom(leafB, 10, 12, 14, 16)

		child := genericInternalFrom(leafA, leafB, leafC, leafD, leafE)

		tooSmall := child.deleteKey(4, 12)
		if got, want := tooSmall, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}

		ensureGenericLeaf(t, leafA, genericLeafFrom(leafB, 10, 14, 16, 20))
		ensureGenericLeaf(t, leafB, genericLeafFrom(leafC, 22, 24, 26, 28))
		ensureGenericLeaf(t, leafC, genericLeafFrom(leafD, 30, 32, 34, 36, 38))
		ensureGenericLeaf(t, leafD, genericLeafFrom(leafE, 40, 42, 44, 46, 48))
		ensureGenericLeaf(t, leafE, genericLeafFrom(nil, 50, 52, 54, 56, 58))
	})
	t.Run("left absorbs child when skinny left and no right", func(t *testing.T) {
		t.Run("too small", func(t *testing.T) {
			leafD := genericLeafFrom(nil, 40, 42, 44, 46)
			leafC := genericLeafFrom(leafD, 30, 32, 34, 36)
			leafB := genericLeafFrom(leafC, 20, 22, 24, 26)
			leafA := genericLeafFrom(leafB, 10, 12, 14, 16)

			child := genericInternalFrom(leafA, leafB, leafC, leafD)

			tooSmall := child.deleteKey(4, 42)
			if got, want := tooSmall, true; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}

			ensureGenericLeaf(t, leafA, genericLeafFrom(leafB, 10, 12, 14, 16))
			ensureGenericLeaf(t, leafB, genericLeafFrom(leafC, 20, 22, 24, 26))
			ensureGenericLeaf(t, leafC, genericLeafFrom(nil, 30, 32, 34, 36, 40, 44, 46))
			if got, want := len(leafD.Runts), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := len(leafD.Values), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
		})
		t.Run("not too small", func(t *testing.T) {
			leafE := genericLeafFrom(nil, 50, 52, 54, 56)
			leafD := genericLeafFrom(leafE, 40, 42, 44, 46)
			leafC := genericLeafFrom(leafD, 30, 32, 34, 36)
			leafB := genericLeafFrom(leafC, 20, 22, 24, 26)
			leafA := genericLeafFrom(leafB, 10, 12, 14, 16)

			child := genericInternalFrom(leafA, leafB, leafC, leafD, leafE)

			tooSmall := child.deleteKey(4, 52)
			if got, want := tooSmall, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}

			ensureGenericLeaf(t, leafA, genericLeafFrom(leafB, 10, 12, 14, 16))
			ensureGenericLeaf(t, leafB, genericLeafFrom(leafC, 20, 22, 24, 26))
			ensureGenericLeaf(t, leafC, genericLeafFrom(leafD, 30, 32, 34, 36))
			ensureGenericLeaf(t, leafD, genericLeafFrom(nil, 40, 42, 44, 46, 50, 54, 56))
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
			leafC := genericLeafFrom(nil, 30, 32, 34, 36)
			leafB := genericLeafFrom(leafC, 20, 22, 24, 26)
			leafA := genericLeafFrom(leafB, 10, 12, 14, 16)

			child := genericInternalFrom(leafA, leafB, leafC)

			tooSmall := child.deleteKey(4, 22)
			if got, want := tooSmall, true; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}

			ensureGenericLeaf(t, leafA, genericLeafFrom(leafC, 10, 12, 14, 16, 20, 24, 26))
			if got, want := len(leafB.Runts), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := len(leafB.Values), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureGenericLeaf(t, leafC, genericLeafFrom(nil, 30, 32, 34, 36))
		})
		t.Run("not too small", func(t *testing.T) {
			leafE := genericLeafFrom(nil, 50, 52, 54, 56)
			leafD := genericLeafFrom(leafE, 40, 42, 44, 46)
			leafC := genericLeafFrom(leafD, 30, 32, 34, 36)
			leafB := genericLeafFrom(leafC, 20, 22, 24, 26)
			leafA := genericLeafFrom(leafB, 10, 12, 14, 16)

			child := genericInternalFrom(leafA, leafB, leafC, leafD, leafE)

			tooSmall := child.deleteKey(4, 22)
			if got, want := tooSmall, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}

			ensureGenericLeaf(t, leafA, genericLeafFrom(leafC, 10, 12, 14, 16, 20, 24, 26))
			if got, want := len(leafB.Runts), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := len(leafB.Values), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureGenericLeaf(t, leafC, genericLeafFrom(leafD, 30, 32, 34, 36))
			ensureGenericLeaf(t, leafD, genericLeafFrom(leafE, 40, 42, 44, 46))
		})
	})
	t.Run("child adopts from right when skinny left and fat right", func(t *testing.T) {
		leafE := genericLeafFrom(nil, 50, 52, 54, 56, 58)
		leafD := genericLeafFrom(leafE, 40, 42, 44, 46, 48)
		leafC := genericLeafFrom(leafD, 30, 32, 34, 36, 38)
		leafB := genericLeafFrom(leafC, 20, 22, 24, 26)
		leafA := genericLeafFrom(leafB, 10, 12, 14, 16)

		child := genericInternalFrom(leafA, leafB, leafC, leafD, leafE)

		tooSmall := child.deleteKey(4, 22)
		if got, want := tooSmall, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}

		ensureGenericLeaf(t, leafA, genericLeafFrom(leafB, 10, 12, 14, 16))
		ensureGenericLeaf(t, leafB, genericLeafFrom(leafC, 20, 24, 26, 30))
		ensureGenericLeaf(t, leafC, genericLeafFrom(leafD, 32, 34, 36, 38))
		ensureGenericLeaf(t, leafD, genericLeafFrom(leafE, 40, 42, 44, 46, 48))
		ensureGenericLeaf(t, leafE, genericLeafFrom(nil, 50, 52, 54, 56, 58))
	})
	t.Run("child adopts from left when fat left and no right", func(t *testing.T) {
		leafE := genericLeafFrom(nil, 50, 52, 54, 56)
		leafD := genericLeafFrom(leafE, 40, 42, 44, 46, 48)
		leafC := genericLeafFrom(leafD, 30, 32, 34, 36)
		leafB := genericLeafFrom(leafC, 20, 22, 24, 26)
		leafA := genericLeafFrom(leafB, 10, 12, 14, 16)

		child := genericInternalFrom(leafA, leafB, leafC, leafD, leafE)

		tooSmall := child.deleteKey(4, 52)
		if got, want := tooSmall, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}

		ensureGenericLeaf(t, leafA, genericLeafFrom(leafB, 10, 12, 14, 16))
		ensureGenericLeaf(t, leafB, genericLeafFrom(leafC, 20, 22, 24, 26))
		ensureGenericLeaf(t, leafC, genericLeafFrom(leafD, 30, 32, 34, 36))
		ensureGenericLeaf(t, leafD, genericLeafFrom(leafE, 40, 42, 44, 46))
		ensureGenericLeaf(t, leafE, genericLeafFrom(nil, 48, 50, 54, 56))
	})
	t.Run("child adopts from left when fat left and skinny right", func(t *testing.T) {
		leafE := genericLeafFrom(nil, 50, 52, 54, 56)
		leafD := genericLeafFrom(leafE, 40, 42, 44, 46)
		leafC := genericLeafFrom(leafD, 30, 32, 34, 36)
		leafB := genericLeafFrom(leafC, 20, 22, 24, 26, 28)
		leafA := genericLeafFrom(leafB, 10, 12, 14, 16)

		child := genericInternalFrom(leafA, leafB, leafC, leafD, leafE)

		tooSmall := child.deleteKey(4, 32)
		if got, want := tooSmall, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}

		ensureGenericLeaf(t, leafA, genericLeafFrom(leafB, 10, 12, 14, 16))
		ensureGenericLeaf(t, leafB, genericLeafFrom(leafC, 20, 22, 24, 26))
		ensureGenericLeaf(t, leafC, genericLeafFrom(leafD, 28, 30, 34, 36))
		ensureGenericLeaf(t, leafD, genericLeafFrom(leafE, 40, 42, 44, 46))
		ensureGenericLeaf(t, leafE, genericLeafFrom(nil, 50, 52, 54, 56))
	})
	t.Run("child adopts from right when fat left and fat right", func(t *testing.T) {
		leafE := genericLeafFrom(nil, 50, 52, 54, 56, 58)
		leafD := genericLeafFrom(leafE, 40, 42, 44, 46, 48)
		leafC := genericLeafFrom(leafD, 30, 32, 34, 36)
		leafB := genericLeafFrom(leafC, 20, 22, 24, 26, 28)
		leafA := genericLeafFrom(leafB, 10, 12, 14, 16, 18)

		child := genericInternalFrom(leafA, leafB, leafC, leafD, leafE)

		tooSmall := child.deleteKey(4, 32)
		if got, want := tooSmall, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}

		ensureGenericLeaf(t, leafA, genericLeafFrom(leafB, 10, 12, 14, 16, 18))
		ensureGenericLeaf(t, leafB, genericLeafFrom(leafC, 20, 22, 24, 26, 28))
		ensureGenericLeaf(t, leafC, genericLeafFrom(leafD, 30, 34, 36, 40))
		ensureGenericLeaf(t, leafD, genericLeafFrom(leafE, 42, 44, 46, 48))
		ensureGenericLeaf(t, leafE, genericLeafFrom(nil, 50, 52, 54, 56, 58))
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
