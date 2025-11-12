package gobptree

import (
	"cmp"
	"fmt"
	"math"
	"math/rand"
	"sort"
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
		// Cannot get here unless error introduced in function argument types.
		t.Errorf("BUG: GOT: %#v; WANT: node[K,V]", want)
	}
}

func ensureStructure[K cmp.Ordered, V any](t *testing.T, got, want node[K, V]) {
	t.Helper()

	t.Run("structure", func(t *testing.T) {
		t.Helper()

		// IMPORTANT: Must stitch before running following checks.
		_ = stitchNextValues(want, nil)

		ensureNodesMatch(t, got, want)
	})
}

func ensureTreeValues[K cmp.Ordered, V any](t *testing.T, tree *GenericTree[K, V], want []V) {
	t.Helper()

	t.Run("values", func(t *testing.T) {
		t.Helper()

		got := getTreeValues(t, tree)

		ensureSame(t, got, want)
	})
}

func getTreeValues[K cmp.Ordered, V any](t *testing.T, tree *GenericTree[K, V]) []V {
	t.Helper()

	var got []V

	scanner := tree.NewScannerAll()
	for scanner.Scan() {
		_, value := scanner.Pair()
		got = append(got, value)
	}

	ensureError(t, scanner.Close())
	return got
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
//
// NOTE: This function is only intended to be used in the test cases when
// comparing the actual structure of a tree against an expected structure. In
// this use-case the expected structure of a tree is specified by building
// internal nodes composed of leaf nodes, with the caveat that the next field
// for each leaf node is not specified in the test code, but rather is created
// by this function by setting the next pointer in each leaf node while
// walking the tree.
func stitchNextValues[K cmp.Ordered, V any](n node[K, V], nextLeaf *leafNode[K, V]) *leafNode[K, V] {
	switch tv := n.(type) {
	case *internalNode[K, V]:
		// Enumerate in reverse order, from the final to the first child,
		// capturing the next leaf pointer from the previous iteration and
		// passing it to the next iteration. This walks the tree at the
		// current node in depth-first order. After enumerating all children
		// return the pointer to the leaf node to the caller so the value can
		// bubble up and be passed on to left siblings of this node.
		for i := len(tv.Children) - 1; i >= 0; i-- {
			nextLeaf = stitchNextValues(tv.Children[i], nextLeaf)
		}
		return nextLeaf
	case *leafNode[K, V]:
		// Base case: when arrive at a leaf node, set its next pointer to the
		// value provided, as it points to the next leaf node to the right of
		// this node. Then return the pointer to this leaf node so it can be
		// passed to the left sibling of this node.
		tv.Next = nextLeaf
		return tv
	default:
		// Cannot get here unless error introduced in function argument types.
		panic(fmt.Errorf("BUG: GOT: %#v; WANT: node[K,V]", n))
	}
}

////////////////////////////////////////
// tests
////////////////////////////////////////

func TestGenericBinarySearch(t *testing.T) {
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
				i := searchGreaterThanOrEqualTo(0, []int64{1, 3, 5})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match first", func(t *testing.T) {
				i := searchGreaterThanOrEqualTo(1, []int64{1, 3, 5})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key between first and second", func(t *testing.T) {
				i := searchGreaterThanOrEqualTo(2, []int64{1, 3, 5})
				if got, want := i, 1; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match second", func(t *testing.T) {
				i := searchGreaterThanOrEqualTo(3, []int64{1, 3, 5})
				if got, want := i, 1; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key between second and third", func(t *testing.T) {
				i := searchGreaterThanOrEqualTo(4, []int64{1, 3, 5})
				if got, want := i, 2; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match third", func(t *testing.T) {
				i := searchGreaterThanOrEqualTo(5, []int64{1, 3, 5})
				if got, want := i, 2; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key after third", func(t *testing.T) {
				i := searchGreaterThanOrEqualTo(6, []int64{1, 3, 5})
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
				i := searchLessThanOrEqualTo(0, []int64{1, 3, 5})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match first", func(t *testing.T) {
				i := searchLessThanOrEqualTo(1, []int64{1, 3, 5})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key between first and second", func(t *testing.T) {
				i := searchLessThanOrEqualTo(2, []int64{1, 3, 5})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match second", func(t *testing.T) {
				i := searchLessThanOrEqualTo(3, []int64{1, 3, 5})
				if got, want := i, 1; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key between second and third", func(t *testing.T) {
				i := searchLessThanOrEqualTo(4, []int64{1, 3, 5})
				if got, want := i, 1; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match third", func(t *testing.T) {
				i := searchLessThanOrEqualTo(5, []int64{1, 3, 5})
				if got, want := i, 2; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key after third", func(t *testing.T) {
				i := searchLessThanOrEqualTo(6, []int64{1, 3, 5})
				if got, want := i, 2; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
		})
	})
}

////////////////////////////////////////
// Generic Internal Node
////////////////////////////////////////

func TestGenericInternalNode(t *testing.T) {
	// NOTE: For these tests there is no specific order of the nodes, because
	// the tested methods ignore the order.

	// NOTE: Must create nodes in descending order in order to be able to set
	// the next field of each node.
	leafI := &leafNode[int, int]{
		Runts:  []int{91, 93, 95},
		Values: []int{91, 93, 95},
		Next:   nil,
	}
	leafH := &leafNode[int, int]{
		Runts:  []int{81, 83, 85},
		Values: []int{81, 83, 85},
		Next:   leafI,
	}
	leafG := &leafNode[int, int]{
		Runts:  []int{71, 73, 75},
		Values: []int{71, 73, 75},
		Next:   leafH,
	}
	leafF := &leafNode[int, int]{
		Runts:  []int{61, 63, 65},
		Values: []int{61, 63, 65},
		Next:   leafG,
	}
	leafE := &leafNode[int, int]{
		Runts:  []int{51, 53, 55},
		Values: []int{51, 53, 55},
		Next:   leafF,
	}
	leafD := &leafNode[int, int]{
		Runts:  []int{41, 43, 45},
		Values: []int{41, 43, 45},
		Next:   leafE,
	}
	leafC := &leafNode[int, int]{
		Runts:  []int{31, 33, 35},
		Values: []int{31, 33, 35},
		Next:   leafD,
	}
	leafB := &leafNode[int, int]{
		Runts:  []int{21, 23, 15},
		Values: []int{21, 23, 15},
		Next:   leafC,
	}
	leafA := &leafNode[int, int]{
		Runts:  []int{11, 13, 15},
		Values: []int{11, 13, 15},
		Next:   leafB,
	}

	t.Run("absorbFromRight", func(t *testing.T) {
		t.Run("when right single node", func(t *testing.T) {
			t.Run("when left empty", func(t *testing.T) {
				left := &internalNode[int, int]{}
				right := newInternal(leafA)

				left.absorbFromRight(right)

				ensureInternalNodesMatch(t, left, newInternal(leafA))
				ensureInternalNodesMatch(t, right, &internalNode[int, int]{})
			})
			t.Run("when left single node", func(t *testing.T) {
				left := newInternal(leafA)
				right := newInternal(leafB)

				left.absorbFromRight(right)

				ensureInternalNodesMatch(t, left, newInternal(leafA, leafB))
				ensureInternalNodesMatch(t, right, &internalNode[int, int]{})
			})
			t.Run("when left multiple nodes", func(t *testing.T) {
				left := newInternal(leafA, leafB)
				right := newInternal(leafC)

				left.absorbFromRight(right)

				ensureInternalNodesMatch(t, left, newInternal(leafA, leafB, leafC))
				ensureInternalNodesMatch(t, right, &internalNode[int, int]{})
			})
		})

		t.Run("when right multiple nodes", func(t *testing.T) {
			t.Run("when left empty", func(t *testing.T) {
				left := &internalNode[int, int]{}
				right := newInternal(leafA, leafB)

				left.absorbFromRight(right)

				ensureInternalNodesMatch(t, left, newInternal(leafA, leafB))
				ensureInternalNodesMatch(t, right, &internalNode[int, int]{})
			})
			t.Run("when left single node", func(t *testing.T) {
				left := newInternal(leafA)
				right := newInternal(leafB, leafC)

				left.absorbFromRight(right)

				ensureInternalNodesMatch(t, left, newInternal(leafA, leafB, leafC))
				ensureInternalNodesMatch(t, right, &internalNode[int, int]{})
			})
			t.Run("when left multiple nodes", func(t *testing.T) {
				left := newInternal(leafA, leafB)
				right := newInternal(leafC, leafD)

				left.absorbFromRight(right)

				ensureInternalNodesMatch(t, left, newInternal(leafA, leafB, leafC, leafD))
				ensureInternalNodesMatch(t, right, &internalNode[int, int]{})
			})
		})
	})

	t.Run("adoptFromLeft", func(t *testing.T) {
		// NOTE: Cannot adopt from empty node, therefore no test cases for
		// adopting from left node when it is empty.

		t.Run("when left single node", func(t *testing.T) {
			t.Run("when right empty", func(t *testing.T) {
				left := newInternal(leafA)
				right := &internalNode[int, int]{}

				right.adoptFromLeft(left)

				ensureInternalNodesMatch(t, left, &internalNode[int, int]{Runts: []int{}})
				ensureInternalNodesMatch(t, right, newInternal(leafA))
			})
			t.Run("when right single node", func(t *testing.T) {
				left := newInternal(leafA)
				right := newInternal(leafB)

				right.adoptFromLeft(left)

				ensureInternalNodesMatch(t, left, &internalNode[int, int]{Runts: []int{}})
				ensureInternalNodesMatch(t, right, newInternal(leafA, leafB))
			})
			t.Run("when right multiple nodes", func(t *testing.T) {
				left := newInternal(leafA)
				right := newInternal(leafB, leafC)

				right.adoptFromLeft(left)

				ensureInternalNodesMatch(t, left, &internalNode[int, int]{Runts: []int{}})
				ensureInternalNodesMatch(t, right, newInternal(leafA, leafB, leafC))
			})
		})

		t.Run("when left multiple nodes", func(t *testing.T) {
			t.Run("when right empty", func(t *testing.T) {
				left := newInternal(leafA, leafB)
				right := &internalNode[int, int]{}

				right.adoptFromLeft(left)

				ensureInternalNodesMatch(t, left, newInternal(leafA))
				ensureInternalNodesMatch(t, right, newInternal(leafB))
			})
			t.Run("when right single node", func(t *testing.T) {
				left := newInternal(leafA, leafB)
				right := newInternal(leafC)

				right.adoptFromLeft(left)

				ensureInternalNodesMatch(t, left, newInternal(leafA))
				ensureInternalNodesMatch(t, right, newInternal(leafB, leafC))
			})
			t.Run("when right multiple nodes", func(t *testing.T) {
				left := newInternal(leafA, leafB)
				right := newInternal(leafC, leafD)

				right.adoptFromLeft(left)

				ensureInternalNodesMatch(t, left, newInternal(leafA))
				ensureInternalNodesMatch(t, right, newInternal(leafB, leafC, leafD))
			})
		})
	})

	t.Run("adoptFromRight", func(t *testing.T) {
		// NOTE: Cannot adopt from empty node, therefore no test cases for
		// adopting from right node when it is empty.

		t.Run("when right single node", func(t *testing.T) {
			t.Run("when left empty", func(t *testing.T) {
				left := &internalNode[int, int]{}
				right := newInternal(leafA)

				left.adoptFromRight(right)

				ensureInternalNodesMatch(t, left, newInternal(leafA))
				ensureInternalNodesMatch(t, right, &internalNode[int, int]{Runts: []int{}})
			})
			t.Run("when left single node", func(t *testing.T) {
				left := newInternal(leafA)
				right := newInternal(leafB)

				left.adoptFromRight(right)

				ensureInternalNodesMatch(t, left, newInternal(leafA, leafB))
				ensureInternalNodesMatch(t, right, &internalNode[int, int]{Runts: []int{}})
			})
			t.Run("when left multiple nodes", func(t *testing.T) {
				left := newInternal(leafA, leafB)
				right := newInternal(leafC)

				left.adoptFromRight(right)

				ensureInternalNodesMatch(t, left, newInternal(leafA, leafB, leafC))
				ensureInternalNodesMatch(t, right, &internalNode[int, int]{Runts: []int{}})
			})
		})

		t.Run("when right multiple nodes", func(t *testing.T) {
			t.Run("when left empty", func(t *testing.T) {
				left := &internalNode[int, int]{}
				right := newInternal(leafA, leafB)

				left.adoptFromRight(right)

				ensureInternalNodesMatch(t, left, newInternal(leafA))
				ensureInternalNodesMatch(t, right, newInternal(leafB))
			})
			t.Run("when left single node", func(t *testing.T) {
				left := newInternal(leafA)
				right := newInternal(leafB, leafC)

				left.adoptFromRight(right)

				ensureInternalNodesMatch(t, left, newInternal(leafA, leafB))
				ensureInternalNodesMatch(t, right, newInternal(leafC))
			})
			t.Run("when left multiple nodes", func(t *testing.T) {
				left := newInternal(leafA, leafB)
				right := newInternal(leafC, leafD)

				left.adoptFromRight(right)

				ensureInternalNodesMatch(t, left, newInternal(leafA, leafB, leafC))
				ensureInternalNodesMatch(t, right, newInternal(leafD))
			})
		})
	})

	t.Run("deleteKey", func(t *testing.T) {
		t.Run("quick return", func(t *testing.T) {
			leafE := &leafNode[int, int]{
				Runts:  []int{51, 53, 55, 57},
				Values: []int{51, 53, 55, 57},
				Next:   nil,
			}
			leafD := &leafNode[int, int]{
				Runts:  []int{41, 43, 45, 47},
				Values: []int{41, 43, 45, 47},
				Next:   leafE,
			}
			leafC := &leafNode[int, int]{
				Runts:  []int{31, 33, 35, 37, 39},
				Values: []int{31, 33, 35, 37, 39},
				Next:   leafD,
			}
			leafB := &leafNode[int, int]{
				Runts:  []int{21, 23, 25, 27},
				Values: []int{21, 23, 25, 27},
				Next:   leafC,
			}
			leafA := &leafNode[int, int]{
				Runts:  []int{11, 13, 15, 17},
				Values: []int{11, 13, 15, 17},
				Next:   leafB,
			}
			internal := newInternal(leafA, leafB, leafC, leafD, leafE)

			size, smallest := internal.deleteKey(4, 35)

			t.Run("size", func(t *testing.T) {
				if got, want := size, 5; got != want {
					t.Errorf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("smallest", func(t *testing.T) {
				if got, want := smallest, 11; got != want {
					t.Errorf("GOT: %v; WANT: %v", got, want)
				}
			})
			ensureStructure(t, internal,
				newInternal(
					leafA,
					leafB,
					// new leaf c
					&leafNode[int, int]{
						Runts:  []int{31, 33, 37, 39},
						Values: []int{31, 33, 37, 39},
					},
					leafD,
					leafE,
				),
			)
		})

		t.Run("child adopts from right", func(t *testing.T) {
			leafE := &leafNode[int, int]{
				Runts:  []int{51, 53, 55, 57},
				Values: []int{51, 53, 55, 57},
				Next:   nil,
			}
			leafD := &leafNode[int, int]{
				Runts:  []int{41, 43, 45, 47, 49},
				Values: []int{41, 43, 45, 47, 49},
				Next:   leafE,
			}
			leafC := &leafNode[int, int]{
				Runts:  []int{31, 33, 35, 37},
				Values: []int{31, 33, 35, 37},
				Next:   leafD,
			}
			leafB := &leafNode[int, int]{
				Runts:  []int{21, 23, 25, 27},
				Values: []int{21, 23, 25, 27},
				Next:   leafC,
			}
			leafA := &leafNode[int, int]{
				Runts:  []int{11, 13, 15, 17},
				Values: []int{11, 13, 15, 17},
				Next:   leafB,
			}
			internal := newInternal(leafA, leafB, leafC, leafD, leafE)

			size, smallest := internal.deleteKey(4, 35)

			t.Run("size", func(t *testing.T) {
				if got, want := size, 5; got != want {
					t.Errorf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("smallest", func(t *testing.T) {
				if got, want := smallest, 11; got != want {
					t.Errorf("GOT: %v; WANT: %v", got, want)
				}
			})
			ensureStructure(t, internal,
				newInternal(
					leafA,
					leafB,
					// new leaf c
					&leafNode[int, int]{
						Runts:  []int{31, 33, 37, 41},
						Values: []int{31, 33, 37, 41},
					},
					// new leaf d
					&leafNode[int, int]{
						Runts:  []int{43, 45, 47, 49},
						Values: []int{43, 45, 47, 49},
					},
					leafE,
				),
			)
		})

		t.Run("child adopts from left", func(t *testing.T) {
			leafE := &leafNode[int, int]{
				Runts:  []int{51, 53, 55, 57},
				Values: []int{51, 53, 55, 57},
				Next:   nil,
			}
			leafD := &leafNode[int, int]{
				Runts:  []int{41, 43, 45, 47},
				Values: []int{41, 43, 45, 47},
				Next:   leafE,
			}
			leafC := &leafNode[int, int]{
				Runts:  []int{31, 33, 35, 37},
				Values: []int{31, 33, 35, 37},
				Next:   leafD,
			}
			leafB := &leafNode[int, int]{
				Runts:  []int{21, 23, 25, 27, 29},
				Values: []int{21, 23, 25, 27, 29},
				Next:   leafC,
			}
			leafA := &leafNode[int, int]{
				Runts:  []int{11, 13, 15, 17},
				Values: []int{11, 13, 15, 17},
				Next:   leafB,
			}
			internal := newInternal(leafA, leafB, leafC, leafD, leafE)

			size, smallest := internal.deleteKey(4, 35)

			t.Run("size", func(t *testing.T) {
				if got, want := size, 5; got != want {
					t.Errorf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("smallest", func(t *testing.T) {
				if got, want := smallest, 11; got != want {
					t.Errorf("GOT: %v; WANT: %v", got, want)
				}
			})
			ensureStructure(t, internal,
				newInternal(
					leafA,
					// new leaf b
					&leafNode[int, int]{
						Runts:  []int{21, 23, 25, 27},
						Values: []int{21, 23, 25, 27},
					},
					// new leaf c
					&leafNode[int, int]{
						Runts:  []int{29, 31, 33, 37},
						Values: []int{29, 31, 33, 37},
					},
					leafD,
					leafE,
				),
			)
		})

		t.Run("child absorbed by left", func(t *testing.T) {
			leafE := &leafNode[int, int]{
				Runts:  []int{51, 53, 55, 57},
				Values: []int{51, 53, 55, 57},
				Next:   nil,
			}
			leafD := &leafNode[int, int]{
				Runts:  []int{41, 43, 45, 47},
				Values: []int{41, 43, 45, 47},
				Next:   leafE,
			}
			leafC := &leafNode[int, int]{
				Runts:  []int{31, 33, 35, 37},
				Values: []int{31, 33, 35, 37},
				Next:   leafD,
			}
			leafB := &leafNode[int, int]{
				Runts:  []int{21, 23, 25, 27},
				Values: []int{21, 23, 25, 27},
				Next:   leafC,
			}
			leafA := &leafNode[int, int]{
				Runts:  []int{11, 13, 15, 17},
				Values: []int{11, 13, 15, 17},
				Next:   leafB,
			}
			internal := newInternal(leafA, leafB, leafC, leafD, leafE)

			size, smallest := internal.deleteKey(4, 35)

			t.Run("size", func(t *testing.T) {
				if got, want := size, 4; got != want {
					t.Errorf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("smallest", func(t *testing.T) {
				if got, want := smallest, 11; got != want {
					t.Errorf("GOT: %v; WANT: %v", got, want)
				}
			})
			ensureStructure(t, internal,
				newInternal(
					leafA,
					// new leaf b
					&leafNode[int, int]{
						Runts:  []int{21, 23, 25, 27, 31, 33, 37},
						Values: []int{21, 23, 25, 27, 31, 33, 37},
					},
					// leaf c removed
					leafD,
					leafE,
				),
			)
		})

		t.Run("right absorbed by child", func(t *testing.T) {
			leafE := &leafNode[int, int]{
				Runts:  []int{51, 53, 55, 57},
				Values: []int{51, 53, 55, 57},
				Next:   nil,
			}
			leafD := &leafNode[int, int]{
				Runts:  []int{41, 43, 45, 47},
				Values: []int{41, 43, 45, 47},
				Next:   leafE,
			}
			leafC := &leafNode[int, int]{
				Runts:  []int{31, 33, 35, 37},
				Values: []int{31, 33, 35, 37},
				Next:   leafD,
			}
			leafB := &leafNode[int, int]{
				Runts:  []int{21, 23, 25, 27},
				Values: []int{21, 23, 25, 27},
				Next:   leafC,
			}
			leafA := &leafNode[int, int]{
				Runts:  []int{11, 13, 15, 17},
				Values: []int{11, 13, 15, 17},
				Next:   leafB,
			}
			internal := newInternal(leafA, leafB, leafC, leafD, leafE)

			size, smallest := internal.deleteKey(4, 15)

			t.Run("size", func(t *testing.T) {
				if got, want := size, 4; got != want {
					t.Errorf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("smallest", func(t *testing.T) {
				if got, want := smallest, 11; got != want {
					t.Errorf("GOT: %v; WANT: %v", got, want)
				}
			})
			ensureStructure(t, internal,
				newInternal(
					// new leaf a
					&leafNode[int, int]{
						Runts:  []int{11, 13, 17, 21, 23, 25, 27},
						Values: []int{11, 13, 17, 21, 23, 25, 27},
					},
					// leaf b removed
					// new leaf c
					&leafNode[int, int]{
						Runts:  []int{31, 33, 35, 37},
						Values: []int{31, 33, 35, 37},
					},
					leafD,
					leafE,
				),
			)
		})

		t.Run("no siblings", func(t *testing.T) {
			leafA := &leafNode[int, int]{
				Runts:  []int{11, 13, 15, 17},
				Values: []int{11, 13, 15, 17},
			}
			internal := newInternal(leafA)

			size, smallest := internal.deleteKey(4, 15)

			t.Run("size", func(t *testing.T) {
				if got, want := size, 1; got != want {
					t.Errorf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("smallest", func(t *testing.T) {
				if got, want := smallest, 11; got != want {
					t.Errorf("GOT: %v; WANT: %v", got, want)
				}
			})
			ensureStructure(t, internal,
				newInternal(
					// new leaf a
					&leafNode[int, int]{
						Runts:  []int{11, 13, 17},
						Values: []int{11, 13, 17},
					},
				),
			)
		})
	})

	t.Run("maybeSplit", func(t *testing.T) {

		t.Run("does nothing when not full", func(t *testing.T) {
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

			_, right := internal.maybeSplit(6)

			if right != nil {
				t.Errorf("GOT: %v; WANT: %v", right, nil)
			}
		})

		t.Run("splits when full", func(t *testing.T) {
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

			gotLeft, gotRight := internal.maybeSplit(4)

			ensureNodesMatch(t, gotLeft, newInternal(leafA, leafB))
			ensureNodesMatch(t, gotRight, newInternal(leafC, leafD))
		})
	})
}

////////////////////////////////////////
// Generic Leaf Node
////////////////////////////////////////

func TestGenericLeafNode(t *testing.T) {
	t.Run("absorbFromRight", func(t *testing.T) {
		// NOTE: For these tests there is no specific order of the nodes, because
		// the absorbion methods ignore the order when pulling in every element
		// from the right sibling into the left sibling.

		t.Run("when right single node", func(t *testing.T) {
			t.Run("when left empty", func(t *testing.T) {
				right_right := &leafNode[int, int]{Runts: []int{22}, Values: []int{22}}
				right := &leafNode[int, int]{Runts: []int{11}, Values: []int{11}, Next: right_right}
				left := &leafNode[int, int]{Next: right}
				left.absorbFromRight(right)

				ensureLeafNodesMatch(t, left, &leafNode[int, int]{
					Runts:  []int{11},
					Values: []int{11},
					Next:   right_right,
				})

				ensureLeafNodesMatch(t, right, &leafNode[int, int]{})
			})
			t.Run("when left single node", func(t *testing.T) {
				right_right := &leafNode[int, int]{Runts: []int{33}, Values: []int{33}}
				right := &leafNode[int, int]{Runts: []int{22}, Values: []int{22}, Next: right_right}
				left := &leafNode[int, int]{Runts: []int{11}, Values: []int{11}, Next: right}
				left.absorbFromRight(right)

				ensureLeafNodesMatch(t, left, &leafNode[int, int]{
					Runts:  []int{11, 22},
					Values: []int{11, 22},
					Next:   right_right,
				})

				ensureLeafNodesMatch(t, right, &leafNode[int, int]{})
			})
			t.Run("when left multiple nodes", func(t *testing.T) {
				right_right := &leafNode[int, int]{Runts: []int{44}, Values: []int{44}}
				right := &leafNode[int, int]{Runts: []int{33}, Values: []int{33}, Next: right_right}
				left := &leafNode[int, int]{Runts: []int{11, 22}, Values: []int{11, 22}, Next: right}
				left.absorbFromRight(right)

				ensureLeafNodesMatch(t, left, &leafNode[int, int]{
					Runts:  []int{11, 22, 33},
					Values: []int{11, 22, 33},
					Next:   right_right,
				})

				ensureLeafNodesMatch(t, right, &leafNode[int, int]{})
			})
		})

		t.Run("when right multiple nodes", func(t *testing.T) {
			t.Run("when left empty", func(t *testing.T) {
				right_right := &leafNode[int, int]{Runts: []int{33}, Values: []int{33}}
				right := &leafNode[int, int]{Runts: []int{11, 22}, Values: []int{11, 22}, Next: right_right}
				left := &leafNode[int, int]{Next: right}
				left.absorbFromRight(right)

				ensureLeafNodesMatch(t, left, &leafNode[int, int]{
					Runts:  []int{11, 22},
					Values: []int{11, 22},
					Next:   right_right,
				})

				ensureLeafNodesMatch(t, right, &leafNode[int, int]{})
			})
			t.Run("when left single node", func(t *testing.T) {
				right_right := &leafNode[int, int]{Runts: []int{44}, Values: []int{44}}
				right := &leafNode[int, int]{Runts: []int{22, 33}, Values: []int{22, 33}, Next: right_right}
				left := &leafNode[int, int]{Runts: []int{11}, Values: []int{11}, Next: right}
				left.absorbFromRight(right)

				ensureLeafNodesMatch(t, left, &leafNode[int, int]{
					Runts:  []int{11, 22, 33},
					Values: []int{11, 22, 33},
					Next:   right_right,
				})

				ensureLeafNodesMatch(t, right, &leafNode[int, int]{})
			})
			t.Run("when left multiple nodes", func(t *testing.T) {
				right_right := &leafNode[int, int]{Runts: []int{55}, Values: []int{55}}
				right := &leafNode[int, int]{Runts: []int{33, 44}, Values: []int{33, 44}, Next: right_right}
				left := &leafNode[int, int]{Runts: []int{11, 22}, Values: []int{11, 22}, Next: right}
				left.absorbFromRight(right)

				ensureLeafNodesMatch(t, left, &leafNode[int, int]{
					Runts:  []int{11, 22, 33, 44},
					Values: []int{11, 22, 33, 44},
					Next:   right_right,
				})

				ensureLeafNodesMatch(t, right, &leafNode[int, int]{})
			})
		})
	})

	t.Run("adoptFromLeft", func(t *testing.T) {
		// NOTE: For these tests there is no specific order of the nodes, because
		// the adopt methods ignore the order when pulling in a single element
		// from the sibling into the node.

		t.Run("when left single node", func(t *testing.T) {
			t.Run("when right empty", func(t *testing.T) {
				right := &leafNode[int, int]{}
				left := &leafNode[int, int]{Runts: []int{11}, Values: []int{11}, Next: right}
				right.adoptFromLeft(left)

				ensureLeafNodesMatch(t, left, &leafNode[int, int]{
					Runts:  []int{},
					Values: []int{},
					Next:   right,
				})

				ensureLeafNodesMatch(t, right, &leafNode[int, int]{
					Runts:  []int{11},
					Values: []int{11},
				})
			})
			t.Run("when right single node", func(t *testing.T) {
				right := &leafNode[int, int]{Runts: []int{22}, Values: []int{22}}
				left := &leafNode[int, int]{Runts: []int{11}, Values: []int{11}, Next: right}
				right.adoptFromLeft(left)

				ensureLeafNodesMatch(t, left, &leafNode[int, int]{
					Runts:  []int{},
					Values: []int{},
					Next:   right,
				})

				ensureLeafNodesMatch(t, right, &leafNode[int, int]{
					Runts:  []int{11, 22},
					Values: []int{11, 22},
				})
			})
			t.Run("when right multiple nodes", func(t *testing.T) {
				right := &leafNode[int, int]{Runts: []int{22, 33}, Values: []int{22, 33}}
				left := &leafNode[int, int]{Runts: []int{11}, Values: []int{11}, Next: right}
				right.adoptFromLeft(left)

				ensureLeafNodesMatch(t, left, &leafNode[int, int]{
					Runts:  []int{},
					Values: []int{},
					Next:   right,
				})

				ensureLeafNodesMatch(t, right, &leafNode[int, int]{
					Runts:  []int{11, 22, 33},
					Values: []int{11, 22, 33},
				})
			})
		})

		t.Run("when left multiple nodes", func(t *testing.T) {
			t.Run("when right empty", func(t *testing.T) {
				right := &leafNode[int, int]{}
				left := &leafNode[int, int]{Runts: []int{11, 22}, Values: []int{11, 22}, Next: right}
				right.adoptFromLeft(left)

				ensureLeafNodesMatch(t, left, &leafNode[int, int]{
					Runts:  []int{11},
					Values: []int{11},
					Next:   right,
				})

				ensureLeafNodesMatch(t, right, &leafNode[int, int]{
					Runts:  []int{22},
					Values: []int{22},
				})
			})
			t.Run("when right single node", func(t *testing.T) {
				right := &leafNode[int, int]{Runts: []int{33}, Values: []int{33}}
				left := &leafNode[int, int]{Runts: []int{11, 22}, Values: []int{11, 22}, Next: right}
				right.adoptFromLeft(left)

				ensureLeafNodesMatch(t, left, &leafNode[int, int]{
					Runts:  []int{11},
					Values: []int{11},
					Next:   right,
				})

				ensureLeafNodesMatch(t, right, &leafNode[int, int]{
					Runts:  []int{22, 33},
					Values: []int{22, 33},
				})
			})
			t.Run("when right multiple nodes", func(t *testing.T) {
				right := &leafNode[int, int]{Runts: []int{33, 44}, Values: []int{33, 44}}
				left := &leafNode[int, int]{Runts: []int{11, 22}, Values: []int{11, 22}, Next: right}
				right.adoptFromLeft(left)

				ensureLeafNodesMatch(t, left, &leafNode[int, int]{
					Runts:  []int{11},
					Values: []int{11},
					Next:   right,
				})

				ensureLeafNodesMatch(t, right, &leafNode[int, int]{
					Runts:  []int{22, 33, 44},
					Values: []int{22, 33, 44},
				})
			})
		})
	})

	t.Run("adoptFromRight", func(t *testing.T) {
		// NOTE: For these tests there is no specific order of the nodes, because
		// the adopt methods ignore the order when pulling in a single element
		// from the sibling into the node.

		t.Run("when right single node", func(t *testing.T) {
			t.Run("when left empty", func(t *testing.T) {
				right := &leafNode[int, int]{Runts: []int{11}, Values: []int{11}}
				left := &leafNode[int, int]{Next: right}
				left.adoptFromRight(right)

				ensureLeafNodesMatch(t, left, &leafNode[int, int]{
					Runts:  []int{11},
					Values: []int{11},
					Next:   right,
				})

				ensureLeafNodesMatch(t, right, &leafNode[int, int]{
					Runts:  []int{},
					Values: []int{},
				})
			})
			t.Run("when left single node", func(t *testing.T) {
				right := &leafNode[int, int]{Runts: []int{22}, Values: []int{22}}
				left := &leafNode[int, int]{Runts: []int{11}, Values: []int{11}, Next: right}
				left.adoptFromRight(right)

				ensureLeafNodesMatch(t, left, &leafNode[int, int]{
					Runts:  []int{11, 22},
					Values: []int{11, 22},
					Next:   right,
				})

				ensureLeafNodesMatch(t, right, &leafNode[int, int]{
					Runts:  []int{},
					Values: []int{},
				})
			})
			t.Run("when left multiple nodes", func(t *testing.T) {
				right := &leafNode[int, int]{Runts: []int{33}, Values: []int{33}}
				left := &leafNode[int, int]{Runts: []int{11, 22}, Values: []int{11, 22}, Next: right}
				left.adoptFromRight(right)

				ensureLeafNodesMatch(t, left, &leafNode[int, int]{
					Runts:  []int{11, 22, 33},
					Values: []int{11, 22, 33},
					Next:   right,
				})

				ensureLeafNodesMatch(t, right, &leafNode[int, int]{
					Runts:  []int{},
					Values: []int{},
				})
			})
		})

		t.Run("when right multiple nodes", func(t *testing.T) {
			t.Run("when left empty", func(t *testing.T) {
				right := &leafNode[int, int]{Runts: []int{11, 22}, Values: []int{11, 22}}
				left := &leafNode[int, int]{Next: right}
				left.adoptFromRight(right)

				ensureLeafNodesMatch(t, left, &leafNode[int, int]{
					Runts:  []int{11},
					Values: []int{11},
					Next:   right,
				})

				ensureLeafNodesMatch(t, right, &leafNode[int, int]{
					Runts:  []int{22},
					Values: []int{22},
				})
			})
			t.Run("when left single node", func(t *testing.T) {
				right := &leafNode[int, int]{Runts: []int{22, 33}, Values: []int{22, 33}}
				left := &leafNode[int, int]{Runts: []int{11}, Values: []int{11}, Next: right}
				left.adoptFromRight(right)

				ensureLeafNodesMatch(t, left, &leafNode[int, int]{
					Runts:  []int{11, 22},
					Values: []int{11, 22},
					Next:   right,
				})

				ensureLeafNodesMatch(t, right, &leafNode[int, int]{
					Runts:  []int{33},
					Values: []int{33},
				})
			})
			t.Run("when left multiple nodes", func(t *testing.T) {
				right := &leafNode[int, int]{Runts: []int{33, 44}, Values: []int{33, 44}}
				left := &leafNode[int, int]{Runts: []int{11, 22}, Values: []int{11, 22}, Next: right}
				left.adoptFromRight(right)

				ensureLeafNodesMatch(t, left, &leafNode[int, int]{
					Runts:  []int{11, 22, 33},
					Values: []int{11, 22, 33},
					Next:   right,
				})

				ensureLeafNodesMatch(t, right, &leafNode[int, int]{
					Runts:  []int{44},
					Values: []int{44},
				})
			})
		})
	})

	t.Run("deleteKey", func(t *testing.T) {
		t.Run("before first key", func(t *testing.T) {
			leaf := &leafNode[int, int]{
				Runts:  []int{11, 33, 55},
				Values: []int{11, 33, 55},
			}

			size, smallest := leaf.deleteKey(2, 0)

			t.Run("size", func(t *testing.T) {
				if got, want := size, 3; got != want {
					t.Errorf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("smallest", func(t *testing.T) {
				if got, want := smallest, 11; got != want {
					t.Errorf("GOT: %v; WANT: %v", got, want)
				}
			})
			ensureNodesMatch(t, leaf, &leafNode[int, int]{
				Runts:  []int{11, 33, 55},
				Values: []int{11, 33, 55},
			})
		})

		t.Run("first key", func(t *testing.T) {
			leaf := &leafNode[int, int]{
				Runts:  []int{11, 33, 55},
				Values: []int{11, 33, 55},
			}

			size, smallest := leaf.deleteKey(3, 11)

			t.Run("size", func(t *testing.T) {
				if got, want := size, 2; got != want {
					t.Errorf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("smallest", func(t *testing.T) {
				if got, want := smallest, 33; got != want {
					t.Errorf("GOT: %v; WANT: %v", got, want)
				}
			})
			ensureNodesMatch(t, leaf, &leafNode[int, int]{
				Runts:  []int{33, 55},
				Values: []int{33, 55},
			})
		})

		t.Run("between first and second key", func(t *testing.T) {
			leaf := &leafNode[int, int]{
				Runts:  []int{11, 33, 55},
				Values: []int{11, 33, 55},
			}

			size, smallest := leaf.deleteKey(2, 22)

			t.Run("size", func(t *testing.T) {
				if got, want := size, 3; got != want {
					t.Errorf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("smallest", func(t *testing.T) {
				if got, want := smallest, 11; got != want {
					t.Errorf("GOT: %v; WANT: %v", got, want)
				}
			})
			ensureNodesMatch(t, leaf, &leafNode[int, int]{
				Runts:  []int{11, 33, 55},
				Values: []int{11, 33, 55},
			})
		})

		t.Run("second key", func(t *testing.T) {
			leaf := &leafNode[int, int]{
				Runts:  []int{11, 33, 55},
				Values: []int{11, 33, 55},
			}

			size, smallest := leaf.deleteKey(3, 33)

			t.Run("size", func(t *testing.T) {
				if got, want := size, 2; got != want {
					t.Errorf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("smallest", func(t *testing.T) {
				if got, want := smallest, 11; got != want {
					t.Errorf("GOT: %v; WANT: %v", got, want)
				}
			})
			ensureNodesMatch(t, leaf, &leafNode[int, int]{
				Runts:  []int{11, 55},
				Values: []int{11, 55},
			})
		})

		t.Run("between second and third key", func(t *testing.T) {
			leaf := &leafNode[int, int]{
				Runts:  []int{11, 33, 55},
				Values: []int{11, 33, 55},
			}

			size, smallest := leaf.deleteKey(3, 44)

			t.Run("size", func(t *testing.T) {
				if got, want := size, 3; got != want {
					t.Errorf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("smallest", func(t *testing.T) {
				if got, want := smallest, 11; got != want {
					t.Errorf("GOT: %v; WANT: %v", got, want)
				}
			})
			ensureNodesMatch(t, leaf, &leafNode[int, int]{
				Runts:  []int{11, 33, 55},
				Values: []int{11, 33, 55},
			})
		})

		t.Run("third key", func(t *testing.T) {
			leaf := &leafNode[int, int]{
				Runts:  []int{11, 33, 55},
				Values: []int{11, 33, 55},
			}
			size, smallest := leaf.deleteKey(3, 55)

			t.Run("size", func(t *testing.T) {
				if got, want := size, 2; got != want {
					t.Errorf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("smallest", func(t *testing.T) {
				if got, want := smallest, 11; got != want {
					t.Errorf("GOT: %v; WANT: %v", got, want)
				}
			})
			ensureNodesMatch(t, leaf, &leafNode[int, int]{
				Runts:  []int{11, 33},
				Values: []int{11, 33},
			})
		})

		t.Run("after third key", func(t *testing.T) {
			leaf := &leafNode[int, int]{
				Runts:  []int{11, 33, 55},
				Values: []int{11, 33, 55},
			}
			size, smallest := leaf.deleteKey(3, 66)

			t.Run("size", func(t *testing.T) {
				if got, want := size, 3; got != want {
					t.Errorf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("smallest", func(t *testing.T) {
				if got, want := smallest, 11; got != want {
					t.Errorf("GOT: %v; WANT: %v", got, want)
				}
			})
			ensureNodesMatch(t, leaf, &leafNode[int, int]{
				Runts:  []int{11, 33, 55},
				Values: []int{11, 33, 55},
			})
		})
	})

	t.Run("maybeSplit", func(t *testing.T) {
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
			_, right := leafB.maybeSplit(8)
			if right != nil {
				t.Errorf("GOT: %v; WANT: %v", right, nil)
			}
		})

		t.Run("when full", func(t *testing.T) {
			t.Run("when split a full node that is not the right edge", func(t *testing.T) {
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

			t.Run("split split a full node that is the right edge", func(t *testing.T) {
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
		})
	})
}

func TestGenericTree(t *testing.T) {
	t.Run("New with invalid order", func(t *testing.T) {
		for _, v := range []int{0, -1, 1, 3, 11} {
			_, err := NewGenericTree[int, int](v)
			if err == nil {
				ensureError(t, err, fmt.Sprintf("multiple of 2: %d", v))
			}
		}
	})

	t.Run("Delete", func(t *testing.T) {
		t.Run("order 2", func(t *testing.T) {
			// t.Skip("FIXME: order of 2 panics")

			tree, err := NewGenericTree[int, int](2)
			ensureError(t, err)
			ensureTreeValues(t, tree, nil)

			values := rand.Perm(8)

			if true { // DEBUG make insertion order deterministic for debugging
				sort.Ints(values)
			}

			for _, v := range values {
				tree.Insert(v, v)
				// tree.Insert(10*(v+1), 100*(v+1))
			}

			// Ensure all values can be found in the tree.
			ensureTreeValues(t, tree, []int{0, 1, 2, 3, 4, 5, 6, 7})

			// NOTE: Only delete up to but not including the final value, so can
			// verify when only a single datum remaining, the root should point to
			// a leaf node.

			t.Run("delete from non empty tree", func(t *testing.T) {
				for _, v := range values[:len(values)-1] {
					t.Logf("Before removing %d: values: %v\n", v, getTreeValues(t, tree))
					tree.Delete(v)
				}
			})

			t.Fatal("DEBUG")

			final := values[len(values)-1]
			ensureTreeValues(t, tree, []int{final})

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
			// t.Skip("TODO")
			const order = 4

			tree, err := NewGenericTree[int, int](order)
			ensureError(t, err)

			t.Run("before insertion tree is empty", func(t *testing.T) {
				ensureTreeValues(t, tree, nil)
			})

			values := rand.Perm(16)

			if true {
				// TODO: Race condition on insertion when retest over and over.
				values = []int{
					10,
					1,
					11,
					5,
					14,
					4,
					9,
					7,
					3,
					6,
					13,
					2,
					15,
					12,
					8,
					0,
				}
			}

			for _, v := range values {
				// t.Log(v)
				tree.Insert(v, v)
			}

			// Ensure all values can be found in the tree.
			t.Run("after insertion tree has values", func(t *testing.T) {
				ensureTreeValues(t, tree, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15})
			})

			// NOTE: Only delete up to but not including the final value, so can
			// verify when only a single datum remaining, the root should point to
			// a leaf node.

			t.Run("delete from non empty tree", func(t *testing.T) {
				for _, v := range values[:len(values)-1] {
					tree.Delete(v)
				}
			})

			final := values[len(values)-1]
			ensureTreeValues(t, tree, []int{final})

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
			const order = 32

			tree, err := NewGenericTree[int, int](order)
			ensureError(t, err)
			ensureTreeValues(t, tree, nil)

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
	})

	t.Run("Insert", func(t *testing.T) {
		t.Run("smaller key than first runt", func(t *testing.T) {
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
		})

		t.Run("order 2", func(t *testing.T) {
			tree, err := NewGenericTree[int, int](2)
			ensureError(t, err)

			t.Run("1", func(t *testing.T) {
				tree.Insert(1, 1)

				ensureStructure(t, tree.root, &leafNode[int, int]{
					Runts:  []int{1},
					Values: []int{1},
				})
			})

			t.Run("2", func(t *testing.T) {
				tree.Insert(2, 2)

				ensureStructure(t, tree.root, &leafNode[int, int]{
					Runts:  []int{1, 2},
					Values: []int{1, 2},
				})
			})

			t.Run("3", func(t *testing.T) {
				tree.Insert(3, 3)

				ensureStructure(t, tree.root,
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

				ensureStructure(t, tree.root,
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

				ensureStructure(t, tree.root,
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

				ensureStructure(t, tree.root,
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
		})

		t.Run("order 4", func(t *testing.T) {
			tree, err := NewGenericTree[int, int](4)
			ensureError(t, err)

			t.Run("1", func(t *testing.T) {
				tree.Insert(1, 1)
				ensureStructure(t, tree.root, &leafNode[int, int]{
					Runts:  []int{1},
					Values: []int{1},
				})
			})

			t.Run("2", func(t *testing.T) {
				tree.Insert(2, 2)
				ensureStructure(t, tree.root, &leafNode[int, int]{
					Runts:  []int{1, 2},
					Values: []int{1, 2},
				})
			})

			t.Run("3", func(t *testing.T) {
				tree.Insert(3, 3)
				ensureStructure(t, tree.root, &leafNode[int, int]{
					Runts:  []int{1, 2, 3},
					Values: []int{1, 2, 3},
				})
			})

			t.Run("4", func(t *testing.T) {
				tree.Insert(4, 4)
				ensureStructure(t, tree.root, &leafNode[int, int]{
					Runts:  []int{1, 2, 3, 4},
					Values: []int{1, 2, 3, 4},
				})
			})

			t.Run("5", func(t *testing.T) {
				tree.Insert(5, 5)

				ensureStructure(t, tree.root,
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

				ensureStructure(t, tree.root,
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

				ensureStructure(t, tree.root,
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

				ensureStructure(t, tree.root,
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

				ensureStructure(t, tree.root,
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

				ensureStructure(t, tree.root,
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

				ensureStructure(t, tree.root,
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

				ensureStructure(t, tree.root,
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

				ensureStructure(t, tree.root,
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
		})

		t.Run("single leaf", func(t *testing.T) {
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
		})
	})

	t.Run("Rebalance", func(t *testing.T) {
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

			ensureStructure(t, tree.root,
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

			ensureStructure(t, tree.root,
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

			ensureStructure(t, tree.root,
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
	})

	t.Run("Search", func(t *testing.T) {
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
	})

	t.Run("Update", func(t *testing.T) {
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
	})

	t.Run("Scanner", func(t *testing.T) {
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
	})
}
