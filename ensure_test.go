package gobptree

import (
	"cmp"
	"fmt"
	"strings"
	"testing"

	gocmp "github.com/google/go-cmp/cmp"
	// "github.com/google/go-cmp/cmp/cmpopts"
)

func ensureError(tb testing.TB, err error, contains ...string) {
	tb.Helper()
	if len(contains) == 0 || (len(contains) == 1 && contains[0] == "") {
		if err != nil {
			tb.Fatalf("GOT: %v; WANT: %v", err, contains)
		}
	} else if err == nil {
		tb.Errorf("GOT: %v; WANT: %v", err, contains)
	} else {
		for _, stub := range contains {
			if stub != "" && !strings.Contains(err.Error(), stub) {
				tb.Errorf("GOT: %v; WANT: %q", err, stub)
			}
		}
	}
}

func ensureInternalNodesMatch[K cmp.Ordered, V any](t *testing.T, got, want *internalNode[K, V]) {
	t.Helper()

	t.Run("internal", func(t *testing.T) {
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
			if true { // DEBUG normally true
				for i := 0; i < len(got.Children); i++ {
					t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
						t.Helper()
						ensureNodesMatch(t, got.Children[i], want.Children[i])
					})
				}
			}
		})
	})
}

func ensureLeafNodesMatch[K cmp.Ordered, V any](t *testing.T, got, want *leafNode[K, V]) {
	t.Helper()

	t.Run("leaf", func(t *testing.T) {
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

func ensureSame(tb testing.TB, got, want any) {
	tb.Helper()
	if diff := gocmp.Diff(want, got); diff != "" {
		tb.Errorf("(-want; +got)\n%s", diff)
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

func callbackExpects(t *testing.T, wantValue int, wantOk bool) func(int, bool) int {
	// t.Helper()
	return func(gotValue int, gotOk bool) int {
		// t.Helper()
		if gotValue != wantValue {
			t.Errorf("value: GOT: %v; WANT: %v", gotValue, wantValue)
		}
		if gotOk != wantOk {
			t.Errorf("ok: GOT: %v; WANT: %v", gotOk, wantOk)
		}
		return -1
	}
}

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
