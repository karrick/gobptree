package gobptree

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestUint64BinarySearch(t *testing.T) {
	t.Run("greater than or equal to", func(t *testing.T) {
		t.Run("empty list", func(t *testing.T) {
			i := uint64SearchGreaterThanOrEqualTo(1, nil)
			if got, want := i, 0; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
		})
		t.Run("single item list", func(t *testing.T) {
			t.Run("key before", func(t *testing.T) {
				i := uint64SearchGreaterThanOrEqualTo(1, []uint64{2})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match", func(t *testing.T) {
				i := uint64SearchGreaterThanOrEqualTo(2, []uint64{2})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key after", func(t *testing.T) {
				i := uint64SearchGreaterThanOrEqualTo(3, []uint64{2})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
		})
		t.Run("multiple item list", func(t *testing.T) {
			t.Run("key before first", func(t *testing.T) {
				i := uint64SearchGreaterThanOrEqualTo(1, []uint64{2, 4, 6})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match first", func(t *testing.T) {
				i := uint64SearchGreaterThanOrEqualTo(2, []uint64{2, 4, 6})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key between first and second", func(t *testing.T) {
				i := uint64SearchGreaterThanOrEqualTo(3, []uint64{2, 4, 6})
				if got, want := i, 1; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match second", func(t *testing.T) {
				i := uint64SearchGreaterThanOrEqualTo(4, []uint64{2, 4, 6})
				if got, want := i, 1; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key between second and third", func(t *testing.T) {
				i := uint64SearchGreaterThanOrEqualTo(5, []uint64{2, 4, 6})
				if got, want := i, 2; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match third", func(t *testing.T) {
				i := uint64SearchGreaterThanOrEqualTo(6, []uint64{2, 4, 6})
				if got, want := i, 2; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key after third", func(t *testing.T) {
				i := uint64SearchGreaterThanOrEqualTo(7, []uint64{2, 4, 6})
				if got, want := i, 2; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
		})
	})
	t.Run("less than or equal to", func(t *testing.T) {
		t.Run("empty list", func(t *testing.T) {
			i := uint64SearchLessThanOrEqualTo(1, nil)
			if got, want := i, 0; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
		})
		t.Run("single item list", func(t *testing.T) {
			t.Run("key before", func(t *testing.T) {
				i := uint64SearchLessThanOrEqualTo(1, []uint64{2})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match", func(t *testing.T) {
				i := uint64SearchLessThanOrEqualTo(uint64(2), []uint64{2})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key after", func(t *testing.T) {
				i := uint64SearchLessThanOrEqualTo(3, []uint64{2})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
		})
		t.Run("multiple item list", func(t *testing.T) {
			t.Run("key before first", func(t *testing.T) {
				i := uint64SearchLessThanOrEqualTo(1, []uint64{2, 4, 6})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match first", func(t *testing.T) {
				i := uint64SearchLessThanOrEqualTo(uint64(2), []uint64{2, 4, 6})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key between first and second", func(t *testing.T) {
				i := uint64SearchLessThanOrEqualTo(3, []uint64{2, 4, 6})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match second", func(t *testing.T) {
				i := uint64SearchLessThanOrEqualTo(4, []uint64{2, 4, 6})
				if got, want := i, 1; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key between second and third", func(t *testing.T) {
				i := uint64SearchLessThanOrEqualTo(5, []uint64{2, 4, 6})
				if got, want := i, 1; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match third", func(t *testing.T) {
				i := uint64SearchLessThanOrEqualTo(6, []uint64{2, 4, 6})
				if got, want := i, 2; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key after third", func(t *testing.T) {
				i := uint64SearchLessThanOrEqualTo(7, []uint64{2, 4, 6})
				if got, want := i, 2; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
		})
	})
}

func TestNewUint64TreeReturnsErrorWhenInvalidOrder(t *testing.T) {
	for _, v := range []int{0, -1, 1, 3, 11} {
		_, err := NewUint64Tree(v)
		if err == nil {
			t.Errorf("GOT: %v; WANT: %v", err, fmt.Sprintf("power of 2: %d", v))
		}
	}
}

func uint64LeafFrom(next *uint64LeafNode, items ...uint64) *uint64LeafNode {
	n := &uint64LeafNode{
		runts:  make([]uint64, len(items)),
		values: make([]interface{}, len(items)),
		next:   next,
	}
	for i := 0; i < len(items); i++ {
		n.runts[i] = items[i]
		n.values[i] = items[i]
	}
	return n
}

func uint64InternalFrom(items ...uint64Node) *uint64InternalNode {
	n := &uint64InternalNode{
		runts:    make([]uint64, len(items)),
		children: make([]uint64Node, len(items)),
	}
	for i := 0; i < len(items); i++ {
		n.runts[i] = items[i].smallest()
		n.children[i] = items[i]
	}
	return n
}

////////////////////////////////////////

func ensureUint64Leaf(t *testing.T, actual, expected *uint64LeafNode) {
	t.Helper()

	if got, want := len(actual.runts), len(expected.runts); got != want {
		t.Errorf("length(runts) GOT: %v; WANT: %v", got, want)
	}
	if got, want := len(actual.values), len(expected.values); got != want {
		t.Errorf("length(values) GOT: %v; WANT: %v", got, want)
	}
	for i := 0; i < len(actual.runts) && i < len(expected.runts); i++ {
		if got, want := actual.runts[i], expected.runts[i]; got != want {
			t.Errorf("runts[%d] GOT: %v; WANT: %v", i, got, want)
		}
		if got, want := actual.values[i], expected.values[i]; got != want {
			t.Errorf("values[%d] GOT: %v; WANT: %v", i, got, want)
		}
	}
	// ensureUint64Leaf(t, actual.next, expected.next)
	if got, want := actual.next, expected.next; got != want {
		t.Errorf("next GOT: %v; WANT: %v", got, want)
	}
	if t.Failed() {
		t.Errorf("\nGOT:\n\t%#v\nWANT:\n\t%#v", actual, expected)
	}
}

func ensureUint64Internal(t *testing.T, a, e *uint64InternalNode) {
	t.Helper()

	if got, want := len(a.runts), len(e.runts); got != want {
		t.Errorf("length(runts) GOT: %v; WANT: %v", got, want)
	}
	if got, want := len(a.children), len(e.children); got != want {
		t.Errorf("length(children) GOT: %v; WANT: %v", got, want)
	}
	for i := 0; i < len(a.runts) && i < len(e.runts); i++ {
		if got, want := a.runts[i], e.runts[i]; got != want {
			t.Errorf("runts[%d] GOT: %v; WANT: %v", i, got, want)
		}
		ensureUint64Node(t, a.children[i], e.children[i])
	}
}

func ensureUint64Node(t *testing.T, actual, expected uint64Node) {
	t.Helper()

	switch e := expected.(type) {
	case *uint64LeafNode:
		a, ok := actual.(*uint64LeafNode)
		if got, want := ok, true; got != want {
			t.Errorf("GOT: %T; WANT: %T", actual, e)
		}
		ensureUint64Leaf(t, a, e)
	case *uint64InternalNode:
		a, ok := actual.(*uint64InternalNode)
		if got, want := ok, true; got != want {
			t.Errorf("GOT: %T; WANT: %T", actual, e)
		}
		ensureUint64Internal(t, a, e)
	default:
		t.Errorf("GOT: %T; WANT: uint64Node", expected)
	}
}

////////////////////////////////////////

func TestUint64InternalNodeMaybeSplit(t *testing.T) {
	leafD := uint64LeafFrom(nil, 40, 41, 42, 43)
	leafC := uint64LeafFrom(leafD, 30, 31, 32, 33)
	leafB := uint64LeafFrom(leafC, 20, 21, 22, 23)
	leafA := uint64LeafFrom(leafB, 10, 11, 12, 13)

	ni := uint64InternalFrom(leafA, leafB, leafC, leafD)

	t.Run("does nothing when not full", func(t *testing.T) {
		_, right := ni.maybeSplit(6)
		if right != nil {
			t.Errorf("GOT: %v; WANT: %v", right, nil)
		}
	})

	t.Run("splits when full", func(t *testing.T) {
		expectedLeft := uint64InternalFrom(leafA, leafB)
		expectedRight := uint64InternalFrom(leafC, leafD)

		leftNode, rightNode := ni.maybeSplit(4)

		ensureUint64Node(t, leftNode, expectedLeft)
		ensureUint64Node(t, rightNode, expectedRight)
	})
}

func TestInternalUint64NodeInsertSmallerKey(t *testing.T) {
	gimme := func() (*uint64LeafNode, *uint64LeafNode) {
		leafB := uint64LeafFrom(nil, 21, 22)
		leafA := uint64LeafFrom(leafB, 12, 13)
		return leafA, leafB
	}

	leafA, leafB := gimme()
	ni := uint64InternalFrom(leafA, leafB)

	d := &Uint64Tree{root: ni, order: 4}

	d.Insert(11, 11)

	if got, want := ni.runts[0], uint64(11); got != want {
		t.Fatalf("GOT: %v; WANT: %v", got, want)
	}
}

func TestUint64LeafNodeMaybeSplit(t *testing.T) {
	gimme := func() (*uint64LeafNode, *uint64LeafNode) {
		leafB := uint64LeafFrom(nil, 21, 22, 23, 24)
		leafA := uint64LeafFrom(leafB, 11, 12, 13, 14)
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
		ensureUint64Node(t, leftNode, uint64LeafFrom(rightNode.(*uint64LeafNode), 11, 12))
		ensureUint64Node(t, rightNode, uint64LeafFrom(leafB, 13, 14))
	})
	t.Run("splits right edge when full", func(t *testing.T) {
		leafA, leafB := gimme()
		leftNode, rightNode := leafB.maybeSplit(4)
		if got, want := leafA.next, leftNode; got != want {
			t.Fatalf("GOT: %v; WANT: %v", got, want)
		}
		ensureUint64Node(t, leftNode, uint64LeafFrom(rightNode.(*uint64LeafNode), 21, 22))
		ensureUint64Node(t, rightNode, uint64LeafFrom(nil, 23, 24))
	})
}

func TestInsertIntoSingleLeafUint64Tree(t *testing.T) {
	t.Run("when fewer than order elements", func(t *testing.T) {
		t.Run("when empty", func(t *testing.T) {
			d, _ := NewUint64Tree(4)
			nl, ok := d.root.(*uint64LeafNode)
			if !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, false)
			}
			d.Insert(uint64(30), uint64(30))
			ensureUint64Leaf(t, nl, uint64LeafFrom(nil, 30))
		})
		t.Run("when less than first runt", func(t *testing.T) {
			d, _ := NewUint64Tree(4)
			nl, ok := d.root.(*uint64LeafNode)
			if !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, false)
			}
			d.Insert(uint64(30), uint64(30))
			d.Insert(uint64(10), uint64(10))
			ensureUint64Node(t, nl, uint64LeafFrom(nil, 10, 30))
		})
		t.Run("when update value", func(t *testing.T) {
			d, _ := NewUint64Tree(4)
			nl, ok := d.root.(*uint64LeafNode)
			if !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, false)
			}
			d.Insert(uint64(30), uint64(30))
			d.Insert(uint64(10), uint64(10))
			d.Insert(uint64(30), uint64(333))
			ensureUint64Node(t, nl, &uint64LeafNode{
				runts:  []uint64{10, 30},
				values: []interface{}{uint64(10), uint64(333)},
			})
		})
		t.Run("when between first and final runt", func(t *testing.T) {
			d, _ := NewUint64Tree(4)
			nl, ok := d.root.(*uint64LeafNode)
			if !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, false)
			}
			d.Insert(uint64(30), uint64(30))
			d.Insert(uint64(10), uint64(10))
			d.Insert(uint64(20), uint64(20))
			ensureUint64Node(t, nl, uint64LeafFrom(nil, 10, 20, 30))
		})
		t.Run("when after final runt", func(t *testing.T) {
			d, _ := NewUint64Tree(4)
			nl, ok := d.root.(*uint64LeafNode)
			if !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, false)
			}
			d.Insert(uint64(30), uint64(30))
			d.Insert(uint64(10), uint64(10))
			d.Insert(uint64(20), uint64(20))
			d.Insert(uint64(40), uint64(40))
			ensureUint64Node(t, nl, uint64LeafFrom(nil, 10, 20, 30, 40))
		})
	})

	t.Run("when insertion splits single leaf node", func(t *testing.T) {
		gimme := func() *Uint64Tree {
			d, _ := NewUint64Tree(4)
			for _, v := range []uint64{10, 20, 30, 40} {
				d.Insert(uint64(v), uint64(v))
			}
			return d
		}
		t.Run("when new key will be first node in left leaf", func(t *testing.T) {
			d := gimme()
			d.Insert(uint64(0), uint64(0))

			root, ok := d.root.(*uint64InternalNode)
			if !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, true)
			}
			// root should have two runts and two leaf nodes for children
			if got, want := len(root.runts), 2; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := len(root.children), 2; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			// ensure children nodes are as expected for this case
			if got, want := root.runts[0], uint64(0); got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			ensureUint64Node(t, root.children[0], uint64LeafFrom(root.children[1].(*uint64LeafNode), 0, 10, 20))

			if got, want := root.runts[1], uint64(30); got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			ensureUint64Node(t, root.children[1], uint64LeafFrom(nil, 30, 40))
		})
		t.Run("when new key is in middle", func(t *testing.T) {
			d := gimme()
			d.Insert(uint64(25), uint64(25))
			root, ok := d.root.(*uint64InternalNode)
			if !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, true)
			}
			// root should have two runts and two leaf nodes for children
			if got, want := len(root.runts), 2; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := len(root.children), 2; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			// ensure children nodes are as expected for this case
			if got, want := root.runts[0], uint64(10); got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			ensureUint64Node(t, root.children[0], uint64LeafFrom(root.children[1].(*uint64LeafNode), 10, 20, 25))

			if got, want := root.runts[1], uint64(30); got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			ensureUint64Node(t, root.children[1], uint64LeafFrom(nil, 30, 40))
		})
		t.Run("when new key will be final node in right leaf", func(t *testing.T) {
			d := gimme()
			d.Insert(uint64(50), uint64(50))
			root, ok := d.root.(*uint64InternalNode)
			if !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, true)
			}
			// root should have two runts and two leaf nodes for children
			if got, want := len(root.runts), 2; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := len(root.children), 2; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			// ensure children nodes are as expected for this case
			if got, want := root.runts[0], uint64(10); got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			ensureUint64Node(t, root.children[0], uint64LeafFrom(root.children[1].(*uint64LeafNode), 10, 20))

			if got, want := root.runts[1], uint64(30); got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			ensureUint64Node(t, root.children[1], uint64LeafFrom(nil, 30, 40, 50))
		})
	})
}

func TestUint64TreeSearch(t *testing.T) {
	t.Run("empty tree", func(t *testing.T) {
		d, _ := NewUint64Tree(16)

		_, ok := d.Search(13)
		if got, want := ok, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
	})
	t.Run("single-leaf tree", func(t *testing.T) {
		t.Run("missing value", func(t *testing.T) {
			d, _ := NewUint64Tree(16)
			for i := uint64(0); i < 15; i++ {
				if i != 13 {
					d.Insert(uint64(i), uint64(i))
				}
			}

			_, ok := d.Search(13)
			if got, want := ok, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
		})
		t.Run("existing value", func(t *testing.T) {
			d, _ := NewUint64Tree(16)
			for i := uint64(0); i < 15; i++ {
				if i != 13 {
					d.Insert(uint64(i), uint64(i))
				}
			}

			value, ok := d.Search(8)
			if got, want := ok, true; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := value, uint64(8); got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
		})
	})
	t.Run("multi-leaf tree", func(t *testing.T) {
		t.Run("missing value", func(t *testing.T) {
			d, _ := NewUint64Tree(4)
			for i := uint64(0); i < 15; i++ {
				if i != 13 {
					d.Insert(uint64(i), uint64(i))
				}
			}

			_, ok := d.Search(13)
			if got, want := ok, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
		})
		t.Run("existing value", func(t *testing.T) {
			d, _ := NewUint64Tree(4)
			for i := uint64(0); i < 15; i++ {
				if i != 13 {
					d.Insert(uint64(i), uint64(i))
				}
			}

			value, ok := d.Search(8)
			if got, want := ok, true; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := value, uint64(8); got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
		})
	})
}

func TestUint64TreeCursor(t *testing.T) {
	t.Run("empty tree", func(t *testing.T) {
		var count int

		d, _ := NewUint64Tree(4)
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
			var values []uint64

			d, _ := NewUint64Tree(16)
			for i := uint64(0); i < 15; i++ {
				d.Insert(uint64(i), uint64(i))
			}

			c := d.NewScanner(0)
			for c.Scan() {
				_, v := c.Pair()
				values = append(values, v.(uint64))
			}

			expected := []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}

			for i := 0; i < len(values) && i < len(expected); i++ {
				if got, want := values[i], expected[i]; got != want {
					t.Errorf("GOT: %v; WANT: %v", got, want)
				}
			}
		})
		t.Run("scan for missing element", func(t *testing.T) {
			var values []uint64

			d, _ := NewUint64Tree(16)
			for i := uint64(0); i < 15; i++ {
				if i != 13 {
					d.Insert(uint64(i), uint64(i))
				}
			}

			c := d.NewScanner(13)
			for c.Scan() {
				_, v := c.Pair()
				values = append(values, v.(uint64))
			}

			expected := []uint64{14, 2, 3, 4, 5, 6, 7, 8, 9}

			for i := 0; i < len(values) && i < len(expected); i++ {
				if got, want := values[i], expected[i]; got != want {
					t.Errorf("GOT: %v; WANT: %v", got, want)
				}
			}
		})
		t.Run("scan for existing element", func(t *testing.T) {
			var values []uint64

			d, _ := NewUint64Tree(16)
			for i := uint64(0); i < 15; i++ {
				d.Insert(uint64(i), uint64(i))
			}

			c := d.NewScanner(13)
			for c.Scan() {
				_, v := c.Pair()
				values = append(values, v.(uint64))
			}

			expected := []uint64{13, 14, 2, 3, 4, 5, 6, 7, 8, 9}

			for i := 0; i < len(values) && i < len(expected); i++ {
				if got, want := values[i], expected[i]; got != want {
					t.Errorf("GOT: %v; WANT: %v", got, want)
				}
			}
		})
	})
	t.Run("multi-leaf tree", func(t *testing.T) {
		var values []uint64

		d, _ := NewUint64Tree(4)
		for i := uint64(0); i < 15; i++ {
			d.Insert(uint64(i), uint64(i))
		}

		c := d.NewScanner(0)
		for c.Scan() {
			_, v := c.Pair()
			values = append(values, v.(uint64))
		}

		expected := []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}

		for i := 0; i < len(values) && i < len(expected); i++ {
			if got, want := values[i], expected[i]; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
		}
	})
}

func TestUint64TreeUpdate(t *testing.T) {
	d, _ := NewUint64Tree(8)
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
	d.Insert(uint64(3), uint64(3))
	d.Update(uint64(2), func(value interface{}, ok bool) interface{} {
		if got, want := ok, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := value, error(nil); got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		return "fourth"
	})
	value, ok = d.Search(uint64(2))
	if got, want := ok, true; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := value, "fourth"; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
}

func TestUint64LeafNodeDelete(t *testing.T) {
	t.Run("still big enough", func(t *testing.T) {
		t.Run("key is missing", func(t *testing.T) {
			l := &uint64LeafNode{
				runts:  []uint64{11, 21, 31},
				values: []interface{}{11, 21, 31},
			}
			tooSmall := l.deleteKey(2, 42)
			if got, want := tooSmall, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureUint64Node(t, l, &uint64LeafNode{
				runts:  []uint64{11, 21, 31},
				values: []interface{}{11, 21, 31},
			})
		})
		t.Run("key is first", func(t *testing.T) {
			l := &uint64LeafNode{
				runts:  []uint64{11, 21, 31},
				values: []interface{}{11, 21, 31},
			}
			tooSmall := l.deleteKey(2, 11)
			if got, want := tooSmall, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureUint64Node(t, l, &uint64LeafNode{
				runts:  []uint64{21, 31},
				values: []interface{}{21, 31},
			})
		})
		t.Run("key is middle", func(t *testing.T) {
			l := &uint64LeafNode{
				runts:  []uint64{11, 21, 31},
				values: []interface{}{11, 21, 31},
			}
			tooSmall := l.deleteKey(2, 21)
			if got, want := tooSmall, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureUint64Node(t, l, &uint64LeafNode{
				runts:  []uint64{11, 31},
				values: []interface{}{11, 31},
			})
		})
		t.Run("key is last", func(t *testing.T) {
			l := &uint64LeafNode{
				runts:  []uint64{11, 21, 31},
				values: []interface{}{11, 21, 31},
			}
			tooSmall := l.deleteKey(2, 31)
			if got, want := tooSmall, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureUint64Node(t, l, &uint64LeafNode{
				runts:  []uint64{11, 21},
				values: []interface{}{11, 21},
			})
		})
	})
	t.Run("will be too small", func(t *testing.T) {
		l := uint64LeafFrom(nil, 11, 21, 31, 41)
		tooSmall := l.deleteKey(4, 21)
		if got, want := tooSmall, true; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		ensureUint64Node(t, l, uint64LeafFrom(nil, 11, 31, 41))
	})
}

func TestUint64LeafNodeAdoptFrom(t *testing.T) {
	t.Run("left", func(t *testing.T) {
		r := uint64LeafFrom(nil, 5, 6, 7)
		l := uint64LeafFrom(r, 0, 1, 2, 3, 4)

		r.adoptFromLeft(l)

		ensureUint64Node(t, l, uint64LeafFrom(r, 0, 1, 2, 3))
		ensureUint64Node(t, r, uint64LeafFrom(nil, 4, 5, 6, 7))
	})
	t.Run("right", func(t *testing.T) {
		r := uint64LeafFrom(nil, 3, 4, 5, 6, 7)
		l := uint64LeafFrom(r, 0, 1, 2)

		l.adoptFromRight(r)

		ensureUint64Node(t, l, uint64LeafFrom(r, 0, 1, 2, 3))
		ensureUint64Node(t, r, uint64LeafFrom(nil, 4, 5, 6, 7))
	})
}

func TestUint64InternalNodeAdoptFrom(t *testing.T) {
	t.Run("left", func(t *testing.T) {
		leafI := uint64LeafFrom(nil, 90, 92, 94, 96, 98)
		leafH := uint64LeafFrom(leafI, 80, 82, 84, 86, 88)
		leafG := uint64LeafFrom(leafH, 70, 72, 74, 76, 78)
		leafF := uint64LeafFrom(leafG, 60, 62, 64, 66, 68)
		leafE := uint64LeafFrom(leafF, 50, 52, 54, 56, 58)
		leafD := uint64LeafFrom(leafE, 40, 42, 44, 46, 48)
		leafC := uint64LeafFrom(leafD, 30, 32, 34, 36, 38)
		leafB := uint64LeafFrom(leafC, 20, 22, 24, 26, 28)
		leafA := uint64LeafFrom(leafB, 10, 12, 14, 16, 18)

		left := uint64InternalFrom(leafA, leafB, leafC, leafD, leafE, leafF)
		right := uint64InternalFrom(leafG, leafH, leafI)

		right.adoptFromLeft(left)

		ensureUint64Internal(t, left, uint64InternalFrom(leafA, leafB, leafC, leafD, leafE))
		ensureUint64Internal(t, right, uint64InternalFrom(leafF, leafG, leafH, leafI))
	})
	t.Run("right", func(t *testing.T) {
		leafI := uint64LeafFrom(nil, 90, 92, 94, 96, 98)
		leafH := uint64LeafFrom(leafI, 80, 82, 84, 86, 88)
		leafG := uint64LeafFrom(leafH, 70, 72, 74, 76, 78)
		leafF := uint64LeafFrom(leafG, 60, 62, 64, 66, 68)
		leafE := uint64LeafFrom(leafF, 50, 52, 54, 56, 58)
		leafD := uint64LeafFrom(leafE, 40, 42, 44, 46, 48)
		leafC := uint64LeafFrom(leafD, 30, 32, 34, 36, 38)
		leafB := uint64LeafFrom(leafC, 20, 22, 24, 26, 28)
		leafA := uint64LeafFrom(leafB, 10, 12, 14, 16, 18)

		left := uint64InternalFrom(leafA, leafB, leafC)
		right := uint64InternalFrom(leafD, leafE, leafF, leafG, leafH, leafI)

		left.adoptFromRight(right)

		ensureUint64Internal(t, left, uint64InternalFrom(leafA, leafB, leafC, leafD))
		ensureUint64Internal(t, right, uint64InternalFrom(leafE, leafF, leafG, leafH, leafI))
	})
}

func TestUint64LeafNodeMergeWithRight(t *testing.T) {
	leafC := uint64LeafFrom(nil, 6, 7, 8, 9)
	leafB := uint64LeafFrom(leafC, 3, 4, 5)
	leafA := uint64LeafFrom(leafB, 0, 1, 2)

	leafA.absorbRight(leafB)

	ensureUint64Node(t, leafA, uint64LeafFrom(leafC, 0, 1, 2, 3, 4, 5))

	if got, want := len(leafB.runts), 0; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := len(leafB.values), 0; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := leafB.next, (*uint64LeafNode)(nil); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
}

func TestUint64InternalNodeMergeWithRight(t *testing.T) {
	leafI := uint64LeafFrom(nil, 90, 92, 94, 96, 98)
	leafH := uint64LeafFrom(leafI, 80, 82, 84, 86, 88)
	leafG := uint64LeafFrom(leafH, 70, 72, 74, 76, 78)
	leafF := uint64LeafFrom(leafG, 60, 62, 64, 66, 68)
	leafE := uint64LeafFrom(leafF, 50, 52, 54, 56, 58)
	leafD := uint64LeafFrom(leafE, 40, 42, 44, 46, 48)
	leafC := uint64LeafFrom(leafD, 30, 32, 34, 36, 38)
	leafB := uint64LeafFrom(leafC, 20, 22, 24, 26, 28)
	leafA := uint64LeafFrom(leafB, 10, 12, 14, 16, 18)

	left := uint64InternalFrom(leafA, leafB, leafC)
	right := uint64InternalFrom(leafD, leafE, leafF, leafG)

	left.absorbRight(right)

	ensureUint64Internal(t, left, uint64InternalFrom(leafA, leafB, leafC, leafD, leafE, leafF, leafG))

	if got, want := len(right.runts), 0; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := len(right.children), 0; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
}

func TestUint64InternalNodeDeleteKey(t *testing.T) {
	t.Run("not too small", func(t *testing.T) {
		leafE := uint64LeafFrom(nil, 50, 52, 54, 56, 58)
		leafD := uint64LeafFrom(leafE, 40, 42, 44, 46, 48)
		leafC := uint64LeafFrom(leafD, 30, 32, 34, 36, 38)
		leafB := uint64LeafFrom(leafC, 20, 22, 24, 26, 28)
		leafA := uint64LeafFrom(leafB, 10, 12, 14, 16, 18)

		child := uint64InternalFrom(leafA, leafB, leafC, leafD)

		if got, want := child.deleteKey(4, 22), false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
	})
	t.Run("child absorbs right when no left and skinny right", func(t *testing.T) {
		t.Run("child not too small", func(t *testing.T) {
			leafE := uint64LeafFrom(nil, 50, 52, 54, 56, 58)
			leafD := uint64LeafFrom(leafE, 40, 42, 44, 46, 48)
			leafC := uint64LeafFrom(leafD, 30, 32, 34, 36, 38)
			leafB := uint64LeafFrom(leafC, 20, 22, 24, 26)
			leafA := uint64LeafFrom(leafB, 10, 12, 14, 16)

			child := uint64InternalFrom(leafA, leafB, leafC, leafD, leafE)

			tooSmall := child.deleteKey(4, 12)
			if got, want := tooSmall, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}

			ensureUint64Leaf(t, leafA, uint64LeafFrom(leafC, 10, 14, 16, 20, 22, 24, 26))
			if got, want := len(leafB.runts), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := len(leafB.values), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureUint64Leaf(t, leafC, uint64LeafFrom(leafD, 30, 32, 34, 36, 38))
			ensureUint64Leaf(t, leafD, uint64LeafFrom(leafE, 40, 42, 44, 46, 48))
			ensureUint64Leaf(t, leafE, uint64LeafFrom(nil, 50, 52, 54, 56, 58))
		})
		t.Run("child too small", func(t *testing.T) {
			leafD := uint64LeafFrom(nil, 40, 42, 44, 46, 48)
			leafC := uint64LeafFrom(leafD, 30, 32, 34, 36, 38)
			leafB := uint64LeafFrom(leafC, 20, 22, 24, 26)
			leafA := uint64LeafFrom(leafB, 10, 12, 14, 16)

			child := uint64InternalFrom(leafA, leafB, leafC, leafD)

			tooSmall := child.deleteKey(4, 12)
			if got, want := tooSmall, true; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}

			ensureUint64Leaf(t, leafA, uint64LeafFrom(leafC, 10, 14, 16, 20, 22, 24, 26))
			if got, want := len(leafB.runts), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := len(leafB.values), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureUint64Leaf(t, leafC, uint64LeafFrom(leafD, 30, 32, 34, 36, 38))
			ensureUint64Leaf(t, leafD, uint64LeafFrom(nil, 40, 42, 44, 46, 48))
		})
	})
	t.Run("child adopts from right when no left and fat right", func(t *testing.T) {
		leafE := uint64LeafFrom(nil, 50, 52, 54, 56, 58)
		leafD := uint64LeafFrom(leafE, 40, 42, 44, 46, 48)
		leafC := uint64LeafFrom(leafD, 30, 32, 34, 36, 38)
		leafB := uint64LeafFrom(leafC, 20, 22, 24, 26, 28)
		leafA := uint64LeafFrom(leafB, 10, 12, 14, 16)

		child := uint64InternalFrom(leafA, leafB, leafC, leafD, leafE)

		tooSmall := child.deleteKey(4, 12)
		if got, want := tooSmall, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}

		ensureUint64Leaf(t, leafA, uint64LeafFrom(leafB, 10, 14, 16, 20))
		ensureUint64Leaf(t, leafB, uint64LeafFrom(leafC, 22, 24, 26, 28))
		ensureUint64Leaf(t, leafC, uint64LeafFrom(leafD, 30, 32, 34, 36, 38))
		ensureUint64Leaf(t, leafD, uint64LeafFrom(leafE, 40, 42, 44, 46, 48))
		ensureUint64Leaf(t, leafE, uint64LeafFrom(nil, 50, 52, 54, 56, 58))
	})
	t.Run("left absorbs child when skinny left and no right", func(t *testing.T) {
		t.Run("too small", func(t *testing.T) {
			leafD := uint64LeafFrom(nil, 40, 42, 44, 46)
			leafC := uint64LeafFrom(leafD, 30, 32, 34, 36)
			leafB := uint64LeafFrom(leafC, 20, 22, 24, 26)
			leafA := uint64LeafFrom(leafB, 10, 12, 14, 16)

			child := uint64InternalFrom(leafA, leafB, leafC, leafD)

			tooSmall := child.deleteKey(4, 42)
			if got, want := tooSmall, true; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}

			ensureUint64Leaf(t, leafA, uint64LeafFrom(leafB, 10, 12, 14, 16))
			ensureUint64Leaf(t, leafB, uint64LeafFrom(leafC, 20, 22, 24, 26))
			ensureUint64Leaf(t, leafC, uint64LeafFrom(nil, 30, 32, 34, 36, 40, 44, 46))
			if got, want := len(leafD.runts), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := len(leafD.values), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
		})
		t.Run("not too small", func(t *testing.T) {
			leafE := uint64LeafFrom(nil, 50, 52, 54, 56)
			leafD := uint64LeafFrom(leafE, 40, 42, 44, 46)
			leafC := uint64LeafFrom(leafD, 30, 32, 34, 36)
			leafB := uint64LeafFrom(leafC, 20, 22, 24, 26)
			leafA := uint64LeafFrom(leafB, 10, 12, 14, 16)

			child := uint64InternalFrom(leafA, leafB, leafC, leafD, leafE)

			tooSmall := child.deleteKey(4, 52)
			if got, want := tooSmall, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}

			ensureUint64Leaf(t, leafA, uint64LeafFrom(leafB, 10, 12, 14, 16))
			ensureUint64Leaf(t, leafB, uint64LeafFrom(leafC, 20, 22, 24, 26))
			ensureUint64Leaf(t, leafC, uint64LeafFrom(leafD, 30, 32, 34, 36))
			ensureUint64Leaf(t, leafD, uint64LeafFrom(nil, 40, 42, 44, 46, 50, 54, 56))
			if got, want := len(leafE.runts), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := len(leafE.values), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
		})
	})
	t.Run("left absorbs child when skinny left and skinny right", func(t *testing.T) {
		t.Run("too small", func(t *testing.T) {
			leafC := uint64LeafFrom(nil, 30, 32, 34, 36)
			leafB := uint64LeafFrom(leafC, 20, 22, 24, 26)
			leafA := uint64LeafFrom(leafB, 10, 12, 14, 16)

			child := uint64InternalFrom(leafA, leafB, leafC)

			tooSmall := child.deleteKey(4, 22)
			if got, want := tooSmall, true; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}

			ensureUint64Leaf(t, leafA, uint64LeafFrom(leafC, 10, 12, 14, 16, 20, 24, 26))
			if got, want := len(leafB.runts), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := len(leafB.values), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureUint64Leaf(t, leafC, uint64LeafFrom(nil, 30, 32, 34, 36))
		})
		t.Run("not too small", func(t *testing.T) {
			leafE := uint64LeafFrom(nil, 50, 52, 54, 56)
			leafD := uint64LeafFrom(leafE, 40, 42, 44, 46)
			leafC := uint64LeafFrom(leafD, 30, 32, 34, 36)
			leafB := uint64LeafFrom(leafC, 20, 22, 24, 26)
			leafA := uint64LeafFrom(leafB, 10, 12, 14, 16)

			child := uint64InternalFrom(leafA, leafB, leafC, leafD, leafE)

			tooSmall := child.deleteKey(4, 22)
			if got, want := tooSmall, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}

			ensureUint64Leaf(t, leafA, uint64LeafFrom(leafC, 10, 12, 14, 16, 20, 24, 26))
			if got, want := len(leafB.runts), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := len(leafB.values), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureUint64Leaf(t, leafC, uint64LeafFrom(leafD, 30, 32, 34, 36))
			ensureUint64Leaf(t, leafD, uint64LeafFrom(leafE, 40, 42, 44, 46))
		})
	})
	t.Run("child adopts from right when skinny left and fat right", func(t *testing.T) {
		leafE := uint64LeafFrom(nil, 50, 52, 54, 56, 58)
		leafD := uint64LeafFrom(leafE, 40, 42, 44, 46, 48)
		leafC := uint64LeafFrom(leafD, 30, 32, 34, 36, 38)
		leafB := uint64LeafFrom(leafC, 20, 22, 24, 26)
		leafA := uint64LeafFrom(leafB, 10, 12, 14, 16)

		child := uint64InternalFrom(leafA, leafB, leafC, leafD, leafE)

		tooSmall := child.deleteKey(4, 22)
		if got, want := tooSmall, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}

		ensureUint64Leaf(t, leafA, uint64LeafFrom(leafB, 10, 12, 14, 16))
		ensureUint64Leaf(t, leafB, uint64LeafFrom(leafC, 20, 24, 26, 30))
		ensureUint64Leaf(t, leafC, uint64LeafFrom(leafD, 32, 34, 36, 38))
		ensureUint64Leaf(t, leafD, uint64LeafFrom(leafE, 40, 42, 44, 46, 48))
		ensureUint64Leaf(t, leafE, uint64LeafFrom(nil, 50, 52, 54, 56, 58))
	})
	t.Run("child adopts from left when fat left and no right", func(t *testing.T) {
		leafE := uint64LeafFrom(nil, 50, 52, 54, 56)
		leafD := uint64LeafFrom(leafE, 40, 42, 44, 46, 48)
		leafC := uint64LeafFrom(leafD, 30, 32, 34, 36)
		leafB := uint64LeafFrom(leafC, 20, 22, 24, 26)
		leafA := uint64LeafFrom(leafB, 10, 12, 14, 16)

		child := uint64InternalFrom(leafA, leafB, leafC, leafD, leafE)

		tooSmall := child.deleteKey(4, 52)
		if got, want := tooSmall, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}

		ensureUint64Leaf(t, leafA, uint64LeafFrom(leafB, 10, 12, 14, 16))
		ensureUint64Leaf(t, leafB, uint64LeafFrom(leafC, 20, 22, 24, 26))
		ensureUint64Leaf(t, leafC, uint64LeafFrom(leafD, 30, 32, 34, 36))
		ensureUint64Leaf(t, leafD, uint64LeafFrom(leafE, 40, 42, 44, 46))
		ensureUint64Leaf(t, leafE, uint64LeafFrom(nil, 48, 50, 54, 56))
	})
	t.Run("child adopts from left when fat left and skinny right", func(t *testing.T) {
		leafE := uint64LeafFrom(nil, 50, 52, 54, 56)
		leafD := uint64LeafFrom(leafE, 40, 42, 44, 46)
		leafC := uint64LeafFrom(leafD, 30, 32, 34, 36)
		leafB := uint64LeafFrom(leafC, 20, 22, 24, 26, 28)
		leafA := uint64LeafFrom(leafB, 10, 12, 14, 16)

		child := uint64InternalFrom(leafA, leafB, leafC, leafD, leafE)

		tooSmall := child.deleteKey(4, 32)
		if got, want := tooSmall, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}

		ensureUint64Leaf(t, leafA, uint64LeafFrom(leafB, 10, 12, 14, 16))
		ensureUint64Leaf(t, leafB, uint64LeafFrom(leafC, 20, 22, 24, 26))
		ensureUint64Leaf(t, leafC, uint64LeafFrom(leafD, 28, 30, 34, 36))
		ensureUint64Leaf(t, leafD, uint64LeafFrom(leafE, 40, 42, 44, 46))
		ensureUint64Leaf(t, leafE, uint64LeafFrom(nil, 50, 52, 54, 56))
	})
	t.Run("child adopts from right when fat left and fat right", func(t *testing.T) {
		leafE := uint64LeafFrom(nil, 50, 52, 54, 56, 58)
		leafD := uint64LeafFrom(leafE, 40, 42, 44, 46, 48)
		leafC := uint64LeafFrom(leafD, 30, 32, 34, 36)
		leafB := uint64LeafFrom(leafC, 20, 22, 24, 26, 28)
		leafA := uint64LeafFrom(leafB, 10, 12, 14, 16, 18)

		child := uint64InternalFrom(leafA, leafB, leafC, leafD, leafE)

		tooSmall := child.deleteKey(4, 32)
		if got, want := tooSmall, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}

		ensureUint64Leaf(t, leafA, uint64LeafFrom(leafB, 10, 12, 14, 16, 18))
		ensureUint64Leaf(t, leafB, uint64LeafFrom(leafC, 20, 22, 24, 26, 28))
		ensureUint64Leaf(t, leafC, uint64LeafFrom(leafD, 30, 34, 36, 40))
		ensureUint64Leaf(t, leafD, uint64LeafFrom(leafE, 42, 44, 46, 48))
		ensureUint64Leaf(t, leafE, uint64LeafFrom(nil, 50, 52, 54, 56, 58))
	})
}

func TestUint64Delete(t *testing.T) {
	const order = 32
	const count = 1 << 10

	d, err := NewUint64Tree(order)
	if err != nil {
		t.Fatal(err)
	}

	randomizedValues := rand.Perm(count)

	for _, v := range randomizedValues {
		d.Insert(uint64(v), uint64(v))
	}

	for _, v := range randomizedValues {
		if _, ok := d.Search(uint64(v)); !ok {
			t.Fatalf("GOT: %v; WANT: %v", ok, true)
		}
	}

	for i := rand.Intn(10) + 5; i >= 0; i-- {
		for _, v := range randomizedValues {
			d.Delete(uint64(v))
		}
	}
}

func benchmarkUint64(b *testing.B, order, count int) {
	t, err := NewUint64Tree(order)
	if err != nil {
		b.Fatal(err)
	}

	randomizedValues := rand.Perm(count)

	b.ResetTimer()

	b.Run("insert", func(b *testing.B) {
		for _, v := range randomizedValues {
			t.Insert(uint64(v), uint64(v))
		}
	})

	b.Run("search", func(b *testing.B) {
		for _, v := range randomizedValues {
			if _, ok := t.Search(uint64(v)); !ok {
				b.Fatalf("GOT: %v; WANT: %v", ok, true)
			}
		}
	})

	b.Run("delete", func(b *testing.B) {
		for _, v := range randomizedValues {
			t.Delete(uint64(v))
		}
	})
}

func BenchmarkUint64Order16(b *testing.B) {
	const order = 16
	const count = 1 << 20
	benchmarkUint64(b, order, count)
}

func BenchmarkUint64Order32(b *testing.B) {
	const order = 32
	const count = 1 << 20
	benchmarkUint64(b, order, count)
}

func BenchmarkUint64Order64(b *testing.B) {
	const order = 64
	const count = 1 << 20
	benchmarkUint64(b, order, count)
}

func BenchmarkUint64Order128(b *testing.B) {
	const order = 128
	const count = 1 << 20
	benchmarkUint64(b, order, count)
}

func BenchmarkUint64Order256(b *testing.B) {
	const order = 256
	const count = 1 << 20
	benchmarkUint64(b, order, count)
}

func BenchmarkUint64Order512(b *testing.B) {
	const order = 512
	const count = 1 << 20
	benchmarkUint64(b, order, count)
}
