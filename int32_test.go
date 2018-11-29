package gobptree

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestInt32BinarySearch(t *testing.T) {
	t.Run("greater than or equal to", func(t *testing.T) {
		t.Run("empty list", func(t *testing.T) {
			i := int32SearchGreaterThanOrEqualTo(1, nil)
			if got, want := i, 0; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
		})
		t.Run("single item list", func(t *testing.T) {
			t.Run("key before", func(t *testing.T) {
				i := int32SearchGreaterThanOrEqualTo(1, []int32{2})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match", func(t *testing.T) {
				i := int32SearchGreaterThanOrEqualTo(2, []int32{2})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key after", func(t *testing.T) {
				i := int32SearchGreaterThanOrEqualTo(3, []int32{2})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
		})
		t.Run("multiple item list", func(t *testing.T) {
			t.Run("key before first", func(t *testing.T) {
				i := int32SearchGreaterThanOrEqualTo(1, []int32{2, 4, 6})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match first", func(t *testing.T) {
				i := int32SearchGreaterThanOrEqualTo(2, []int32{2, 4, 6})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key between first and second", func(t *testing.T) {
				i := int32SearchGreaterThanOrEqualTo(3, []int32{2, 4, 6})
				if got, want := i, 1; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match second", func(t *testing.T) {
				i := int32SearchGreaterThanOrEqualTo(4, []int32{2, 4, 6})
				if got, want := i, 1; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key between second and third", func(t *testing.T) {
				i := int32SearchGreaterThanOrEqualTo(5, []int32{2, 4, 6})
				if got, want := i, 2; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match third", func(t *testing.T) {
				i := int32SearchGreaterThanOrEqualTo(6, []int32{2, 4, 6})
				if got, want := i, 2; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key after third", func(t *testing.T) {
				i := int32SearchGreaterThanOrEqualTo(7, []int32{2, 4, 6})
				if got, want := i, 2; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
		})
	})
	t.Run("less than or equal to", func(t *testing.T) {
		t.Run("empty list", func(t *testing.T) {
			i := int32SearchLessThanOrEqualTo(1, nil)
			if got, want := i, 0; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
		})
		t.Run("single item list", func(t *testing.T) {
			t.Run("key before", func(t *testing.T) {
				i := int32SearchLessThanOrEqualTo(1, []int32{2})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match", func(t *testing.T) {
				i := int32SearchLessThanOrEqualTo(int32(2), []int32{2})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key after", func(t *testing.T) {
				i := int32SearchLessThanOrEqualTo(3, []int32{2})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
		})
		t.Run("multiple item list", func(t *testing.T) {
			t.Run("key before first", func(t *testing.T) {
				i := int32SearchLessThanOrEqualTo(1, []int32{2, 4, 6})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match first", func(t *testing.T) {
				i := int32SearchLessThanOrEqualTo(int32(2), []int32{2, 4, 6})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key between first and second", func(t *testing.T) {
				i := int32SearchLessThanOrEqualTo(3, []int32{2, 4, 6})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match second", func(t *testing.T) {
				i := int32SearchLessThanOrEqualTo(4, []int32{2, 4, 6})
				if got, want := i, 1; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key between second and third", func(t *testing.T) {
				i := int32SearchLessThanOrEqualTo(5, []int32{2, 4, 6})
				if got, want := i, 1; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match third", func(t *testing.T) {
				i := int32SearchLessThanOrEqualTo(6, []int32{2, 4, 6})
				if got, want := i, 2; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key after third", func(t *testing.T) {
				i := int32SearchLessThanOrEqualTo(7, []int32{2, 4, 6})
				if got, want := i, 2; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
		})
	})
}

func TestNewInt32TreeReturnsErrorWhenInvalidOrder(t *testing.T) {
	for _, v := range []int{0, -1, 1, 3, 11} {
		_, err := NewInt32Tree(v)
		if err == nil {
			t.Errorf("GOT: %v; WANT: %v", err, fmt.Sprintf("power of 2: %d", v))
		}
	}
}

func int32LeafFrom(next *int32LeafNode, items ...int32) *int32LeafNode {
	n := &int32LeafNode{
		runts:  make([]int32, len(items)),
		values: make([]interface{}, len(items)),
		next:   next,
	}
	for i := 0; i < len(items); i++ {
		n.runts[i] = items[i]
		n.values[i] = items[i]
	}
	return n
}

func int32InternalFrom(items ...int32Node) *int32InternalNode {
	n := &int32InternalNode{
		runts:    make([]int32, len(items)),
		children: make([]int32Node, len(items)),
	}
	for i := 0; i < len(items); i++ {
		n.runts[i] = items[i].smallest()
		n.children[i] = items[i]
	}
	return n
}

////////////////////////////////////////

func ensureInt32Leaf(t *testing.T, actual, expected *int32LeafNode) {
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
	// ensureInt32Leaf(t, actual.next, expected.next)
	if got, want := actual.next, expected.next; got != want {
		t.Errorf("next GOT: %v; WANT: %v", got, want)
	}
	if t.Failed() {
		t.Errorf("\nGOT:\n\t%#v\nWANT:\n\t%#v", actual, expected)
	}
}

func ensureInt32Internal(t *testing.T, a, e *int32InternalNode) {
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
		ensureInt32Node(t, a.children[i], e.children[i])
	}
}

func ensureInt32Node(t *testing.T, actual, expected int32Node) {
	t.Helper()

	switch e := expected.(type) {
	case *int32LeafNode:
		a, ok := actual.(*int32LeafNode)
		if got, want := ok, true; got != want {
			t.Errorf("GOT: %T; WANT: %T", actual, e)
		}
		ensureInt32Leaf(t, a, e)
	case *int32InternalNode:
		a, ok := actual.(*int32InternalNode)
		if got, want := ok, true; got != want {
			t.Errorf("GOT: %T; WANT: %T", actual, e)
		}
		ensureInt32Internal(t, a, e)
	default:
		t.Errorf("GOT: %T; WANT: int32Node", expected)
	}
}

////////////////////////////////////////

func TestInt32InternalNodeMaybeSplit(t *testing.T) {
	leafD := int32LeafFrom(nil, 40, 41, 42, 43)
	leafC := int32LeafFrom(leafD, 30, 31, 32, 33)
	leafB := int32LeafFrom(leafC, 20, 21, 22, 23)
	leafA := int32LeafFrom(leafB, 10, 11, 12, 13)

	ni := int32InternalFrom(leafA, leafB, leafC, leafD)

	t.Run("does nothing when not full", func(t *testing.T) {
		_, right := ni.maybeSplit(6)
		if right != nil {
			t.Errorf("GOT: %v; WANT: %v", right, nil)
		}
	})

	t.Run("splits when full", func(t *testing.T) {
		expectedLeft := int32InternalFrom(leafA, leafB)
		expectedRight := int32InternalFrom(leafC, leafD)

		leftNode, rightNode := ni.maybeSplit(4)

		ensureInt32Node(t, leftNode, expectedLeft)
		ensureInt32Node(t, rightNode, expectedRight)
	})
}

func TestInternalInt32NodeInsertSmallerKey(t *testing.T) {
	gimme := func() (*int32LeafNode, *int32LeafNode) {
		leafB := int32LeafFrom(nil, 21, 22)
		leafA := int32LeafFrom(leafB, 12, 13)
		return leafA, leafB
	}

	leafA, leafB := gimme()
	ni := int32InternalFrom(leafA, leafB)

	d := &Int32Tree{root: ni, order: 4}

	d.Insert(11, 11)

	if got, want := ni.runts[0], int32(11); got != want {
		t.Fatalf("GOT: %v; WANT: %v", got, want)
	}
}

func TestInt32LeafNodeMaybeSplit(t *testing.T) {
	gimme := func() (*int32LeafNode, *int32LeafNode) {
		leafB := int32LeafFrom(nil, 21, 22, 23, 24)
		leafA := int32LeafFrom(leafB, 11, 12, 13, 14)
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
		ensureInt32Node(t, leftNode, int32LeafFrom(rightNode.(*int32LeafNode), 11, 12))
		ensureInt32Node(t, rightNode, int32LeafFrom(leafB, 13, 14))
	})
	t.Run("splits right edge when full", func(t *testing.T) {
		leafA, leafB := gimme()
		leftNode, rightNode := leafB.maybeSplit(4)
		if got, want := leafA.next, leftNode; got != want {
			t.Fatalf("GOT: %v; WANT: %v", got, want)
		}
		ensureInt32Node(t, leftNode, int32LeafFrom(rightNode.(*int32LeafNode), 21, 22))
		ensureInt32Node(t, rightNode, int32LeafFrom(nil, 23, 24))
	})
}

func TestInsertIntoSingleLeafInt32Tree(t *testing.T) {
	t.Run("when fewer than order elements", func(t *testing.T) {
		t.Run("when empty", func(t *testing.T) {
			d, _ := NewInt32Tree(4)
			nl, ok := d.root.(*int32LeafNode)
			if !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, false)
			}
			d.Insert(int32(30), int32(30))
			ensureInt32Leaf(t, nl, int32LeafFrom(nil, 30))
		})
		t.Run("when less than first runt", func(t *testing.T) {
			d, _ := NewInt32Tree(4)
			nl, ok := d.root.(*int32LeafNode)
			if !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, false)
			}
			d.Insert(int32(30), int32(30))
			d.Insert(int32(10), int32(10))
			ensureInt32Node(t, nl, int32LeafFrom(nil, 10, 30))
		})
		t.Run("when update value", func(t *testing.T) {
			d, _ := NewInt32Tree(4)
			nl, ok := d.root.(*int32LeafNode)
			if !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, false)
			}
			d.Insert(int32(30), int32(30))
			d.Insert(int32(10), int32(10))
			d.Insert(int32(30), int32(333))
			ensureInt32Node(t, nl, &int32LeafNode{
				runts:  []int32{10, 30},
				values: []interface{}{int32(10), int32(333)},
			})
		})
		t.Run("when between first and final runt", func(t *testing.T) {
			d, _ := NewInt32Tree(4)
			nl, ok := d.root.(*int32LeafNode)
			if !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, false)
			}
			d.Insert(int32(30), int32(30))
			d.Insert(int32(10), int32(10))
			d.Insert(int32(20), int32(20))
			ensureInt32Node(t, nl, int32LeafFrom(nil, 10, 20, 30))
		})
		t.Run("when after final runt", func(t *testing.T) {
			d, _ := NewInt32Tree(4)
			nl, ok := d.root.(*int32LeafNode)
			if !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, false)
			}
			d.Insert(int32(30), int32(30))
			d.Insert(int32(10), int32(10))
			d.Insert(int32(20), int32(20))
			d.Insert(int32(40), int32(40))
			ensureInt32Node(t, nl, int32LeafFrom(nil, 10, 20, 30, 40))
		})
	})

	t.Run("when insertion splits single leaf node", func(t *testing.T) {
		gimme := func() *Int32Tree {
			d, _ := NewInt32Tree(4)
			for _, v := range []int32{10, 20, 30, 40} {
				d.Insert(int32(v), int32(v))
			}
			return d
		}
		t.Run("when new key will be first node in left leaf", func(t *testing.T) {
			d := gimme()
			d.Insert(int32(0), int32(0))

			root, ok := d.root.(*int32InternalNode)
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
			if got, want := root.runts[0], int32(0); got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			ensureInt32Node(t, root.children[0], int32LeafFrom(root.children[1].(*int32LeafNode), 0, 10, 20))

			if got, want := root.runts[1], int32(30); got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			ensureInt32Node(t, root.children[1], int32LeafFrom(nil, 30, 40))
		})
		t.Run("when new key is in middle", func(t *testing.T) {
			d := gimme()
			d.Insert(int32(25), int32(25))
			root, ok := d.root.(*int32InternalNode)
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
			if got, want := root.runts[0], int32(10); got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			ensureInt32Node(t, root.children[0], int32LeafFrom(root.children[1].(*int32LeafNode), 10, 20, 25))

			if got, want := root.runts[1], int32(30); got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			ensureInt32Node(t, root.children[1], int32LeafFrom(nil, 30, 40))
		})
		t.Run("when new key will be final node in right leaf", func(t *testing.T) {
			d := gimme()
			d.Insert(int32(50), int32(50))
			root, ok := d.root.(*int32InternalNode)
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
			if got, want := root.runts[0], int32(10); got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			ensureInt32Node(t, root.children[0], int32LeafFrom(root.children[1].(*int32LeafNode), 10, 20))

			if got, want := root.runts[1], int32(30); got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			ensureInt32Node(t, root.children[1], int32LeafFrom(nil, 30, 40, 50))
		})
	})
}

func TestInt32TreeSearch(t *testing.T) {
	t.Run("empty tree", func(t *testing.T) {
		d, _ := NewInt32Tree(16)

		_, ok := d.Search(13)
		if got, want := ok, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
	})
	t.Run("single-leaf tree", func(t *testing.T) {
		t.Run("missing value", func(t *testing.T) {
			d, _ := NewInt32Tree(16)
			for i := int32(0); i < 15; i++ {
				if i != 13 {
					d.Insert(int32(i), int32(i))
				}
			}

			_, ok := d.Search(13)
			if got, want := ok, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
		})
		t.Run("existing value", func(t *testing.T) {
			d, _ := NewInt32Tree(16)
			for i := int32(0); i < 15; i++ {
				if i != 13 {
					d.Insert(int32(i), int32(i))
				}
			}

			value, ok := d.Search(8)
			if got, want := ok, true; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := value, int32(8); got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
		})
	})
	t.Run("multi-leaf tree", func(t *testing.T) {
		t.Run("missing value", func(t *testing.T) {
			d, _ := NewInt32Tree(4)
			for i := int32(0); i < 15; i++ {
				if i != 13 {
					d.Insert(int32(i), int32(i))
				}
			}

			_, ok := d.Search(13)
			if got, want := ok, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
		})
		t.Run("existing value", func(t *testing.T) {
			d, _ := NewInt32Tree(4)
			for i := int32(0); i < 15; i++ {
				if i != 13 {
					d.Insert(int32(i), int32(i))
				}
			}

			value, ok := d.Search(8)
			if got, want := ok, true; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := value, int32(8); got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
		})
	})
}

func TestInt32TreeCursor(t *testing.T) {
	t.Run("empty tree", func(t *testing.T) {
		var count int

		d, _ := NewInt32Tree(4)
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
			var values []int32

			d, _ := NewInt32Tree(16)
			for i := int32(0); i < 15; i++ {
				d.Insert(int32(i), int32(i))
			}

			c := d.NewScanner(0)
			for c.Scan() {
				_, v := c.Pair()
				values = append(values, v.(int32))
			}

			expected := []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}

			for i := 0; i < len(values) && i < len(expected); i++ {
				if got, want := values[i], expected[i]; got != want {
					t.Errorf("GOT: %v; WANT: %v", got, want)
				}
			}
		})
		t.Run("scan for missing element", func(t *testing.T) {
			var values []int32

			d, _ := NewInt32Tree(16)
			for i := int32(0); i < 15; i++ {
				if i != 13 {
					d.Insert(int32(i), int32(i))
				}
			}

			c := d.NewScanner(13)
			for c.Scan() {
				_, v := c.Pair()
				values = append(values, v.(int32))
			}

			expected := []int32{14, 2, 3, 4, 5, 6, 7, 8, 9}

			for i := 0; i < len(values) && i < len(expected); i++ {
				if got, want := values[i], expected[i]; got != want {
					t.Errorf("GOT: %v; WANT: %v", got, want)
				}
			}
		})
		t.Run("scan for existing element", func(t *testing.T) {
			var values []int32

			d, _ := NewInt32Tree(16)
			for i := int32(0); i < 15; i++ {
				d.Insert(int32(i), int32(i))
			}

			c := d.NewScanner(13)
			for c.Scan() {
				_, v := c.Pair()
				values = append(values, v.(int32))
			}

			expected := []int32{13, 14, 2, 3, 4, 5, 6, 7, 8, 9}

			for i := 0; i < len(values) && i < len(expected); i++ {
				if got, want := values[i], expected[i]; got != want {
					t.Errorf("GOT: %v; WANT: %v", got, want)
				}
			}
		})
	})
	t.Run("multi-leaf tree", func(t *testing.T) {
		var values []int32

		d, _ := NewInt32Tree(4)
		for i := int32(0); i < 15; i++ {
			d.Insert(int32(i), int32(i))
		}

		c := d.NewScanner(0)
		for c.Scan() {
			_, v := c.Pair()
			values = append(values, v.(int32))
		}

		expected := []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}

		for i := 0; i < len(values) && i < len(expected); i++ {
			if got, want := values[i], expected[i]; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
		}
	})
}

func TestInt32TreeUpdate(t *testing.T) {
	d, _ := NewInt32Tree(8)
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
	d.Insert(int32(3), int32(3))
	d.Update(int32(2), func(value interface{}, ok bool) interface{} {
		if got, want := ok, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := value, error(nil); got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		return "fourth"
	})
	value, ok = d.Search(int32(2))
	if got, want := ok, true; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := value, "fourth"; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
}

func TestInt32LeafNodeDelete(t *testing.T) {
	t.Run("still big enough", func(t *testing.T) {
		t.Run("key is missing", func(t *testing.T) {
			l := &int32LeafNode{
				runts:  []int32{11, 21, 31},
				values: []interface{}{11, 21, 31},
			}
			tooSmall := l.deleteKey(2, 42)
			if got, want := tooSmall, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureInt32Node(t, l, &int32LeafNode{
				runts:  []int32{11, 21, 31},
				values: []interface{}{11, 21, 31},
			})
		})
		t.Run("key is first", func(t *testing.T) {
			l := &int32LeafNode{
				runts:  []int32{11, 21, 31},
				values: []interface{}{11, 21, 31},
			}
			tooSmall := l.deleteKey(2, 11)
			if got, want := tooSmall, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureInt32Node(t, l, &int32LeafNode{
				runts:  []int32{21, 31},
				values: []interface{}{21, 31},
			})
		})
		t.Run("key is middle", func(t *testing.T) {
			l := &int32LeafNode{
				runts:  []int32{11, 21, 31},
				values: []interface{}{11, 21, 31},
			}
			tooSmall := l.deleteKey(2, 21)
			if got, want := tooSmall, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureInt32Node(t, l, &int32LeafNode{
				runts:  []int32{11, 31},
				values: []interface{}{11, 31},
			})
		})
		t.Run("key is last", func(t *testing.T) {
			l := &int32LeafNode{
				runts:  []int32{11, 21, 31},
				values: []interface{}{11, 21, 31},
			}
			tooSmall := l.deleteKey(2, 31)
			if got, want := tooSmall, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureInt32Node(t, l, &int32LeafNode{
				runts:  []int32{11, 21},
				values: []interface{}{11, 21},
			})
		})
	})
	t.Run("will be too small", func(t *testing.T) {
		l := int32LeafFrom(nil, 11, 21, 31, 41)
		tooSmall := l.deleteKey(4, 21)
		if got, want := tooSmall, true; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		ensureInt32Node(t, l, int32LeafFrom(nil, 11, 31, 41))
	})
}

func TestInt32LeafNodeAdoptFrom(t *testing.T) {
	t.Run("left", func(t *testing.T) {
		r := int32LeafFrom(nil, 5, 6, 7)
		l := int32LeafFrom(r, 0, 1, 2, 3, 4)

		r.adoptFromLeft(l)

		ensureInt32Node(t, l, int32LeafFrom(r, 0, 1, 2, 3))
		ensureInt32Node(t, r, int32LeafFrom(nil, 4, 5, 6, 7))
	})
	t.Run("right", func(t *testing.T) {
		r := int32LeafFrom(nil, 3, 4, 5, 6, 7)
		l := int32LeafFrom(r, 0, 1, 2)

		l.adoptFromRight(r)

		ensureInt32Node(t, l, int32LeafFrom(r, 0, 1, 2, 3))
		ensureInt32Node(t, r, int32LeafFrom(nil, 4, 5, 6, 7))
	})
}

func TestInt32InternalNodeAdoptFrom(t *testing.T) {
	t.Run("left", func(t *testing.T) {
		leafI := int32LeafFrom(nil, 90, 92, 94, 96, 98)
		leafH := int32LeafFrom(leafI, 80, 82, 84, 86, 88)
		leafG := int32LeafFrom(leafH, 70, 72, 74, 76, 78)
		leafF := int32LeafFrom(leafG, 60, 62, 32, 66, 68)
		leafE := int32LeafFrom(leafF, 50, 52, 54, 56, 58)
		leafD := int32LeafFrom(leafE, 40, 42, 44, 46, 48)
		leafC := int32LeafFrom(leafD, 30, 32, 34, 36, 38)
		leafB := int32LeafFrom(leafC, 20, 22, 24, 26, 28)
		leafA := int32LeafFrom(leafB, 10, 12, 14, 16, 18)

		left := int32InternalFrom(leafA, leafB, leafC, leafD, leafE, leafF)
		right := int32InternalFrom(leafG, leafH, leafI)

		right.adoptFromLeft(left)

		ensureInt32Internal(t, left, int32InternalFrom(leafA, leafB, leafC, leafD, leafE))
		ensureInt32Internal(t, right, int32InternalFrom(leafF, leafG, leafH, leafI))
	})
	t.Run("right", func(t *testing.T) {
		leafI := int32LeafFrom(nil, 90, 92, 94, 96, 98)
		leafH := int32LeafFrom(leafI, 80, 82, 84, 86, 88)
		leafG := int32LeafFrom(leafH, 70, 72, 74, 76, 78)
		leafF := int32LeafFrom(leafG, 60, 62, 32, 66, 68)
		leafE := int32LeafFrom(leafF, 50, 52, 54, 56, 58)
		leafD := int32LeafFrom(leafE, 40, 42, 44, 46, 48)
		leafC := int32LeafFrom(leafD, 30, 32, 34, 36, 38)
		leafB := int32LeafFrom(leafC, 20, 22, 24, 26, 28)
		leafA := int32LeafFrom(leafB, 10, 12, 14, 16, 18)

		left := int32InternalFrom(leafA, leafB, leafC)
		right := int32InternalFrom(leafD, leafE, leafF, leafG, leafH, leafI)

		left.adoptFromRight(right)

		ensureInt32Internal(t, left, int32InternalFrom(leafA, leafB, leafC, leafD))
		ensureInt32Internal(t, right, int32InternalFrom(leafE, leafF, leafG, leafH, leafI))
	})
}

func TestInt32LeafNodeMergeWithRight(t *testing.T) {
	leafC := int32LeafFrom(nil, 6, 7, 8, 9)
	leafB := int32LeafFrom(leafC, 3, 4, 5)
	leafA := int32LeafFrom(leafB, 0, 1, 2)

	leafA.absorbRight(leafB)

	ensureInt32Node(t, leafA, int32LeafFrom(leafC, 0, 1, 2, 3, 4, 5))

	if got, want := len(leafB.runts), 0; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := len(leafB.values), 0; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := leafB.next, (*int32LeafNode)(nil); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
}

func TestInt32InternalNodeMergeWithRight(t *testing.T) {
	leafI := int32LeafFrom(nil, 90, 92, 94, 96, 98)
	leafH := int32LeafFrom(leafI, 80, 82, 84, 86, 88)
	leafG := int32LeafFrom(leafH, 70, 72, 74, 76, 78)
	leafF := int32LeafFrom(leafG, 60, 62, 32, 66, 68)
	leafE := int32LeafFrom(leafF, 50, 52, 54, 56, 58)
	leafD := int32LeafFrom(leafE, 40, 42, 44, 46, 48)
	leafC := int32LeafFrom(leafD, 30, 32, 34, 36, 38)
	leafB := int32LeafFrom(leafC, 20, 22, 24, 26, 28)
	leafA := int32LeafFrom(leafB, 10, 12, 14, 16, 18)

	left := int32InternalFrom(leafA, leafB, leafC)
	right := int32InternalFrom(leafD, leafE, leafF, leafG)

	left.absorbRight(right)

	ensureInt32Internal(t, left, int32InternalFrom(leafA, leafB, leafC, leafD, leafE, leafF, leafG))

	if got, want := len(right.runts), 0; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := len(right.children), 0; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
}

func TestInt32InternalNodeDeleteKey(t *testing.T) {
	t.Run("not too small", func(t *testing.T) {
		leafE := int32LeafFrom(nil, 50, 52, 54, 56, 58)
		leafD := int32LeafFrom(leafE, 40, 42, 44, 46, 48)
		leafC := int32LeafFrom(leafD, 30, 32, 34, 36, 38)
		leafB := int32LeafFrom(leafC, 20, 22, 24, 26, 28)
		leafA := int32LeafFrom(leafB, 10, 12, 14, 16, 18)

		child := int32InternalFrom(leafA, leafB, leafC, leafD)

		if got, want := child.deleteKey(4, 22), false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
	})
	t.Run("child absorbs right when no left and skinny right", func(t *testing.T) {
		t.Run("child not too small", func(t *testing.T) {
			leafE := int32LeafFrom(nil, 50, 52, 54, 56, 58)
			leafD := int32LeafFrom(leafE, 40, 42, 44, 46, 48)
			leafC := int32LeafFrom(leafD, 30, 32, 34, 36, 38)
			leafB := int32LeafFrom(leafC, 20, 22, 24, 26)
			leafA := int32LeafFrom(leafB, 10, 12, 14, 16)

			child := int32InternalFrom(leafA, leafB, leafC, leafD, leafE)

			tooSmall := child.deleteKey(4, 12)
			if got, want := tooSmall, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}

			ensureInt32Leaf(t, leafA, int32LeafFrom(leafC, 10, 14, 16, 20, 22, 24, 26))
			if got, want := len(leafB.runts), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := len(leafB.values), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureInt32Leaf(t, leafC, int32LeafFrom(leafD, 30, 32, 34, 36, 38))
			ensureInt32Leaf(t, leafD, int32LeafFrom(leafE, 40, 42, 44, 46, 48))
			ensureInt32Leaf(t, leafE, int32LeafFrom(nil, 50, 52, 54, 56, 58))
		})
		t.Run("child too small", func(t *testing.T) {
			leafD := int32LeafFrom(nil, 40, 42, 44, 46, 48)
			leafC := int32LeafFrom(leafD, 30, 32, 34, 36, 38)
			leafB := int32LeafFrom(leafC, 20, 22, 24, 26)
			leafA := int32LeafFrom(leafB, 10, 12, 14, 16)

			child := int32InternalFrom(leafA, leafB, leafC, leafD)

			tooSmall := child.deleteKey(4, 12)
			if got, want := tooSmall, true; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}

			ensureInt32Leaf(t, leafA, int32LeafFrom(leafC, 10, 14, 16, 20, 22, 24, 26))
			if got, want := len(leafB.runts), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := len(leafB.values), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureInt32Leaf(t, leafC, int32LeafFrom(leafD, 30, 32, 34, 36, 38))
			ensureInt32Leaf(t, leafD, int32LeafFrom(nil, 40, 42, 44, 46, 48))
		})
	})
	t.Run("child adopts from right when no left and fat right", func(t *testing.T) {
		leafE := int32LeafFrom(nil, 50, 52, 54, 56, 58)
		leafD := int32LeafFrom(leafE, 40, 42, 44, 46, 48)
		leafC := int32LeafFrom(leafD, 30, 32, 34, 36, 38)
		leafB := int32LeafFrom(leafC, 20, 22, 24, 26, 28)
		leafA := int32LeafFrom(leafB, 10, 12, 14, 16)

		child := int32InternalFrom(leafA, leafB, leafC, leafD, leafE)

		tooSmall := child.deleteKey(4, 12)
		if got, want := tooSmall, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}

		ensureInt32Leaf(t, leafA, int32LeafFrom(leafB, 10, 14, 16, 20))
		ensureInt32Leaf(t, leafB, int32LeafFrom(leafC, 22, 24, 26, 28))
		ensureInt32Leaf(t, leafC, int32LeafFrom(leafD, 30, 32, 34, 36, 38))
		ensureInt32Leaf(t, leafD, int32LeafFrom(leafE, 40, 42, 44, 46, 48))
		ensureInt32Leaf(t, leafE, int32LeafFrom(nil, 50, 52, 54, 56, 58))
	})
	t.Run("left absorbs child when skinny left and no right", func(t *testing.T) {
		t.Run("too small", func(t *testing.T) {
			leafD := int32LeafFrom(nil, 40, 42, 44, 46)
			leafC := int32LeafFrom(leafD, 30, 32, 34, 36)
			leafB := int32LeafFrom(leafC, 20, 22, 24, 26)
			leafA := int32LeafFrom(leafB, 10, 12, 14, 16)

			child := int32InternalFrom(leafA, leafB, leafC, leafD)

			tooSmall := child.deleteKey(4, 42)
			if got, want := tooSmall, true; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}

			ensureInt32Leaf(t, leafA, int32LeafFrom(leafB, 10, 12, 14, 16))
			ensureInt32Leaf(t, leafB, int32LeafFrom(leafC, 20, 22, 24, 26))
			ensureInt32Leaf(t, leafC, int32LeafFrom(nil, 30, 32, 34, 36, 40, 44, 46))
			if got, want := len(leafD.runts), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := len(leafD.values), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
		})
		t.Run("not too small", func(t *testing.T) {
			leafE := int32LeafFrom(nil, 50, 52, 54, 56)
			leafD := int32LeafFrom(leafE, 40, 42, 44, 46)
			leafC := int32LeafFrom(leafD, 30, 32, 34, 36)
			leafB := int32LeafFrom(leafC, 20, 22, 24, 26)
			leafA := int32LeafFrom(leafB, 10, 12, 14, 16)

			child := int32InternalFrom(leafA, leafB, leafC, leafD, leafE)

			tooSmall := child.deleteKey(4, 52)
			if got, want := tooSmall, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}

			ensureInt32Leaf(t, leafA, int32LeafFrom(leafB, 10, 12, 14, 16))
			ensureInt32Leaf(t, leafB, int32LeafFrom(leafC, 20, 22, 24, 26))
			ensureInt32Leaf(t, leafC, int32LeafFrom(leafD, 30, 32, 34, 36))
			ensureInt32Leaf(t, leafD, int32LeafFrom(nil, 40, 42, 44, 46, 50, 54, 56))
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
			leafC := int32LeafFrom(nil, 30, 32, 34, 36)
			leafB := int32LeafFrom(leafC, 20, 22, 24, 26)
			leafA := int32LeafFrom(leafB, 10, 12, 14, 16)

			child := int32InternalFrom(leafA, leafB, leafC)

			tooSmall := child.deleteKey(4, 22)
			if got, want := tooSmall, true; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}

			ensureInt32Leaf(t, leafA, int32LeafFrom(leafC, 10, 12, 14, 16, 20, 24, 26))
			if got, want := len(leafB.runts), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := len(leafB.values), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureInt32Leaf(t, leafC, int32LeafFrom(nil, 30, 32, 34, 36))
		})
		t.Run("not too small", func(t *testing.T) {
			leafE := int32LeafFrom(nil, 50, 52, 54, 56)
			leafD := int32LeafFrom(leafE, 40, 42, 44, 46)
			leafC := int32LeafFrom(leafD, 30, 32, 34, 36)
			leafB := int32LeafFrom(leafC, 20, 22, 24, 26)
			leafA := int32LeafFrom(leafB, 10, 12, 14, 16)

			child := int32InternalFrom(leafA, leafB, leafC, leafD, leafE)

			tooSmall := child.deleteKey(4, 22)
			if got, want := tooSmall, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}

			ensureInt32Leaf(t, leafA, int32LeafFrom(leafC, 10, 12, 14, 16, 20, 24, 26))
			if got, want := len(leafB.runts), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := len(leafB.values), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureInt32Leaf(t, leafC, int32LeafFrom(leafD, 30, 32, 34, 36))
			ensureInt32Leaf(t, leafD, int32LeafFrom(leafE, 40, 42, 44, 46))
		})
	})
	t.Run("child adopts from right when skinny left and fat right", func(t *testing.T) {
		leafE := int32LeafFrom(nil, 50, 52, 54, 56, 58)
		leafD := int32LeafFrom(leafE, 40, 42, 44, 46, 48)
		leafC := int32LeafFrom(leafD, 30, 32, 34, 36, 38)
		leafB := int32LeafFrom(leafC, 20, 22, 24, 26)
		leafA := int32LeafFrom(leafB, 10, 12, 14, 16)

		child := int32InternalFrom(leafA, leafB, leafC, leafD, leafE)

		tooSmall := child.deleteKey(4, 22)
		if got, want := tooSmall, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}

		ensureInt32Leaf(t, leafA, int32LeafFrom(leafB, 10, 12, 14, 16))
		ensureInt32Leaf(t, leafB, int32LeafFrom(leafC, 20, 24, 26, 30))
		ensureInt32Leaf(t, leafC, int32LeafFrom(leafD, 32, 34, 36, 38))
		ensureInt32Leaf(t, leafD, int32LeafFrom(leafE, 40, 42, 44, 46, 48))
		ensureInt32Leaf(t, leafE, int32LeafFrom(nil, 50, 52, 54, 56, 58))
	})
	t.Run("child adopts from left when fat left and no right", func(t *testing.T) {
		leafE := int32LeafFrom(nil, 50, 52, 54, 56)
		leafD := int32LeafFrom(leafE, 40, 42, 44, 46, 48)
		leafC := int32LeafFrom(leafD, 30, 32, 34, 36)
		leafB := int32LeafFrom(leafC, 20, 22, 24, 26)
		leafA := int32LeafFrom(leafB, 10, 12, 14, 16)

		child := int32InternalFrom(leafA, leafB, leafC, leafD, leafE)

		tooSmall := child.deleteKey(4, 52)
		if got, want := tooSmall, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}

		ensureInt32Leaf(t, leafA, int32LeafFrom(leafB, 10, 12, 14, 16))
		ensureInt32Leaf(t, leafB, int32LeafFrom(leafC, 20, 22, 24, 26))
		ensureInt32Leaf(t, leafC, int32LeafFrom(leafD, 30, 32, 34, 36))
		ensureInt32Leaf(t, leafD, int32LeafFrom(leafE, 40, 42, 44, 46))
		ensureInt32Leaf(t, leafE, int32LeafFrom(nil, 48, 50, 54, 56))
	})
	t.Run("child adopts from left when fat left and skinny right", func(t *testing.T) {
		leafE := int32LeafFrom(nil, 50, 52, 54, 56)
		leafD := int32LeafFrom(leafE, 40, 42, 44, 46)
		leafC := int32LeafFrom(leafD, 30, 32, 34, 36)
		leafB := int32LeafFrom(leafC, 20, 22, 24, 26, 28)
		leafA := int32LeafFrom(leafB, 10, 12, 14, 16)

		child := int32InternalFrom(leafA, leafB, leafC, leafD, leafE)

		tooSmall := child.deleteKey(4, 32)
		if got, want := tooSmall, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}

		ensureInt32Leaf(t, leafA, int32LeafFrom(leafB, 10, 12, 14, 16))
		ensureInt32Leaf(t, leafB, int32LeafFrom(leafC, 20, 22, 24, 26))
		ensureInt32Leaf(t, leafC, int32LeafFrom(leafD, 28, 30, 34, 36))
		ensureInt32Leaf(t, leafD, int32LeafFrom(leafE, 40, 42, 44, 46))
		ensureInt32Leaf(t, leafE, int32LeafFrom(nil, 50, 52, 54, 56))
	})
	t.Run("child adopts from right when fat left and fat right", func(t *testing.T) {
		leafE := int32LeafFrom(nil, 50, 52, 54, 56, 58)
		leafD := int32LeafFrom(leafE, 40, 42, 44, 46, 48)
		leafC := int32LeafFrom(leafD, 30, 32, 34, 36)
		leafB := int32LeafFrom(leafC, 20, 22, 24, 26, 28)
		leafA := int32LeafFrom(leafB, 10, 12, 14, 16, 18)

		child := int32InternalFrom(leafA, leafB, leafC, leafD, leafE)

		tooSmall := child.deleteKey(4, 32)
		if got, want := tooSmall, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}

		ensureInt32Leaf(t, leafA, int32LeafFrom(leafB, 10, 12, 14, 16, 18))
		ensureInt32Leaf(t, leafB, int32LeafFrom(leafC, 20, 22, 24, 26, 28))
		ensureInt32Leaf(t, leafC, int32LeafFrom(leafD, 30, 34, 36, 40))
		ensureInt32Leaf(t, leafD, int32LeafFrom(leafE, 42, 44, 46, 48))
		ensureInt32Leaf(t, leafE, int32LeafFrom(nil, 50, 52, 54, 56, 58))
	})
}

func TestInt32Delete(t *testing.T) {
	const order = 32
	const count = 1 << 10

	d, err := NewInt32Tree(order)
	if err != nil {
		t.Fatal(err)
	}

	randomizedValues := rand.Perm(count)

	for _, v := range randomizedValues {
		d.Insert(int32(v), int32(v))
	}

	for _, v := range randomizedValues {
		if _, ok := d.Search(int32(v)); !ok {
			t.Fatalf("GOT: %v; WANT: %v", ok, true)
		}
	}

	for _, v := range randomizedValues {
		d.Delete(int32(v))
	}

	t.Run("empty", func(t *testing.T) {
		d.Delete(int32(13))
	})
}
