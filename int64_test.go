package gobptree

import (
	"fmt"
	"testing"
)

func TestInt64BinarySearch(t *testing.T) {
	t.Run("greater than or equal to", func(t *testing.T) {
		t.Run("empty list", func(t *testing.T) {
			i := int64SearchGreaterThanOrEqualTo(1, nil)
			if got, want := i, 0; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
		})
		t.Run("single item list", func(t *testing.T) {
			t.Run("key before", func(t *testing.T) {
				i := int64SearchGreaterThanOrEqualTo(1, []int64{2})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match", func(t *testing.T) {
				i := int64SearchGreaterThanOrEqualTo(2, []int64{2})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key after", func(t *testing.T) {
				i := int64SearchGreaterThanOrEqualTo(3, []int64{2})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
		})
		t.Run("multiple item list", func(t *testing.T) {
			t.Run("key before first", func(t *testing.T) {
				i := int64SearchGreaterThanOrEqualTo(1, []int64{2, 4, 6})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match first", func(t *testing.T) {
				i := int64SearchGreaterThanOrEqualTo(2, []int64{2, 4, 6})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key between first and second", func(t *testing.T) {
				i := int64SearchGreaterThanOrEqualTo(3, []int64{2, 4, 6})
				if got, want := i, 1; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match second", func(t *testing.T) {
				i := int64SearchGreaterThanOrEqualTo(4, []int64{2, 4, 6})
				if got, want := i, 1; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key between second and third", func(t *testing.T) {
				i := int64SearchGreaterThanOrEqualTo(5, []int64{2, 4, 6})
				if got, want := i, 2; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match third", func(t *testing.T) {
				i := int64SearchGreaterThanOrEqualTo(6, []int64{2, 4, 6})
				if got, want := i, 2; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key after third", func(t *testing.T) {
				i := int64SearchGreaterThanOrEqualTo(7, []int64{2, 4, 6})
				if got, want := i, 2; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
		})
	})
	t.Run("less than or equal to", func(t *testing.T) {
		t.Run("empty list", func(t *testing.T) {
			i := int64SearchLessThanOrEqualTo(1, nil)
			if got, want := i, 0; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
		})
		t.Run("single item list", func(t *testing.T) {
			t.Run("key before", func(t *testing.T) {
				i := int64SearchLessThanOrEqualTo(1, []int64{2})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match", func(t *testing.T) {
				i := int64SearchLessThanOrEqualTo(int64(2), []int64{2})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key after", func(t *testing.T) {
				i := int64SearchLessThanOrEqualTo(3, []int64{2})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
		})
		t.Run("multiple item list", func(t *testing.T) {
			t.Run("key before first", func(t *testing.T) {
				i := int64SearchLessThanOrEqualTo(1, []int64{2, 4, 6})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match first", func(t *testing.T) {
				i := int64SearchLessThanOrEqualTo(int64(2), []int64{2, 4, 6})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key between first and second", func(t *testing.T) {
				i := int64SearchLessThanOrEqualTo(3, []int64{2, 4, 6})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match second", func(t *testing.T) {
				i := int64SearchLessThanOrEqualTo(4, []int64{2, 4, 6})
				if got, want := i, 1; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key between second and third", func(t *testing.T) {
				i := int64SearchLessThanOrEqualTo(5, []int64{2, 4, 6})
				if got, want := i, 1; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match third", func(t *testing.T) {
				i := int64SearchLessThanOrEqualTo(6, []int64{2, 4, 6})
				if got, want := i, 2; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key after third", func(t *testing.T) {
				i := int64SearchLessThanOrEqualTo(7, []int64{2, 4, 6})
				if got, want := i, 2; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
		})
	})
}

func TestNewInt64TreeReturnsErrorWhenInvalidOrder(t *testing.T) {
	for _, v := range []int{0, -1, 1, 3, 11} {
		_, err := NewInt64Tree(v)
		if err == nil {
			t.Errorf("GOT: %v; WANT: %v", err, fmt.Sprintf("power of 2: %d", v))
		}
	}
}

func int64LeafFrom(next *int64LeafNode, items ...int64) *int64LeafNode {
	n := &int64LeafNode{
		runts:  make([]int64, len(items)),
		values: make([]interface{}, len(items)),
		next:   next,
	}
	for i := 0; i < len(items); i++ {
		n.runts[i] = items[i]
		n.values[i] = items[i]
	}
	return n
}

func int64InternalFrom(items ...int64Node) *int64InternalNode {
	n := &int64InternalNode{
		runts:    make([]int64, len(items)),
		children: make([]int64Node, len(items)),
	}
	for i := 0; i < len(items); i++ {
		n.runts[i] = items[i].smallest()
		n.children[i] = items[i]
	}
	return n
}

////////////////////////////////////////

func ensureInt64Leaf(t *testing.T, actual, expected *int64LeafNode) {
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
	// ensureInt64Leaf(t, actual.next, expected.next)
	if got, want := actual.next, expected.next; got != want {
		t.Errorf("next GOT: %v; WANT: %v", got, want)
	}
	if t.Failed() {
		t.Errorf("\nGOT:\n\t%#v\nWANT:\n\t%#v", actual, expected)
	}
}

func ensureInt64Internal(t *testing.T, a, e *int64InternalNode) {
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
		ensureInt64Node(t, a.children[i], e.children[i])
	}
}

func ensureInt64Node(t *testing.T, actual, expected int64Node) {
	t.Helper()

	switch e := expected.(type) {
	case *int64LeafNode:
		a, ok := actual.(*int64LeafNode)
		if got, want := ok, true; got != want {
			t.Errorf("GOT: %T; WANT: %T", actual, e)
		}
		ensureInt64Leaf(t, a, e)
	case *int64InternalNode:
		a, ok := actual.(*int64InternalNode)
		if got, want := ok, true; got != want {
			t.Errorf("GOT: %T; WANT: %T", actual, e)
		}
		ensureInt64Internal(t, a, e)
	default:
		t.Errorf("GOT: %T; WANT: int64Node", expected)
	}
}

////////////////////////////////////////

func TestInt64InternalNodeMaybeSplit(t *testing.T) {
	leafD := int64LeafFrom(nil, 40, 41, 42, 43)
	leafC := int64LeafFrom(leafD, 30, 31, 32, 33)
	leafB := int64LeafFrom(leafC, 20, 21, 22, 23)
	leafA := int64LeafFrom(leafB, 10, 11, 12, 13)

	ni := int64InternalFrom(leafA, leafB, leafC, leafD)

	t.Run("does nothing when not full", func(t *testing.T) {
		_, right := ni.maybeSplit(6)
		if right != nil {
			t.Errorf("GOT: %v; WANT: %v", right, nil)
		}
	})

	t.Run("splits when full", func(t *testing.T) {
		expectedLeft := int64InternalFrom(leafA, leafB)
		expectedRight := int64InternalFrom(leafC, leafD)

		leftNode, rightNode := ni.maybeSplit(4)

		ensureInt64Node(t, leftNode, expectedLeft)
		ensureInt64Node(t, rightNode, expectedRight)
	})
}

func TestInternalInt64NodeInsertSmallerKey(t *testing.T) {
	gimme := func() (*int64LeafNode, *int64LeafNode) {
		leafB := int64LeafFrom(nil, 21, 22)
		leafA := int64LeafFrom(leafB, 12, 13)
		return leafA, leafB
	}

	leafA, leafB := gimme()
	ni := int64InternalFrom(leafA, leafB)

	d := &Int64Tree{root: ni, order: 4}

	d.Insert(11, 11)

	if got, want := ni.runts[0], int64(11); got != want {
		t.Fatalf("GOT: %v; WANT: %v", got, want)
	}
}

func TestInt64LeafNodeMaybeSplit(t *testing.T) {
	gimme := func() (*int64LeafNode, *int64LeafNode) {
		leafB := int64LeafFrom(nil, 21, 22, 23, 24)
		leafA := int64LeafFrom(leafB, 11, 12, 13, 14)
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
		ensureInt64Node(t, leftNode, int64LeafFrom(rightNode.(*int64LeafNode), 11, 12))
		ensureInt64Node(t, rightNode, int64LeafFrom(leafB, 13, 14))
	})
	t.Run("splits right edge when full", func(t *testing.T) {
		leafA, leafB := gimme()
		leftNode, rightNode := leafB.maybeSplit(4)
		if got, want := leafA.next, leftNode; got != want {
			t.Fatalf("GOT: %v; WANT: %v", got, want)
		}
		ensureInt64Node(t, leftNode, int64LeafFrom(rightNode.(*int64LeafNode), 21, 22))
		ensureInt64Node(t, rightNode, int64LeafFrom(nil, 23, 24))
	})
}

func TestInsertIntoSingleLeafInt64Tree(t *testing.T) {
	t.Run("when fewer than order elements", func(t *testing.T) {
		t.Run("when empty", func(t *testing.T) {
			d, _ := NewInt64Tree(4)
			nl, ok := d.root.(*int64LeafNode)
			if !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, false)
			}
			d.Insert(int64(30), int64(30))
			ensureInt64Leaf(t, nl, int64LeafFrom(nil, 30))
		})
		t.Run("when less than first runt", func(t *testing.T) {
			d, _ := NewInt64Tree(4)
			nl, ok := d.root.(*int64LeafNode)
			if !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, false)
			}
			d.Insert(int64(30), int64(30))
			d.Insert(int64(10), int64(10))
			ensureInt64Node(t, nl, int64LeafFrom(nil, 10, 30))
		})
		t.Run("when update value", func(t *testing.T) {
			d, _ := NewInt64Tree(4)
			nl, ok := d.root.(*int64LeafNode)
			if !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, false)
			}
			d.Insert(int64(30), int64(30))
			d.Insert(int64(10), int64(10))
			d.Insert(int64(30), int64(333))
			ensureInt64Node(t, nl, &int64LeafNode{
				runts:  []int64{10, 30},
				values: []interface{}{int64(10), int64(333)},
			})
		})
		t.Run("when between first and final runt", func(t *testing.T) {
			d, _ := NewInt64Tree(4)
			nl, ok := d.root.(*int64LeafNode)
			if !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, false)
			}
			d.Insert(int64(30), int64(30))
			d.Insert(int64(10), int64(10))
			d.Insert(int64(20), int64(20))
			ensureInt64Node(t, nl, int64LeafFrom(nil, 10, 20, 30))
		})
		t.Run("when after final runt", func(t *testing.T) {
			d, _ := NewInt64Tree(4)
			nl, ok := d.root.(*int64LeafNode)
			if !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, false)
			}
			d.Insert(int64(30), int64(30))
			d.Insert(int64(10), int64(10))
			d.Insert(int64(20), int64(20))
			d.Insert(int64(40), int64(40))
			ensureInt64Node(t, nl, int64LeafFrom(nil, 10, 20, 30, 40))
		})
	})

	t.Run("when insertion splits single leaf node", func(t *testing.T) {
		gimme := func() *Int64Tree {
			d, _ := NewInt64Tree(4)
			for _, v := range []int64{10, 20, 30, 40} {
				d.Insert(int64(v), int64(v))
			}
			return d
		}
		t.Run("when new key will be first node in left leaf", func(t *testing.T) {
			d := gimme()
			d.Insert(int64(0), int64(0))

			root, ok := d.root.(*int64InternalNode)
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
			if got, want := root.runts[0], int64(0); got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			ensureInt64Node(t, root.children[0], int64LeafFrom(root.children[1].(*int64LeafNode), 0, 10, 20))

			if got, want := root.runts[1], int64(30); got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			ensureInt64Node(t, root.children[1], int64LeafFrom(nil, 30, 40))
		})
		t.Run("when new key is in middle", func(t *testing.T) {
			d := gimme()
			d.Insert(int64(25), int64(25))
			root, ok := d.root.(*int64InternalNode)
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
			if got, want := root.runts[0], int64(10); got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			ensureInt64Node(t, root.children[0], int64LeafFrom(root.children[1].(*int64LeafNode), 10, 20, 25))

			if got, want := root.runts[1], int64(30); got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			ensureInt64Node(t, root.children[1], int64LeafFrom(nil, 30, 40))
		})
		t.Run("when new key will be final node in right leaf", func(t *testing.T) {
			d := gimme()
			d.Insert(int64(50), int64(50))
			root, ok := d.root.(*int64InternalNode)
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
			if got, want := root.runts[0], int64(10); got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			ensureInt64Node(t, root.children[0], int64LeafFrom(root.children[1].(*int64LeafNode), 10, 20))

			if got, want := root.runts[1], int64(30); got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			ensureInt64Node(t, root.children[1], int64LeafFrom(nil, 30, 40, 50))
		})
	})
}

func TestInt64TreeSearch(t *testing.T) {
	t.Run("empty tree", func(t *testing.T) {
		d, _ := NewInt64Tree(16)

		_, ok := d.Search(13)
		if got, want := ok, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
	})
	t.Run("single-leaf tree", func(t *testing.T) {
		t.Run("missing value", func(t *testing.T) {
			d, _ := NewInt64Tree(16)
			for i := int64(0); i < 15; i++ {
				if i != 13 {
					d.Insert(int64(i), int64(i))
				}
			}

			_, ok := d.Search(13)
			if got, want := ok, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
		})
		t.Run("existing value", func(t *testing.T) {
			d, _ := NewInt64Tree(16)
			for i := int64(0); i < 15; i++ {
				if i != 13 {
					d.Insert(int64(i), int64(i))
				}
			}

			value, ok := d.Search(8)
			if got, want := ok, true; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := value, int64(8); got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
		})
	})
	t.Run("multi-leaf tree", func(t *testing.T) {
		t.Run("missing value", func(t *testing.T) {
			d, _ := NewInt64Tree(4)
			for i := int64(0); i < 15; i++ {
				if i != 13 {
					d.Insert(int64(i), int64(i))
				}
			}

			_, ok := d.Search(13)
			if got, want := ok, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
		})
		t.Run("existing value", func(t *testing.T) {
			d, _ := NewInt64Tree(4)
			for i := int64(0); i < 15; i++ {
				if i != 13 {
					d.Insert(int64(i), int64(i))
				}
			}

			value, ok := d.Search(8)
			if got, want := ok, true; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := value, int64(8); got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
		})
	})
}

func TestInt64TreeCursor(t *testing.T) {
	t.Run("empty tree", func(t *testing.T) {
		var count int

		d, _ := NewInt64Tree(4)
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
			var values []int64

			d, _ := NewInt64Tree(16)
			for i := int64(0); i < 15; i++ {
				d.Insert(int64(i), int64(i))
			}

			c := d.NewScanner(0)
			for c.Scan() {
				_, v := c.Pair()
				values = append(values, v.(int64))
			}

			expected := []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}

			for i := 0; i < len(values) && i < len(expected); i++ {
				if got, want := values[i], expected[i]; got != want {
					t.Errorf("GOT: %v; WANT: %v", got, want)
				}
			}
		})
		t.Run("scan for missing element", func(t *testing.T) {
			var values []int64

			d, _ := NewInt64Tree(16)
			for i := int64(0); i < 15; i++ {
				if i != 13 {
					d.Insert(int64(i), int64(i))
				}
			}

			c := d.NewScanner(13)
			for c.Scan() {
				_, v := c.Pair()
				values = append(values, v.(int64))
			}

			expected := []int64{14, 2, 3, 4, 5, 6, 7, 8, 9}

			for i := 0; i < len(values) && i < len(expected); i++ {
				if got, want := values[i], expected[i]; got != want {
					t.Errorf("GOT: %v; WANT: %v", got, want)
				}
			}
		})
		t.Run("scan for existing element", func(t *testing.T) {
			var values []int64

			d, _ := NewInt64Tree(16)
			for i := int64(0); i < 15; i++ {
				d.Insert(int64(i), int64(i))
			}

			c := d.NewScanner(13)
			for c.Scan() {
				_, v := c.Pair()
				values = append(values, v.(int64))
			}

			expected := []int64{13, 14, 2, 3, 4, 5, 6, 7, 8, 9}

			for i := 0; i < len(values) && i < len(expected); i++ {
				if got, want := values[i], expected[i]; got != want {
					t.Errorf("GOT: %v; WANT: %v", got, want)
				}
			}
		})
	})
	t.Run("multi-leaf tree", func(t *testing.T) {
		var values []int64

		d, _ := NewInt64Tree(4)
		for i := int64(0); i < 15; i++ {
			d.Insert(int64(i), int64(i))
		}

		c := d.NewScanner(0)
		for c.Scan() {
			_, v := c.Pair()
			values = append(values, v.(int64))
		}

		expected := []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}

		for i := 0; i < len(values) && i < len(expected); i++ {
			if got, want := values[i], expected[i]; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
		}
	})
}

func TestInt64TreeUpdate(t *testing.T) {
	d, _ := NewInt64Tree(8)
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
	d.Insert(int64(3), int64(3))
	d.Update(int64(2), func(value interface{}, ok bool) interface{} {
		if got, want := ok, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := value, error(nil); got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		return "fourth"
	})
	value, ok = d.Search(int64(2))
	if got, want := ok, true; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := value, "fourth"; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
}

func TestInt64LeafNodeDelete(t *testing.T) {
	t.Run("still big enough", func(t *testing.T) {
		t.Run("key is missing", func(t *testing.T) {
			l := &int64LeafNode{
				runts:  []int64{11, 21, 31},
				values: []interface{}{11, 21, 31},
			}
			tooSmall := l.deleteKey(2, 42)
			if got, want := tooSmall, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureInt64Node(t, l, &int64LeafNode{
				runts:  []int64{11, 21, 31},
				values: []interface{}{11, 21, 31},
			})
		})
		t.Run("key is first", func(t *testing.T) {
			l := &int64LeafNode{
				runts:  []int64{11, 21, 31},
				values: []interface{}{11, 21, 31},
			}
			tooSmall := l.deleteKey(2, 11)
			if got, want := tooSmall, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureInt64Node(t, l, &int64LeafNode{
				runts:  []int64{21, 31},
				values: []interface{}{21, 31},
			})
		})
		t.Run("key is middle", func(t *testing.T) {
			l := &int64LeafNode{
				runts:  []int64{11, 21, 31},
				values: []interface{}{11, 21, 31},
			}
			tooSmall := l.deleteKey(2, 21)
			if got, want := tooSmall, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureInt64Node(t, l, &int64LeafNode{
				runts:  []int64{11, 31},
				values: []interface{}{11, 31},
			})
		})
		t.Run("key is last", func(t *testing.T) {
			l := &int64LeafNode{
				runts:  []int64{11, 21, 31},
				values: []interface{}{11, 21, 31},
			}
			tooSmall := l.deleteKey(2, 31)
			if got, want := tooSmall, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureInt64Node(t, l, &int64LeafNode{
				runts:  []int64{11, 21},
				values: []interface{}{11, 21},
			})
		})
	})
	t.Run("will be too small", func(t *testing.T) {
		l := int64LeafFrom(nil, 11, 21, 31, 41)
		tooSmall := l.deleteKey(4, 21)
		if got, want := tooSmall, true; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		ensureInt64Node(t, l, int64LeafFrom(nil, 11, 31, 41))
	})
}

func TestInt64LeafNodeAdoptFrom(t *testing.T) {
	t.Run("left", func(t *testing.T) {
		r := int64LeafFrom(nil, 5, 6, 7)
		l := int64LeafFrom(r, 0, 1, 2, 3, 4)

		r.adoptFromLeft(l)

		ensureInt64Node(t, l, int64LeafFrom(r, 0, 1, 2, 3))
		ensureInt64Node(t, r, int64LeafFrom(nil, 4, 5, 6, 7))
	})
	t.Run("right", func(t *testing.T) {
		r := int64LeafFrom(nil, 3, 4, 5, 6, 7)
		l := int64LeafFrom(r, 0, 1, 2)

		l.adoptFromRight(r)

		ensureInt64Node(t, l, int64LeafFrom(r, 0, 1, 2, 3))
		ensureInt64Node(t, r, int64LeafFrom(nil, 4, 5, 6, 7))
	})
}

func TestInt64InternalNodeAdoptFrom(t *testing.T) {
	t.Run("left", func(t *testing.T) {
		leafI := int64LeafFrom(nil, 90, 92, 94, 96, 98)
		leafH := int64LeafFrom(leafI, 80, 82, 84, 86, 88)
		leafG := int64LeafFrom(leafH, 70, 72, 74, 76, 78)
		leafF := int64LeafFrom(leafG, 60, 62, 64, 66, 68)
		leafE := int64LeafFrom(leafF, 50, 52, 54, 56, 58)
		leafD := int64LeafFrom(leafE, 40, 42, 44, 46, 48)
		leafC := int64LeafFrom(leafD, 30, 32, 34, 36, 38)
		leafB := int64LeafFrom(leafC, 20, 22, 24, 26, 28)
		leafA := int64LeafFrom(leafB, 10, 12, 14, 16, 18)

		left := int64InternalFrom(leafA, leafB, leafC, leafD, leafE, leafF)
		right := int64InternalFrom(leafG, leafH, leafI)

		right.adoptFromLeft(left)

		ensureInt64Internal(t, left, int64InternalFrom(leafA, leafB, leafC, leafD, leafE))
		ensureInt64Internal(t, right, int64InternalFrom(leafF, leafG, leafH, leafI))
	})
	t.Run("right", func(t *testing.T) {
		leafI := int64LeafFrom(nil, 90, 92, 94, 96, 98)
		leafH := int64LeafFrom(leafI, 80, 82, 84, 86, 88)
		leafG := int64LeafFrom(leafH, 70, 72, 74, 76, 78)
		leafF := int64LeafFrom(leafG, 60, 62, 64, 66, 68)
		leafE := int64LeafFrom(leafF, 50, 52, 54, 56, 58)
		leafD := int64LeafFrom(leafE, 40, 42, 44, 46, 48)
		leafC := int64LeafFrom(leafD, 30, 32, 34, 36, 38)
		leafB := int64LeafFrom(leafC, 20, 22, 24, 26, 28)
		leafA := int64LeafFrom(leafB, 10, 12, 14, 16, 18)

		left := int64InternalFrom(leafA, leafB, leafC)
		right := int64InternalFrom(leafD, leafE, leafF, leafG, leafH, leafI)

		left.adoptFromRight(right)

		ensureInt64Internal(t, left, int64InternalFrom(leafA, leafB, leafC, leafD))
		ensureInt64Internal(t, right, int64InternalFrom(leafE, leafF, leafG, leafH, leafI))
	})
}

func TestInt64LeafNodeMergeWithRight(t *testing.T) {
	leafC := int64LeafFrom(nil, 6, 7, 8, 9)
	leafB := int64LeafFrom(leafC, 3, 4, 5)
	leafA := int64LeafFrom(leafB, 0, 1, 2)

	leafA.absorbRight(leafB)

	ensureInt64Node(t, leafA, int64LeafFrom(leafC, 0, 1, 2, 3, 4, 5))

	if got, want := len(leafB.runts), 0; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := len(leafB.values), 0; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := leafB.next, (*int64LeafNode)(nil); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
}

func TestInt64InternalNodeMergeWithRight(t *testing.T) {
	leafI := int64LeafFrom(nil, 90, 92, 94, 96, 98)
	leafH := int64LeafFrom(leafI, 80, 82, 84, 86, 88)
	leafG := int64LeafFrom(leafH, 70, 72, 74, 76, 78)
	leafF := int64LeafFrom(leafG, 60, 62, 64, 66, 68)
	leafE := int64LeafFrom(leafF, 50, 52, 54, 56, 58)
	leafD := int64LeafFrom(leafE, 40, 42, 44, 46, 48)
	leafC := int64LeafFrom(leafD, 30, 32, 34, 36, 38)
	leafB := int64LeafFrom(leafC, 20, 22, 24, 26, 28)
	leafA := int64LeafFrom(leafB, 10, 12, 14, 16, 18)

	left := int64InternalFrom(leafA, leafB, leafC)
	right := int64InternalFrom(leafD, leafE, leafF, leafG)

	left.absorbRight(right)

	ensureInt64Internal(t, left, int64InternalFrom(leafA, leafB, leafC, leafD, leafE, leafF, leafG))

	if got, want := len(right.runts), 0; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := len(right.children), 0; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
}

func TestInt64InternalNodeDeleteKey(t *testing.T) {
	t.Run("not too small", func(t *testing.T) {
		leafE := int64LeafFrom(nil, 50, 52, 54, 56, 58)
		leafD := int64LeafFrom(leafE, 40, 42, 44, 46, 48)
		leafC := int64LeafFrom(leafD, 30, 32, 34, 36, 38)
		leafB := int64LeafFrom(leafC, 20, 22, 24, 26, 28)
		leafA := int64LeafFrom(leafB, 10, 12, 14, 16, 18)

		child := int64InternalFrom(leafA, leafB, leafC, leafD)

		if got, want := child.deleteKey(4, 22), false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
	})
	t.Run("child absorbs right when no left and skinny right", func(t *testing.T) {
		t.Run("child not too small", func(t *testing.T) {
			leafE := int64LeafFrom(nil, 50, 52, 54, 56, 58)
			leafD := int64LeafFrom(leafE, 40, 42, 44, 46, 48)
			leafC := int64LeafFrom(leafD, 30, 32, 34, 36, 38)
			leafB := int64LeafFrom(leafC, 20, 22, 24, 26)
			leafA := int64LeafFrom(leafB, 10, 12, 14, 16)

			child := int64InternalFrom(leafA, leafB, leafC, leafD, leafE)

			tooSmall := child.deleteKey(4, 12)
			if got, want := tooSmall, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}

			ensureInt64Leaf(t, leafA, int64LeafFrom(leafC, 10, 14, 16, 20, 22, 24, 26))
			if got, want := len(leafB.runts), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := len(leafB.values), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureInt64Leaf(t, leafC, int64LeafFrom(leafD, 30, 32, 34, 36, 38))
			ensureInt64Leaf(t, leafD, int64LeafFrom(leafE, 40, 42, 44, 46, 48))
			ensureInt64Leaf(t, leafE, int64LeafFrom(nil, 50, 52, 54, 56, 58))
		})
		t.Run("child too small", func(t *testing.T) {
			leafD := int64LeafFrom(nil, 40, 42, 44, 46, 48)
			leafC := int64LeafFrom(leafD, 30, 32, 34, 36, 38)
			leafB := int64LeafFrom(leafC, 20, 22, 24, 26)
			leafA := int64LeafFrom(leafB, 10, 12, 14, 16)

			child := int64InternalFrom(leafA, leafB, leafC, leafD)

			tooSmall := child.deleteKey(4, 12)
			if got, want := tooSmall, true; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}

			ensureInt64Leaf(t, leafA, int64LeafFrom(leafC, 10, 14, 16, 20, 22, 24, 26))
			if got, want := len(leafB.runts), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := len(leafB.values), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureInt64Leaf(t, leafC, int64LeafFrom(leafD, 30, 32, 34, 36, 38))
			ensureInt64Leaf(t, leafD, int64LeafFrom(nil, 40, 42, 44, 46, 48))
		})
	})
	t.Run("child adopts from right when no left and fat right", func(t *testing.T) {
		leafE := int64LeafFrom(nil, 50, 52, 54, 56, 58)
		leafD := int64LeafFrom(leafE, 40, 42, 44, 46, 48)
		leafC := int64LeafFrom(leafD, 30, 32, 34, 36, 38)
		leafB := int64LeafFrom(leafC, 20, 22, 24, 26, 28)
		leafA := int64LeafFrom(leafB, 10, 12, 14, 16)

		child := int64InternalFrom(leafA, leafB, leafC, leafD, leafE)

		tooSmall := child.deleteKey(4, 12)
		if got, want := tooSmall, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}

		ensureInt64Leaf(t, leafA, int64LeafFrom(leafB, 10, 14, 16, 20))
		ensureInt64Leaf(t, leafB, int64LeafFrom(leafC, 22, 24, 26, 28))
		ensureInt64Leaf(t, leafC, int64LeafFrom(leafD, 30, 32, 34, 36, 38))
		ensureInt64Leaf(t, leafD, int64LeafFrom(leafE, 40, 42, 44, 46, 48))
		ensureInt64Leaf(t, leafE, int64LeafFrom(nil, 50, 52, 54, 56, 58))
	})
	t.Run("left absorbs child when skinny left and no right", func(t *testing.T) {
		t.Run("too small", func(t *testing.T) {
			leafD := int64LeafFrom(nil, 40, 42, 44, 46)
			leafC := int64LeafFrom(leafD, 30, 32, 34, 36)
			leafB := int64LeafFrom(leafC, 20, 22, 24, 26)
			leafA := int64LeafFrom(leafB, 10, 12, 14, 16)

			child := int64InternalFrom(leafA, leafB, leafC, leafD)

			tooSmall := child.deleteKey(4, 42)
			if got, want := tooSmall, true; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}

			ensureInt64Leaf(t, leafA, int64LeafFrom(leafB, 10, 12, 14, 16))
			ensureInt64Leaf(t, leafB, int64LeafFrom(leafC, 20, 22, 24, 26))
			ensureInt64Leaf(t, leafC, int64LeafFrom(nil, 30, 32, 34, 36, 40, 44, 46))
			if got, want := len(leafD.runts), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := len(leafD.values), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
		})
		t.Run("not too small", func(t *testing.T) {
			leafE := int64LeafFrom(nil, 50, 52, 54, 56)
			leafD := int64LeafFrom(leafE, 40, 42, 44, 46)
			leafC := int64LeafFrom(leafD, 30, 32, 34, 36)
			leafB := int64LeafFrom(leafC, 20, 22, 24, 26)
			leafA := int64LeafFrom(leafB, 10, 12, 14, 16)

			child := int64InternalFrom(leafA, leafB, leafC, leafD, leafE)

			tooSmall := child.deleteKey(4, 52)
			if got, want := tooSmall, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}

			ensureInt64Leaf(t, leafA, int64LeafFrom(leafB, 10, 12, 14, 16))
			ensureInt64Leaf(t, leafB, int64LeafFrom(leafC, 20, 22, 24, 26))
			ensureInt64Leaf(t, leafC, int64LeafFrom(leafD, 30, 32, 34, 36))
			ensureInt64Leaf(t, leafD, int64LeafFrom(nil, 40, 42, 44, 46, 50, 54, 56))
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
			leafC := int64LeafFrom(nil, 30, 32, 34, 36)
			leafB := int64LeafFrom(leafC, 20, 22, 24, 26)
			leafA := int64LeafFrom(leafB, 10, 12, 14, 16)

			child := int64InternalFrom(leafA, leafB, leafC)

			tooSmall := child.deleteKey(4, 22)
			if got, want := tooSmall, true; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}

			ensureInt64Leaf(t, leafA, int64LeafFrom(leafC, 10, 12, 14, 16, 20, 24, 26))
			if got, want := len(leafB.runts), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := len(leafB.values), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureInt64Leaf(t, leafC, int64LeafFrom(nil, 30, 32, 34, 36))
		})
		t.Run("not too small", func(t *testing.T) {
			leafE := int64LeafFrom(nil, 50, 52, 54, 56)
			leafD := int64LeafFrom(leafE, 40, 42, 44, 46)
			leafC := int64LeafFrom(leafD, 30, 32, 34, 36)
			leafB := int64LeafFrom(leafC, 20, 22, 24, 26)
			leafA := int64LeafFrom(leafB, 10, 12, 14, 16)

			child := int64InternalFrom(leafA, leafB, leafC, leafD, leafE)

			tooSmall := child.deleteKey(4, 22)
			if got, want := tooSmall, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}

			ensureInt64Leaf(t, leafA, int64LeafFrom(leafC, 10, 12, 14, 16, 20, 24, 26))
			if got, want := len(leafB.runts), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := len(leafB.values), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureInt64Leaf(t, leafC, int64LeafFrom(leafD, 30, 32, 34, 36))
			ensureInt64Leaf(t, leafD, int64LeafFrom(leafE, 40, 42, 44, 46))
		})
	})
	t.Run("child adopts from right when skinny left and fat right", func(t *testing.T) {
		leafE := int64LeafFrom(nil, 50, 52, 54, 56, 58)
		leafD := int64LeafFrom(leafE, 40, 42, 44, 46, 48)
		leafC := int64LeafFrom(leafD, 30, 32, 34, 36, 38)
		leafB := int64LeafFrom(leafC, 20, 22, 24, 26)
		leafA := int64LeafFrom(leafB, 10, 12, 14, 16)

		child := int64InternalFrom(leafA, leafB, leafC, leafD, leafE)

		tooSmall := child.deleteKey(4, 22)
		if got, want := tooSmall, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}

		ensureInt64Leaf(t, leafA, int64LeafFrom(leafB, 10, 12, 14, 16))
		ensureInt64Leaf(t, leafB, int64LeafFrom(leafC, 20, 24, 26, 30))
		ensureInt64Leaf(t, leafC, int64LeafFrom(leafD, 32, 34, 36, 38))
		ensureInt64Leaf(t, leafD, int64LeafFrom(leafE, 40, 42, 44, 46, 48))
		ensureInt64Leaf(t, leafE, int64LeafFrom(nil, 50, 52, 54, 56, 58))
	})
	t.Run("child adopts from left when fat left and no right", func(t *testing.T) {
		leafE := int64LeafFrom(nil, 50, 52, 54, 56)
		leafD := int64LeafFrom(leafE, 40, 42, 44, 46, 48)
		leafC := int64LeafFrom(leafD, 30, 32, 34, 36)
		leafB := int64LeafFrom(leafC, 20, 22, 24, 26)
		leafA := int64LeafFrom(leafB, 10, 12, 14, 16)

		child := int64InternalFrom(leafA, leafB, leafC, leafD, leafE)

		tooSmall := child.deleteKey(4, 52)
		if got, want := tooSmall, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}

		ensureInt64Leaf(t, leafA, int64LeafFrom(leafB, 10, 12, 14, 16))
		ensureInt64Leaf(t, leafB, int64LeafFrom(leafC, 20, 22, 24, 26))
		ensureInt64Leaf(t, leafC, int64LeafFrom(leafD, 30, 32, 34, 36))
		ensureInt64Leaf(t, leafD, int64LeafFrom(leafE, 40, 42, 44, 46))
		ensureInt64Leaf(t, leafE, int64LeafFrom(nil, 48, 50, 54, 56))
	})
	t.Run("child adopts from left when fat left and skinny right", func(t *testing.T) {
		leafE := int64LeafFrom(nil, 50, 52, 54, 56)
		leafD := int64LeafFrom(leafE, 40, 42, 44, 46)
		leafC := int64LeafFrom(leafD, 30, 32, 34, 36)
		leafB := int64LeafFrom(leafC, 20, 22, 24, 26, 28)
		leafA := int64LeafFrom(leafB, 10, 12, 14, 16)

		child := int64InternalFrom(leafA, leafB, leafC, leafD, leafE)

		tooSmall := child.deleteKey(4, 32)
		if got, want := tooSmall, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}

		ensureInt64Leaf(t, leafA, int64LeafFrom(leafB, 10, 12, 14, 16))
		ensureInt64Leaf(t, leafB, int64LeafFrom(leafC, 20, 22, 24, 26))
		ensureInt64Leaf(t, leafC, int64LeafFrom(leafD, 28, 30, 34, 36))
		ensureInt64Leaf(t, leafD, int64LeafFrom(leafE, 40, 42, 44, 46))
		ensureInt64Leaf(t, leafE, int64LeafFrom(nil, 50, 52, 54, 56))
	})
	t.Run("child adopts from right when fat left and fat right", func(t *testing.T) {
		leafE := int64LeafFrom(nil, 50, 52, 54, 56, 58)
		leafD := int64LeafFrom(leafE, 40, 42, 44, 46, 48)
		leafC := int64LeafFrom(leafD, 30, 32, 34, 36)
		leafB := int64LeafFrom(leafC, 20, 22, 24, 26, 28)
		leafA := int64LeafFrom(leafB, 10, 12, 14, 16, 18)

		child := int64InternalFrom(leafA, leafB, leafC, leafD, leafE)

		tooSmall := child.deleteKey(4, 32)
		if got, want := tooSmall, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}

		ensureInt64Leaf(t, leafA, int64LeafFrom(leafB, 10, 12, 14, 16, 18))
		ensureInt64Leaf(t, leafB, int64LeafFrom(leafC, 20, 22, 24, 26, 28))
		ensureInt64Leaf(t, leafC, int64LeafFrom(leafD, 30, 34, 36, 40))
		ensureInt64Leaf(t, leafD, int64LeafFrom(leafE, 42, 44, 46, 48))
		ensureInt64Leaf(t, leafE, int64LeafFrom(nil, 50, 52, 54, 56, 58))
	})
}

func TestInt64Delete(t *testing.T) {
	d, _ := NewInt64Tree(4)

	for i := int64(0); i < 16; i++ {
		d.Insert(int64(i), int64(i))
	}

	for i := int64(0); i < 16; i++ {
		d.Delete(i)
	}
}
