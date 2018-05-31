package gobptree

import (
	"fmt"
	"strconv"
	"testing"
)

func ensureStringLeaves(t *testing.T, a stringNode, leafB *stringLeafNode) {
	t.Helper()
	if a == nil {
		t.Fatalf("GOT: %v; WANT: %#v", a, leafB)
	}
	leafA, ok := a.(*stringLeafNode)
	if !ok {
		t.Fatalf("GOT: %v; WANT: %#v", a, leafB)
	}
	if got, want := len(leafA.runts), len(leafB.runts); got != want {
		t.Errorf("length(runts) GOT: %v; WANT: %v", got, want)
	}
	if got, want := len(leafA.values), len(leafB.values); got != want {
		t.Errorf("length(values) GOT: %v; WANT: %v", got, want)
	}
	for i := 0; i < len(leafA.runts) && i < len(leafB.runts); i++ {
		if got, want := leafA.runts[i], leafB.runts[i]; got != want {
			t.Errorf("runt[%d] GOT: %v; WANT: %v", i, got, want)
		}
		if got, want := leafA.values[i], leafB.values[i]; got != want {
			t.Errorf("value[%d] GOT: %v; WANT: %v", i, got, want)
		}
	}
	if got, want := leafA.next, leafB.next; got != want {
		t.Errorf("next GOT: %v; WANT: %v", got, want)
	}
	if t.Failed() {
		t.Errorf("\nGOT:\n\t%#v\nWANT:\n\t%#v", a, leafB)
	}
}

func TestStringBinarySearch(t *testing.T) {
	t.Run("greater than or equal to", func(t *testing.T) {
		t.Run("empty list", func(t *testing.T) {
			i := stringSearchGreaterThanOrEqualTo("A", nil)
			if got, want := i, 0; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
		})
		t.Run("single item list", func(t *testing.T) {
			t.Run("key before", func(t *testing.T) {
				i := stringSearchGreaterThanOrEqualTo("A", []string{"B"})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match", func(t *testing.T) {
				i := stringSearchGreaterThanOrEqualTo("B", []string{"B"})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key after", func(t *testing.T) {
				i := stringSearchGreaterThanOrEqualTo("C", []string{"B"})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
		})
		t.Run("multiple item list", func(t *testing.T) {
			t.Run("key before first", func(t *testing.T) {
				i := stringSearchGreaterThanOrEqualTo("A", []string{"B", "D", "F"})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match first", func(t *testing.T) {
				i := stringSearchGreaterThanOrEqualTo("B", []string{"B", "D", "F"})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key between first and second", func(t *testing.T) {
				i := stringSearchGreaterThanOrEqualTo("C", []string{"B", "D", "F"})
				if got, want := i, 1; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match second", func(t *testing.T) {
				i := stringSearchGreaterThanOrEqualTo("D", []string{"B", "D", "F"})
				if got, want := i, 1; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key between second and third", func(t *testing.T) {
				i := stringSearchGreaterThanOrEqualTo("E", []string{"B", "D", "F"})
				if got, want := i, 2; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match third", func(t *testing.T) {
				i := stringSearchGreaterThanOrEqualTo("F", []string{"B", "D", "F"})
				if got, want := i, 2; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key after third", func(t *testing.T) {
				i := stringSearchGreaterThanOrEqualTo("G", []string{"B", "D", "F"})
				if got, want := i, 2; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
		})
	})
	t.Run("less than or equal to", func(t *testing.T) {
		t.Run("empty list", func(t *testing.T) {
			i := stringSearchLessThanOrEqualTo("A", []string{})
			if got, want := i, 0; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
		})
		t.Run("single item list", func(t *testing.T) {
			t.Run("key before", func(t *testing.T) {
				i := stringSearchLessThanOrEqualTo("A", []string{"B"})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match", func(t *testing.T) {
				i := stringSearchLessThanOrEqualTo("B", []string{"B"})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key after", func(t *testing.T) {
				i := stringSearchLessThanOrEqualTo("C", []string{"B"})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
		})
		t.Run("multiple item list", func(t *testing.T) {
			t.Run("key before first", func(t *testing.T) {
				i := stringSearchLessThanOrEqualTo("A", []string{"B", "D", "F"})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match first", func(t *testing.T) {
				i := stringSearchLessThanOrEqualTo("B", []string{"B", "D", "F"})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key between first and second", func(t *testing.T) {
				i := stringSearchLessThanOrEqualTo("C", []string{"B", "D", "F"})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match second", func(t *testing.T) {
				i := stringSearchLessThanOrEqualTo("D", []string{"B", "D", "F"})
				if got, want := i, 1; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key between second and third", func(t *testing.T) {
				i := stringSearchLessThanOrEqualTo("E", []string{"B", "D", "F"})
				if got, want := i, 1; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match third", func(t *testing.T) {
				i := stringSearchLessThanOrEqualTo("F", []string{"B", "D", "F"})
				if got, want := i, 2; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key after third", func(t *testing.T) {
				i := stringSearchLessThanOrEqualTo("G", []string{"B", "D", "F"})
				if got, want := i, 2; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
		})
	})
}

func TestNewStringTreeReturnsErrorWhenInvalidOrder(t *testing.T) {
	for _, v := range []int{0, -1, 1, 3, 11} {
		_, err := NewStringTree(v)
		if err == nil {
			t.Errorf("GOT: %v; WANT: %v", err, fmt.Sprintf("power of 2: %d", v))
		}
	}
}

func TestStringInternalNodeMaybeSplit(t *testing.T) {
	leafD := &stringLeafNode{
		runts:  []string{"d", "dd", "ddd"},
		values: []interface{}{1, 2, 3},
	}
	leafC := &stringLeafNode{
		runts:  []string{"c", "cc", "ccc"},
		values: []interface{}{1, 2, 3},
		next:   leafD,
	}
	leafB := &stringLeafNode{
		runts:  []string{"b", "bb", "bbb"},
		values: []interface{}{1, 2, 3},
		next:   leafC,
	}
	leafA := &stringLeafNode{
		runts:  []string{"a", "aa", "aaa"},
		values: []interface{}{1, 2, 3},
		next:   leafB,
	}

	ni := &stringInternalNode{
		runts:    []string{"a", "b", "c", "d"},
		children: []stringNode{leafA, leafB, leafC, leafD},
	}

	t.Run("does nothing when not full", func(t *testing.T) {
		_, right := ni.MaybeSplit(6)
		if right != nil {
			t.Errorf("GOT: %v; WANT: %v", right, nil)
		}
	})

	t.Run("splits when full", func(t *testing.T) {
		expectedLeft := &stringInternalNode{
			runts:    []string{"a", "b"},
			children: []stringNode{leafA, leafB},
		}
		expectedRight := &stringInternalNode{
			runts:    []string{"c", "d"},
			children: []stringNode{leafC, leafD},
		}

		leftNode, rightNode := ni.MaybeSplit(4)

		// left side
		if leftNode == nil {
			t.Fatalf("GOT: %v; WANT: %v", leftNode, "some node")
		}
		left, ok := leftNode.(*stringInternalNode)
		if !ok {
			t.Fatalf("GOT: %v; WANT: %v", ok, true)
		}
		if got, want := len(left.runts), len(expectedLeft.runts); got != want {
			t.Fatalf("GOT: %v; WANT: %v", got, want)
		}
		for i := 0; i < len(left.runts); i++ {
			if got, want := left.runts[i], expectedLeft.runts[i]; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
		}
		if got, want := len(left.children), len(expectedLeft.children); got != want {
			t.Fatalf("GOT: %v; WANT: %v", got, want)
		}
		for i := 0; i < len(left.children); i++ {
			if got, want := left.children[i], expectedLeft.children[i]; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
		}

		// right side
		if rightNode == nil {
			t.Fatalf("GOT: %v; WANT: %v", rightNode, "some node")
		}
		right, ok := rightNode.(*stringInternalNode)
		if !ok {
			t.Fatalf("GOT: %v; WANT: %v", ok, true)
		}
		if got, want := len(right.runts), len(expectedRight.runts); got != want {
			t.Fatalf("GOT: %v; WANT: %v", got, want)
		}
		for i := 0; i < len(right.runts); i++ {
			if got, want := right.runts[i], expectedRight.runts[i]; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
		}
		if got, want := len(right.children), len(expectedRight.children); got != want {
			t.Fatalf("GOT: %v; WANT: %v", got, want)
		}
		for i := 0; i < len(right.children); i++ {
			if got, want := right.children[i], expectedRight.children[i]; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
		}
	})
}

func TestStringInternalNodeInsertSmallerKey(t *testing.T) {
	gimme := func() (*stringLeafNode, *stringLeafNode) {
		leafB := &stringLeafNode{
			runts:  []string{"b", "bb"},
			values: []interface{}{1, 2},
		}
		leafA := &stringLeafNode{
			runts:  []string{"aa", "aaa"},
			values: []interface{}{2, 3},
			next:   leafB,
		}
		return leafA, leafB
	}

	leafA, leafB := gimme()
	ni := &stringInternalNode{
		runts:    []string{"aa", "b"},
		children: []stringNode{leafA, leafB},
	}

	d := &StringTree{root: ni, order: 4}

	d.Insert("a", 1)

	if got, want := string(ni.runts[0]), "a"; got != want {
		t.Fatalf("GOT: %v; WANT: %v", got, want)
	}
}

func TestStringLeafNodeMaybeSplit(t *testing.T) {
	gimme := func() (*stringLeafNode, *stringLeafNode) {
		leafB := &stringLeafNode{
			runts:  []string{"b", "bb", "bbb", "bbbb"},
			values: []interface{}{1, 2, 3, 4},
		}
		leafA := &stringLeafNode{
			runts:  []string{"a", "aa", "aaa", "aaaa"},
			values: []interface{}{1, 2, 3, 4},
			next:   leafB,
		}
		return leafA, leafB
	}

	t.Run("when not full does nothing", func(t *testing.T) {
		_, leafB := gimme()
		_, right := leafB.MaybeSplit(6)
		if right != nil {
			t.Errorf("GOT: %v; WANT: %v", right, nil)
		}
	})

	t.Run("splits non-right edge when full", func(t *testing.T) {
		leafA, leafB := gimme()
		leftNode, rightNode := leafA.MaybeSplit(4)
		ensureStringLeaves(t, leftNode, &stringLeafNode{
			runts:  []string{"a", "aa"},
			values: []interface{}{1, 2},
			next:   rightNode.(*stringLeafNode),
		})
		ensureStringLeaves(t, rightNode, &stringLeafNode{
			runts:  []string{"aaa", "aaaa"},
			values: []interface{}{3, 4},
			next:   leafB,
		})
	})
	t.Run("splits right edge when full", func(t *testing.T) {
		leafA, leafB := gimme()
		leftNode, rightNode := leafB.MaybeSplit(4)
		if got, want := leafA.next, leftNode; got != want {
			t.Fatalf("GOT: %v; WANT: %v", got, want)
		}
		ensureStringLeaves(t, leftNode, &stringLeafNode{
			runts:  []string{"b", "bb"},
			values: []interface{}{1, 2},
			next:   rightNode.(*stringLeafNode),
		})
		ensureStringLeaves(t, rightNode, &stringLeafNode{
			runts:  []string{"bbb", "bbbb"},
			values: []interface{}{3, 4},
			next:   nil,
		})
	})
}

func TestStringInsertIntoSingleLeafTree(t *testing.T) {
	t.Run("when fewer than order elements", func(t *testing.T) {
		t.Run("when empty", func(t *testing.T) {
			d, _ := NewStringTree(4)
			ln, ok := d.root.(*stringLeafNode)
			if !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, false)
			}
			d.Insert("30", "thirty")
			ensureStringLeaves(t, ln, &stringLeafNode{
				runts:  []string{"30"},
				values: []interface{}{"thirty"},
			})
		})
		t.Run("when less than first runt", func(t *testing.T) {
			d, _ := NewStringTree(4)
			ln, ok := d.root.(*stringLeafNode)
			if !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, false)
			}
			d.Insert("30", "thirty")
			d.Insert("10", "ten")
			ensureStringLeaves(t, ln, &stringLeafNode{
				runts:  []string{"10", "30"},
				values: []interface{}{"ten", "thirty"},
			})
		})
		t.Run("when update value", func(t *testing.T) {
			d, _ := NewStringTree(4)
			ln, ok := d.root.(*stringLeafNode)
			if !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, false)
			}
			d.Insert("30", "thirty")
			d.Insert("10", "ten")
			d.Insert("30", "THIRTY")
			ensureStringLeaves(t, ln, &stringLeafNode{
				runts:  []string{"10", "30"},
				values: []interface{}{"ten", "THIRTY"},
			})
		})
		t.Run("when between first and final runt", func(t *testing.T) {
			d, _ := NewStringTree(4)
			ln, ok := d.root.(*stringLeafNode)
			if !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, false)
			}
			d.Insert("30", "thirty")
			d.Insert("10", "ten")
			d.Insert("30", "THIRTY")
			d.Insert("20", "twenty")
			ensureStringLeaves(t, ln, &stringLeafNode{
				runts:  []string{"10", "20", "30"},
				values: []interface{}{"ten", "twenty", "THIRTY"},
			})
		})
		t.Run("when after final runt", func(t *testing.T) {
			d, _ := NewStringTree(4)
			ln, ok := d.root.(*stringLeafNode)
			if !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, false)
			}
			d.Insert("30", "thirty")
			d.Insert("10", "ten")
			d.Insert("30", "THIRTY")
			d.Insert("20", "twenty")
			d.Insert("40", "forty")
			ensureStringLeaves(t, ln, &stringLeafNode{
				runts:  []string{"10", "20", "30", "40"},
				values: []interface{}{"ten", "twenty", "THIRTY", "forty"},
			})
		})
	})

	t.Run("when insertion splits single leaf node", func(t *testing.T) {
		gimme := func() *StringTree {
			d, _ := NewStringTree(4)
			for k, v := range map[string]string{"10": "ten", "20": "twenty", "30": "thirty", "40": "forty"} {
				d.Insert(k, v)
			}
			// t.Logf("init root runts: %v\ninit root values: %v\n", d.root.(*leaf).runts, d.root.(*leaf).values)
			return d
		}
		t.Run("when new key will be first node in left leaf", func(t *testing.T) {
			d := gimme()
			d.Insert("0", "zero")
			root, ok := d.root.(*stringInternalNode)
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
			if got, want := string(root.runts[0]), "0"; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			ensureStringLeaves(t, root.children[0], &stringLeafNode{
				runts:  []string{"0", "10", "20"},
				values: []interface{}{"zero", "ten", "twenty"},
				next:   root.children[1].(*stringLeafNode),
			})

			if got, want := string(root.runts[1]), "30"; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			ensureStringLeaves(t, root.children[1], &stringLeafNode{
				runts:  []string{"30", "40"},
				values: []interface{}{"thirty", "forty"},
			})
		})
		t.Run("when new key is in middle", func(t *testing.T) {
			d := gimme()
			d.Insert("25", "twenty-five")
			root, ok := d.root.(*stringInternalNode)
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
			if got, want := string(root.runts[0]), "10"; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			ensureStringLeaves(t, root.children[0], &stringLeafNode{
				runts:  []string{"10", "20", "25"},
				values: []interface{}{"ten", "twenty", "twenty-five"},
				next:   root.children[1].(*stringLeafNode),
			})

			if got, want := string(root.runts[1]), "30"; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			ensureStringLeaves(t, root.children[1], &stringLeafNode{
				runts:  []string{"30", "40"},
				values: []interface{}{"thirty", "forty"},
			})
		})
		t.Run("when new key will be final node in right leaf", func(t *testing.T) {
			d := gimme()
			d.Insert("50", "fifty")
			root, ok := d.root.(*stringInternalNode)
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
			if got, want := string(root.runts[0]), "10"; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			ensureStringLeaves(t, root.children[0], &stringLeafNode{
				runts:  []string{"10", "20"},
				values: []interface{}{"ten", "twenty"},
				next:   root.children[1].(*stringLeafNode),
			})

			if got, want := string(root.runts[1]), "30"; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			ensureStringLeaves(t, root.children[1], &stringLeafNode{
				runts:  []string{"30", "40", "50"},
				values: []interface{}{"thirty", "forty", "fifty"},
			})
		})
	})
}

func TestStringSearch(t *testing.T) {
	t.Run("empty tree", func(t *testing.T) {
		t.Skip("TODO")
	})
	t.Run("single-leaf tree", func(t *testing.T) {
		t.Run("missing value", func(t *testing.T) {
			d, _ := NewStringTree(16)
			for i := 0; i < 15; i++ {
				if i != 13 {
					d.Insert(strconv.Itoa(i), i)
				}
			}

			_, ok := d.Search("13")
			if got, want := ok, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
		})
		t.Run("existing value", func(t *testing.T) {
			d, _ := NewStringTree(16)
			for i := 0; i < 15; i++ {
				if i != 13 {
					d.Insert(strconv.Itoa(i), i)
				}
			}

			value, ok := d.Search("8")
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
			d, _ := NewStringTree(4)
			for i := 0; i < 15; i++ {
				if i != 13 {
					d.Insert(strconv.Itoa(i), i)
				}
			}

			_, ok := d.Search("13")
			if got, want := ok, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
		})
		t.Run("existing value", func(t *testing.T) {
			d, _ := NewStringTree(4)
			for i := 0; i < 15; i++ {
				if i != 13 {
					d.Insert(strconv.Itoa(i), i)
				}
			}

			value, ok := d.Search("8")
			if got, want := ok, true; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := value, 8; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
		})
	})
}

func TestStringCursor(t *testing.T) {
	t.Run("empty tree", func(t *testing.T) {
		var values []int

		d, _ := NewStringTree(4)
		c := d.NewScanner("")
		for c.Scan() {
			_, v := c.Pair()
			values = append(values, v.(int))
		}

		if got, want := len(values), 0; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
	})
	t.Run("single-leaf tree", func(t *testing.T) {
		t.Run("scan for zero-value element", func(t *testing.T) {
			var values []int

			d, _ := NewStringTree(16)
			for i := 0; i < 15; i++ {
				d.Insert(strconv.Itoa(i), i)
			}

			c := d.NewScanner("")
			for c.Scan() {
				_, v := c.Pair()
				values = append(values, v.(int))
			}

			expected := []int{0, 1, 10, 11, 12, 13, 14, 2, 3, 4, 5, 6, 7, 8, 9}

			for i := 0; i < len(values) && i < len(expected); i++ {
				if got, want := values[i], expected[i]; got != want {
					t.Errorf("GOT: %v; WANT: %v", got, want)
				}
			}
		})
		t.Run("scan for missing element", func(t *testing.T) {
			var values []int

			d, _ := NewStringTree(16)
			for i := 0; i < 15; i++ {
				if i != 13 {
					d.Insert(strconv.Itoa(i), i)
				}
			}

			c := d.NewScanner("13")
			for c.Scan() {
				_, v := c.Pair()
				values = append(values, v.(int))
			}

			expected := []int{14, 2, 3, 4, 5, 6, 7, 8, 9}

			for i := 0; i < len(values) && i < len(expected); i++ {
				if got, want := values[i], expected[i]; got != want {
					t.Errorf("GOT: %v; WANT: %v", got, want)
				}
			}
		})
		t.Run("scan for existing element", func(t *testing.T) {
			var values []int

			d, _ := NewStringTree(16)
			for i := 0; i < 15; i++ {
				d.Insert(strconv.Itoa(i), i)
			}

			c := d.NewScanner("13")
			for c.Scan() {
				_, v := c.Pair()
				values = append(values, v.(int))
			}

			expected := []int{13, 14, 2, 3, 4, 5, 6, 7, 8, 9}

			for i := 0; i < len(values) && i < len(expected); i++ {
				if got, want := values[i], expected[i]; got != want {
					t.Errorf("GOT: %v; WANT: %v", got, want)
				}
			}
		})
	})
	t.Run("multi-leaf tree", func(t *testing.T) {
		var values []int

		d, _ := NewStringTree(4)
		for i := 0; i < 15; i++ {
			d.Insert(strconv.Itoa(i), i)
		}

		c := d.NewScanner("")
		for c.Scan() {
			_, v := c.Pair()
			values = append(values, v.(int))
		}

		expected := []int{0, 1, 10, 11, 12, 13, 14, 2, 3, 4, 5, 6, 7, 8, 9}

		for i := 0; i < len(values) && i < len(expected); i++ {
			if got, want := values[i], expected[i]; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
		}
	})
}

func TestStringTreeUpdate(t *testing.T) {
	d, _ := NewStringTree(8)
	d.Update("A", func(value interface{}, ok bool) interface{} {
		if got, want := ok, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := value, error(nil); got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		return "first"
	})
	d.Update("A", func(value interface{}, ok bool) interface{} {
		if got, want := ok, true; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := value, "first"; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		return "second"
	})
	value, ok := d.Search("A")
	if got, want := ok, true; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := value, "second"; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	d.Insert("C", "third")
	d.Update("B", func(value interface{}, ok bool) interface{} {
		if got, want := ok, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := value, error(nil); got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		return "fourth"
	})
	value, ok = d.Search("B")
	if got, want := ok, true; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := value, "fourth"; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
}
