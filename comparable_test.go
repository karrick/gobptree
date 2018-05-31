package gobptree

import (
	"fmt"
	"strconv"
	"testing"
)

// testString is a Comparable data structure used for testing the ComparableTree
// and its helper functions.
type testString string

func (a testString) Less(b interface{}) bool {
	bs, ok := b.(testString)
	return ok && string(a) < string(bs)
}

func (a testString) Greater(b interface{}) bool {
	bs, ok := b.(testString)
	return ok && string(a) > string(bs)
}

func (_ testString) ZeroValue() Comparable { return testString("") }

////////////////////////////////////////

func ensureComparableLeaves(t *testing.T, a comparableNode, leafB *comparableLeafNode) {
	t.Helper()
	if a == nil {
		t.Fatalf("GOT: %v; WANT: %#v", a, leafB)
	}
	leafA, ok := a.(*comparableLeafNode)
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

// cls returns a slice of Comparable String values from its arguments.
func cls(items ...string) []Comparable {
	bar := make([]Comparable, len(items))
	for i := 0; i < len(items); i++ {
		bar[i] = testString(items[i])
	}
	return bar
}

func TestComparableBinarySearch(t *testing.T) {
	t.Run("greater than or equal to", func(t *testing.T) {
		t.Run("empty list", func(t *testing.T) {
			i := comparableSearchGreaterThanOrEqualTo(testString("A"), nil)
			if got, want := i, 0; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
		})
		t.Run("single item list", func(t *testing.T) {
			t.Run("key before", func(t *testing.T) {
				i := comparableSearchGreaterThanOrEqualTo(testString("A"), cls("B"))
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match", func(t *testing.T) {
				i := comparableSearchGreaterThanOrEqualTo(testString("B"), cls("B"))
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key after", func(t *testing.T) {
				i := comparableSearchGreaterThanOrEqualTo(testString("C"), cls("B"))
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
		})
		t.Run("multiple item list", func(t *testing.T) {
			t.Run("key before first", func(t *testing.T) {
				i := comparableSearchGreaterThanOrEqualTo(testString("A"), cls("B", "D", "F"))
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match first", func(t *testing.T) {
				i := comparableSearchGreaterThanOrEqualTo(testString("B"), cls("B", "D", "F"))
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key between first and second", func(t *testing.T) {
				i := comparableSearchGreaterThanOrEqualTo(testString("C"), cls("B", "D", "F"))
				if got, want := i, 1; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match second", func(t *testing.T) {
				i := comparableSearchGreaterThanOrEqualTo(testString("D"), cls("B", "D", "F"))
				if got, want := i, 1; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key between second and third", func(t *testing.T) {
				i := comparableSearchGreaterThanOrEqualTo(testString("E"), cls("B", "D", "F"))
				if got, want := i, 2; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match third", func(t *testing.T) {
				i := comparableSearchGreaterThanOrEqualTo(testString("F"), cls("B", "D", "F"))
				if got, want := i, 2; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key after third", func(t *testing.T) {
				i := comparableSearchGreaterThanOrEqualTo(testString("G"), cls("B", "D", "F"))
				if got, want := i, 2; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
		})
	})
	t.Run("less than or equal to", func(t *testing.T) {
		t.Run("empty list", func(t *testing.T) {
			i := comparableSearchLessThanOrEqualTo(testString("A"), cls())
			if got, want := i, 0; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
		})
		t.Run("single item list", func(t *testing.T) {
			t.Run("key before", func(t *testing.T) {
				i := comparableSearchLessThanOrEqualTo(testString("A"), cls("B"))
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match", func(t *testing.T) {
				i := comparableSearchLessThanOrEqualTo(testString("B"), cls("B"))
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key after", func(t *testing.T) {
				i := comparableSearchLessThanOrEqualTo(testString("C"), cls("B"))
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
		})
		t.Run("multiple item list", func(t *testing.T) {
			t.Run("key before first", func(t *testing.T) {
				i := comparableSearchLessThanOrEqualTo(testString("A"), cls("B", "D", "F"))
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match first", func(t *testing.T) {
				i := comparableSearchLessThanOrEqualTo(testString("B"), cls("B", "D", "F"))
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key between first and second", func(t *testing.T) {
				i := comparableSearchLessThanOrEqualTo(testString("C"), cls("B", "D", "F"))
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match second", func(t *testing.T) {
				i := comparableSearchLessThanOrEqualTo(testString("D"), cls("B", "D", "F"))
				if got, want := i, 1; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key between second and third", func(t *testing.T) {
				i := comparableSearchLessThanOrEqualTo(testString("E"), cls("B", "D", "F"))
				if got, want := i, 1; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match third", func(t *testing.T) {
				i := comparableSearchLessThanOrEqualTo(testString("F"), cls("B", "D", "F"))
				if got, want := i, 2; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key after third", func(t *testing.T) {
				i := comparableSearchLessThanOrEqualTo(testString("G"), cls("B", "D", "F"))
				if got, want := i, 2; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
		})
	})
}

func TestNewComparableTreeReturnsErrorWhenInvalidOrder(t *testing.T) {
	for _, v := range []int{0, -1, 1, 3, 11} {
		_, err := NewComparableTree(v)
		if err == nil {
			t.Errorf("GOT: %v; WANT: %v", err, fmt.Sprintf("power of 2: %d", v))
		}
	}
}

func TestComparableInternalNodeMaybeSplit(t *testing.T) {
	leafD := &comparableLeafNode{
		runts:  cls("d", "dd", "ddd"),
		values: []interface{}{1, 2, 3},
	}
	leafC := &comparableLeafNode{
		runts:  cls("c", "cc", "ccc"),
		values: []interface{}{1, 2, 3},
		next:   leafD,
	}
	leafB := &comparableLeafNode{
		runts:  cls("b", "bb", "bbb"),
		values: []interface{}{1, 2, 3},
		next:   leafC,
	}
	leafA := &comparableLeafNode{
		runts:  cls("a", "aa", "aaa"),
		values: []interface{}{1, 2, 3},
		next:   leafB,
	}

	ni := &comparableInternalNode{
		runts:    cls("a", "b", "c", "d"),
		children: []comparableNode{leafA, leafB, leafC, leafD},
	}

	t.Run("does nothing when not full", func(t *testing.T) {
		_, right := ni.MaybeSplit(6)
		if right != nil {
			t.Errorf("GOT: %v; WANT: %v", right, nil)
		}
	})

	t.Run("splits when full", func(t *testing.T) {
		expectedLeft := &comparableInternalNode{
			runts:    cls("a", "b"),
			children: []comparableNode{leafA, leafB},
		}
		expectedRight := &comparableInternalNode{
			runts:    cls("c", "d"),
			children: []comparableNode{leafC, leafD},
		}

		leftNode, rightNode := ni.MaybeSplit(4)

		// left side
		if leftNode == nil {
			t.Fatalf("GOT: %v; WANT: %v", leftNode, "some node")
		}
		left, ok := leftNode.(*comparableInternalNode)
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
		right, ok := rightNode.(*comparableInternalNode)
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

func TestInternalComparableNodeInsertSmallerKey(t *testing.T) {
	gimme := func() (*comparableLeafNode, *comparableLeafNode) {
		leafB := &comparableLeafNode{
			runts:  cls("b", "bb"),
			values: []interface{}{1, 2},
		}
		leafA := &comparableLeafNode{
			runts:  cls("aa", "aaa"),
			values: []interface{}{2, 3},
			next:   leafB,
		}
		return leafA, leafB
	}

	leafA, leafB := gimme()
	ni := &comparableInternalNode{
		runts:    cls("aa", "b"),
		children: []comparableNode{leafA, leafB},
	}

	d := &ComparableTree{root: ni, order: 4}

	d.Insert(testString("a"), 1)

	if got, want := string(ni.runts[0].(testString)), "a"; got != want {
		t.Fatalf("GOT: %v; WANT: %v", got, want)
	}
}

func TestComparableLeafNodeMaybeSplit(t *testing.T) {
	gimme := func() (*comparableLeafNode, *comparableLeafNode) {
		leafB := &comparableLeafNode{
			runts:  cls("b", "bb", "bbb", "bbbb"),
			values: []interface{}{1, 2, 3, 4},
		}
		leafA := &comparableLeafNode{
			runts:  cls("a", "aa", "aaa", "aaaa"),
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
		ensureComparableLeaves(t, leftNode, &comparableLeafNode{
			runts:  cls("a", "aa"),
			values: []interface{}{1, 2},
			next:   rightNode.(*comparableLeafNode),
		})
		ensureComparableLeaves(t, rightNode, &comparableLeafNode{
			runts:  cls("aaa", "aaaa"),
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
		ensureComparableLeaves(t, leftNode, &comparableLeafNode{
			runts:  cls("b", "bb"),
			values: []interface{}{1, 2},
			next:   rightNode.(*comparableLeafNode),
		})
		ensureComparableLeaves(t, rightNode, &comparableLeafNode{
			runts:  cls("bbb", "bbbb"),
			values: []interface{}{3, 4},
			next:   nil,
		})
	})
}

func TestInsertIntoSingleLeafComparableTree(t *testing.T) {
	t.Run("when fewer than order elements", func(t *testing.T) {
		t.Run("when empty", func(t *testing.T) {
			d, _ := NewComparableTree(4)
			ln, ok := d.root.(*comparableLeafNode)
			if !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, false)
			}
			d.Insert(testString("30"), "thirty")
			ensureComparableLeaves(t, ln, &comparableLeafNode{
				runts:  cls("30"),
				values: []interface{}{"thirty"},
			})
		})
		t.Run("when less than first runt", func(t *testing.T) {
			d, _ := NewComparableTree(4)
			ln, ok := d.root.(*comparableLeafNode)
			if !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, false)
			}
			d.Insert(testString("30"), "thirty")
			d.Insert(testString("10"), "ten")
			ensureComparableLeaves(t, ln, &comparableLeafNode{
				runts:  cls("10", "30"),
				values: []interface{}{"ten", "thirty"},
			})
		})
		t.Run("when update value", func(t *testing.T) {
			d, _ := NewComparableTree(4)
			ln, ok := d.root.(*comparableLeafNode)
			if !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, false)
			}
			d.Insert(testString("30"), "thirty")
			d.Insert(testString("10"), "ten")
			d.Insert(testString("30"), "THIRTY")
			ensureComparableLeaves(t, ln, &comparableLeafNode{
				runts:  cls("10", "30"),
				values: []interface{}{"ten", "THIRTY"},
			})
		})
		t.Run("when between first and final runt", func(t *testing.T) {
			d, _ := NewComparableTree(4)
			ln, ok := d.root.(*comparableLeafNode)
			if !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, false)
			}
			d.Insert(testString("30"), "thirty")
			d.Insert(testString("10"), "ten")
			d.Insert(testString("30"), "THIRTY")
			d.Insert(testString("20"), "twenty")
			ensureComparableLeaves(t, ln, &comparableLeafNode{
				runts:  cls("10", "20", "30"),
				values: []interface{}{"ten", "twenty", "THIRTY"},
			})
		})
		t.Run("when after final runt", func(t *testing.T) {
			d, _ := NewComparableTree(4)
			ln, ok := d.root.(*comparableLeafNode)
			if !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, false)
			}
			d.Insert(testString("30"), "thirty")
			d.Insert(testString("10"), "ten")
			d.Insert(testString("30"), "THIRTY")
			d.Insert(testString("20"), "twenty")
			d.Insert(testString("40"), "forty")
			ensureComparableLeaves(t, ln, &comparableLeafNode{
				runts:  cls("10", "20", "30", "40"),
				values: []interface{}{"ten", "twenty", "THIRTY", "forty"},
			})
		})
	})

	t.Run("when insertion splits single leaf node", func(t *testing.T) {
		gimme := func() *ComparableTree {
			d, _ := NewComparableTree(4)
			for k, v := range map[string]string{"10": "ten", "20": "twenty", "30": "thirty", "40": "forty"} {
				d.Insert(testString(k), v)
			}
			// t.Logf("init root runts: %v\ninit root values: %v\n", d.root.(*leaf).runts, d.root.(*leaf).values)
			return d
		}
		t.Run("when new key will be first node in left leaf", func(t *testing.T) {
			d := gimme()
			d.Insert(testString("0"), "zero")
			root, ok := d.root.(*comparableInternalNode)
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
			if got, want := string(root.runts[0].(testString)), "0"; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			ensureComparableLeaves(t, root.children[0], &comparableLeafNode{
				runts:  cls("0", "10", "20"),
				values: []interface{}{"zero", "ten", "twenty"},
				next:   root.children[1].(*comparableLeafNode),
			})

			if got, want := string(root.runts[1].(testString)), "30"; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			ensureComparableLeaves(t, root.children[1], &comparableLeafNode{
				runts:  cls("30", "40"),
				values: []interface{}{"thirty", "forty"},
			})
		})
		t.Run("when new key is in middle", func(t *testing.T) {
			d := gimme()
			d.Insert(testString("25"), "twenty-five")
			root, ok := d.root.(*comparableInternalNode)
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
			if got, want := string(root.runts[0].(testString)), "10"; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			ensureComparableLeaves(t, root.children[0], &comparableLeafNode{
				runts:  cls("10", "20", "25"),
				values: []interface{}{"ten", "twenty", "twenty-five"},
				next:   root.children[1].(*comparableLeafNode),
			})

			if got, want := string(root.runts[1].(testString)), "30"; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			ensureComparableLeaves(t, root.children[1], &comparableLeafNode{
				runts:  cls("30", "40"),
				values: []interface{}{"thirty", "forty"},
			})
		})
		t.Run("when new key will be final node in right leaf", func(t *testing.T) {
			d := gimme()
			d.Insert(testString("50"), "fifty")
			root, ok := d.root.(*comparableInternalNode)
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
			if got, want := string(root.runts[0].(testString)), "10"; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			ensureComparableLeaves(t, root.children[0], &comparableLeafNode{
				runts:  cls("10", "20"),
				values: []interface{}{"ten", "twenty"},
				next:   root.children[1].(*comparableLeafNode),
			})

			if got, want := string(root.runts[1].(testString)), "30"; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			ensureComparableLeaves(t, root.children[1], &comparableLeafNode{
				runts:  cls("30", "40", "50"),
				values: []interface{}{"thirty", "forty", "fifty"},
			})
		})
	})
}

func TestComparableTreeSearch(t *testing.T) {
	t.Run("empty tree", func(t *testing.T) {
		t.Skip("TODO")
	})
	t.Run("single-leaf tree", func(t *testing.T) {
		t.Run("missing value", func(t *testing.T) {
			d, _ := NewComparableTree(16)
			for i := 0; i < 15; i++ {
				if i != 13 {
					d.Insert(testString(strconv.Itoa(i)), i)
				}
			}

			_, ok := d.Search(testString("13"))
			if got, want := ok, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
		})
		t.Run("existing value", func(t *testing.T) {
			d, _ := NewComparableTree(16)
			for i := 0; i < 15; i++ {
				if i != 13 {
					d.Insert(testString(strconv.Itoa(i)), i)
				}
			}

			value, ok := d.Search(testString("8"))
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
			d, _ := NewComparableTree(4)
			for i := 0; i < 15; i++ {
				if i != 13 {
					d.Insert(testString(strconv.Itoa(i)), i)
				}
			}

			_, ok := d.Search(testString("13"))
			if got, want := ok, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
		})
		t.Run("existing value", func(t *testing.T) {
			d, _ := NewComparableTree(4)
			for i := 0; i < 15; i++ {
				if i != 13 {
					d.Insert(testString(strconv.Itoa(i)), i)
				}
			}

			value, ok := d.Search(testString("8"))
			if got, want := ok, true; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := value, 8; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
		})
	})
}

func TestComparableTreeCursor(t *testing.T) {
	t.Run("empty tree", func(t *testing.T) {
		var values []int

		d, _ := NewComparableTree(4)
		c := d.NewScanner(testString(""))
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

			d, _ := NewComparableTree(16)
			for i := 0; i < 15; i++ {
				d.Insert(testString(strconv.Itoa(i)), i)
			}

			c := d.NewScanner(testString(""))
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

			d, _ := NewComparableTree(16)
			for i := 0; i < 15; i++ {
				if i != 13 {
					d.Insert(testString(strconv.Itoa(i)), i)
				}
			}

			c := d.NewScanner(testString("13"))
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

			d, _ := NewComparableTree(16)
			for i := 0; i < 15; i++ {
				d.Insert(testString(strconv.Itoa(i)), i)
			}

			c := d.NewScanner(testString("13"))
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

		d, _ := NewComparableTree(4)
		for i := 0; i < 15; i++ {
			d.Insert(testString(strconv.Itoa(i)), i)
		}

		c := d.NewScanner(testString(""))
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

func TestComparableTreeUpdate(t *testing.T) {
	d, _ := NewComparableTree(8)
	d.Update(testString("A"), func(value interface{}, ok bool) interface{} {
		if got, want := ok, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := value, error(nil); got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		return "first"
	})
	d.Update(testString("A"), func(value interface{}, ok bool) interface{} {
		if got, want := ok, true; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := value, "first"; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		return "second"
	})
	value, ok := d.Search(testString("A"))
	if got, want := ok, true; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := value, "second"; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	d.Insert(testString("C"), "third")
	d.Update(testString("B"), func(value interface{}, ok bool) interface{} {
		if got, want := ok, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := value, error(nil); got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		return "fourth"
	})
	value, ok = d.Search(testString("B"))
	if got, want := ok, true; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := value, "fourth"; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
}
