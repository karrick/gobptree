package gobptree

import (
	"fmt"
	"strconv"
	"testing"
)

func TestStringBinarySearch(t *testing.T) {
	t.Run("skip values", func(t *testing.T) {
		values := []string{"b", "d", "f", "h", "j", "l", "n", "p", "r", "t", "v", "x"}

		if got, want := stringSearchGreaterThanOrEqualTo("a", values), 0; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := stringSearchGreaterThanOrEqualTo("b", values), 0; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := stringSearchGreaterThanOrEqualTo("c", values), 1; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := stringSearchGreaterThanOrEqualTo("d", values), 1; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := stringSearchGreaterThanOrEqualTo("e", values), 2; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := stringSearchGreaterThanOrEqualTo("f", values), 2; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := stringSearchGreaterThanOrEqualTo("g", values), 3; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := stringSearchGreaterThanOrEqualTo("h", values), 3; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := stringSearchGreaterThanOrEqualTo("i", values), 4; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := stringSearchGreaterThanOrEqualTo("j", values), 4; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := stringSearchGreaterThanOrEqualTo("k", values), 5; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := stringSearchGreaterThanOrEqualTo("l", values), 5; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := stringSearchGreaterThanOrEqualTo("m", values), 6; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := stringSearchGreaterThanOrEqualTo("n", values), 6; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := stringSearchGreaterThanOrEqualTo("o", values), 7; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := stringSearchGreaterThanOrEqualTo("p", values), 7; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := stringSearchGreaterThanOrEqualTo("q", values), 8; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := stringSearchGreaterThanOrEqualTo("r", values), 8; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := stringSearchGreaterThanOrEqualTo("s", values), 9; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := stringSearchGreaterThanOrEqualTo("t", values), 9; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := stringSearchGreaterThanOrEqualTo("u", values), 10; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := stringSearchGreaterThanOrEqualTo("v", values), 10; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := stringSearchGreaterThanOrEqualTo("w", values), 11; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := stringSearchGreaterThanOrEqualTo("x", values), 11; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := stringSearchGreaterThanOrEqualTo("y", values), 11; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := stringSearchGreaterThanOrEqualTo("z", values), 11; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
	})
	t.Run("greater than or equal to", func(t *testing.T) {
		t.Run("empty list", func(t *testing.T) {
			i := stringSearchGreaterThanOrEqualTo("a", nil)
			if got, want := i, 0; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
		})
		t.Run("single item list", func(t *testing.T) {
			t.Run("key before", func(t *testing.T) {
				i := stringSearchGreaterThanOrEqualTo("a", []string{"b"})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match", func(t *testing.T) {
				i := stringSearchGreaterThanOrEqualTo("b", []string{"b"})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key after", func(t *testing.T) {
				i := stringSearchGreaterThanOrEqualTo("c", []string{"b"})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
		})
		t.Run("multiple item list", func(t *testing.T) {
			t.Run("key before first", func(t *testing.T) {
				i := stringSearchGreaterThanOrEqualTo("a", []string{"b", "d", "f"})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match first", func(t *testing.T) {
				i := stringSearchGreaterThanOrEqualTo("b", []string{"b", "d", "f"})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key between first and second", func(t *testing.T) {
				i := stringSearchGreaterThanOrEqualTo("c", []string{"b", "d", "f"})
				if got, want := i, 1; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match second", func(t *testing.T) {
				i := stringSearchGreaterThanOrEqualTo("d", []string{"b", "d", "f"})
				if got, want := i, 1; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key between second and third", func(t *testing.T) {
				i := stringSearchGreaterThanOrEqualTo("e", []string{"b", "d", "f"})
				if got, want := i, 2; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match third", func(t *testing.T) {
				i := stringSearchGreaterThanOrEqualTo("f", []string{"b", "d", "f"})
				if got, want := i, 2; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key after third", func(t *testing.T) {
				i := stringSearchGreaterThanOrEqualTo("g", []string{"b", "d", "f"})
				if got, want := i, 2; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
		})
	})
	t.Run("less than or equal to", func(t *testing.T) {
		t.Run("empty list", func(t *testing.T) {
			i := stringSearchLessThanOrEqualTo("a", nil)
			if got, want := i, 0; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
		})
		t.Run("single item list", func(t *testing.T) {
			t.Run("key before", func(t *testing.T) {
				i := stringSearchLessThanOrEqualTo("a", []string{"b"})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match", func(t *testing.T) {
				i := stringSearchLessThanOrEqualTo("b", []string{"b"})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key after", func(t *testing.T) {
				i := stringSearchLessThanOrEqualTo("c", []string{"b"})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
		})
		t.Run("multiple item list", func(t *testing.T) {
			t.Run("key before first", func(t *testing.T) {
				i := stringSearchLessThanOrEqualTo("a", []string{"b", "d", "f"})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match first", func(t *testing.T) {
				i := stringSearchLessThanOrEqualTo("2", []string{"b", "d", "f"})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key between first and second", func(t *testing.T) {
				i := stringSearchLessThanOrEqualTo("c", []string{"b", "d", "f"})
				if got, want := i, 0; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match second", func(t *testing.T) {
				i := stringSearchLessThanOrEqualTo("d", []string{"b", "d", "f"})
				if got, want := i, 1; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key between second and third", func(t *testing.T) {
				i := stringSearchLessThanOrEqualTo("e", []string{"b", "d", "f"})
				if got, want := i, 1; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key match third", func(t *testing.T) {
				i := stringSearchLessThanOrEqualTo("f", []string{"b", "d", "f"})
				if got, want := i, 2; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("key after third", func(t *testing.T) {
				i := stringSearchLessThanOrEqualTo("g", []string{"b", "d", "f"})
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

func stringLeafFrom(next *stringLeafNode, items ...string) *stringLeafNode {
	n := &stringLeafNode{
		runts:  make([]string, len(items)),
		values: make([]interface{}, len(items)),
		next:   next,
	}
	for i := 0; i < len(items); i++ {
		n.runts[i] = items[i]
		n.values[i] = items[i]
	}
	return n
}

func stringInternalFrom(items ...stringNode) *stringInternalNode {
	n := &stringInternalNode{
		runts:    make([]string, len(items)),
		children: make([]stringNode, len(items)),
	}
	for i := 0; i < len(items); i++ {
		n.runts[i] = items[i].smallest()
		n.children[i] = items[i]
	}
	return n
}

////////////////////////////////////////

func ensureStringLeaf(t *testing.T, actual, expected *stringLeafNode) {
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
	// ensureStringLeaf(t, actual.next, expected.next)
	if got, want := actual.next, expected.next; got != want {
		t.Errorf("next GOT: %v; WANT: %v", got, want)
	}
	if t.Failed() {
		t.Errorf("\nGOT:\n\t%#v\nWANT:\n\t%#v", actual, expected)
	}
}

func ensureStringInternal(t *testing.T, a, e *stringInternalNode) {
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
		ensureStringNode(t, a.children[i], e.children[i])
	}
}

func ensureStringNode(t *testing.T, actual, expected stringNode) {
	t.Helper()

	switch e := expected.(type) {
	case *stringLeafNode:
		a, ok := actual.(*stringLeafNode)
		if got, want := ok, true; got != want {
			t.Errorf("GOT: %T; WANT: %T", actual, e)
		}
		ensureStringLeaf(t, a, e)
	case *stringInternalNode:
		a, ok := actual.(*stringInternalNode)
		if got, want := ok, true; got != want {
			t.Errorf("GOT: %T; WANT: %T", actual, e)
		}
		ensureStringInternal(t, a, e)
	default:
		t.Errorf("GOT: %T; WANT: stringNode", expected)
	}
}

////////////////////////////////////////

func TestStringInternalNodeMaybeSplit(t *testing.T) {
	leafD := stringLeafFrom(nil, "40", "41", "42", "43")
	leafC := stringLeafFrom(leafD, "30", "31", "32", "33")
	leafB := stringLeafFrom(leafC, "20", "21", "22", "23")
	leafA := stringLeafFrom(leafB, "10", "11", "12", "13")

	ni := stringInternalFrom(leafA, leafB, leafC, leafD)

	t.Run("does nothing when not full", func(t *testing.T) {
		_, right := ni.maybeSplit(6)
		if right != nil {
			t.Errorf("GOT: %v; WANT: %v", right, nil)
		}
	})

	t.Run("splits when full", func(t *testing.T) {
		expectedLeft := stringInternalFrom(leafA, leafB)
		expectedRight := stringInternalFrom(leafC, leafD)

		leftNode, rightNode := ni.maybeSplit(4)

		ensureStringNode(t, leftNode, expectedLeft)
		ensureStringNode(t, rightNode, expectedRight)
	})
}

func TestInternalStringNodeInsertSmallerKey(t *testing.T) {
	gimme := func() (*stringLeafNode, *stringLeafNode) {
		leafB := stringLeafFrom(nil, "21", "22")
		leafA := stringLeafFrom(leafB, "12", "13")
		return leafA, leafB
	}

	leafA, leafB := gimme()
	ni := stringInternalFrom(leafA, leafB)

	d := &StringTree{root: ni, order: 4}

	d.Insert("11", "11")

	if got, want := ni.runts[0], "11"; got != want {
		t.Fatalf("GOT: %v; WANT: %v", got, want)
	}
}

func TestStringLeafNodeMaybeSplit(t *testing.T) {
	gimme := func() (*stringLeafNode, *stringLeafNode) {
		leafB := stringLeafFrom(nil, "21", "22", "23", "24")
		leafA := stringLeafFrom(leafB, "11", "12", "13", "14")
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
		ensureStringNode(t, leftNode, stringLeafFrom(rightNode.(*stringLeafNode), "11", "12"))
		ensureStringNode(t, rightNode, stringLeafFrom(leafB, "13", "14"))
	})
	t.Run("splits right edge when full", func(t *testing.T) {
		leafA, leafB := gimme()
		leftNode, rightNode := leafB.maybeSplit(4)
		if got, want := leafA.next, leftNode; got != want {
			t.Fatalf("GOT: %v; WANT: %v", got, want)
		}
		ensureStringNode(t, leftNode, stringLeafFrom(rightNode.(*stringLeafNode), "21", "22"))
		ensureStringNode(t, rightNode, stringLeafFrom(nil, "23", "24"))
	})
}

func TestInsertIntoSingleLeafStringTree(t *testing.T) {
	t.Run("when fewer than order elements", func(t *testing.T) {
		t.Run("when empty", func(t *testing.T) {
			d, _ := NewStringTree(4)
			nl, ok := d.root.(*stringLeafNode)
			if !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, false)
			}
			d.Insert("30", "30")
			ensureStringLeaf(t, nl, stringLeafFrom(nil, "30"))
		})
		t.Run("when less than first runt", func(t *testing.T) {
			d, _ := NewStringTree(4)
			nl, ok := d.root.(*stringLeafNode)
			if !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, false)
			}
			d.Insert("30", "30")
			d.Insert("10", "10")
			ensureStringNode(t, nl, stringLeafFrom(nil, "10", "30"))
		})
		t.Run("when update value", func(t *testing.T) {
			d, _ := NewStringTree(4)
			nl, ok := d.root.(*stringLeafNode)
			if !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, false)
			}
			d.Insert("30", "30")
			d.Insert("10", "10")
			d.Insert("30", "333")
			ensureStringNode(t, nl, &stringLeafNode{
				runts:  []string{"10", "30"},
				values: []interface{}{"10", "333"},
			})
		})
		t.Run("when between first and final runt", func(t *testing.T) {
			d, _ := NewStringTree(4)
			nl, ok := d.root.(*stringLeafNode)
			if !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, false)
			}
			d.Insert("30", "30")
			d.Insert("10", "10")
			d.Insert("20", "20")
			ensureStringNode(t, nl, stringLeafFrom(nil, "10", "20", "30"))
		})
		t.Run("when after final runt", func(t *testing.T) {
			d, _ := NewStringTree(4)
			nl, ok := d.root.(*stringLeafNode)
			if !ok {
				t.Fatalf("GOT: %v; WANT: %v", ok, false)
			}
			d.Insert("30", "30")
			d.Insert("10", "10")
			d.Insert("20", "20")
			d.Insert("40", "40")
			ensureStringNode(t, nl, stringLeafFrom(nil, "10", "20", "30", "40"))
		})
	})

	t.Run("when insertion splits single leaf node", func(t *testing.T) {
		gimme := func() *StringTree {
			d, _ := NewStringTree(4)
			for _, v := range []string{"10", "20", "30", "40"} {
				d.Insert(v, v)
			}
			return d
		}
		t.Run("when new key will be first node in left leaf", func(t *testing.T) {
			d := gimme()
			d.Insert("0", "0")

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
			if got, want := root.runts[0], "0"; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			ensureStringNode(t, root.children[0], stringLeafFrom(root.children[1].(*stringLeafNode), "0", "10", "20"))

			if got, want := root.runts[1], "30"; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			ensureStringNode(t, root.children[1], stringLeafFrom(nil, "30", "40"))
		})
		t.Run("when new key is in middle", func(t *testing.T) {
			d := gimme()
			d.Insert("25", "25")
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
			if got, want := root.runts[0], "10"; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			ensureStringNode(t, root.children[0], stringLeafFrom(root.children[1].(*stringLeafNode), "10", "20", "25"))

			if got, want := root.runts[1], "30"; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			ensureStringNode(t, root.children[1], stringLeafFrom(nil, "30", "40"))
		})
		t.Run("when new key will be final node in right leaf", func(t *testing.T) {
			d := gimme()
			d.Insert("50", "50")
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
			if got, want := root.runts[0], "10"; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			ensureStringNode(t, root.children[0], stringLeafFrom(root.children[1].(*stringLeafNode), "10", "20"))

			if got, want := root.runts[1], "30"; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
			ensureStringNode(t, root.children[1], stringLeafFrom(nil, "30", "40", "50"))
		})
	})
}

func TestStringTreeSearch(t *testing.T) {
	t.Run("empty tree", func(t *testing.T) {
		d, _ := NewStringTree(16)

		_, ok := d.Search("13")
		if got, want := ok, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
	})
	t.Run("single-leaf tree", func(t *testing.T) {
		t.Run("missing value", func(t *testing.T) {
			d, _ := NewStringTree(16)
			for i := 0; i < 15; i++ {
				if i != 13 {
					d.Insert(strconv.Itoa(i), strconv.Itoa(i))
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
					d.Insert(strconv.Itoa(i), strconv.Itoa(i))
				}
			}

			value, ok := d.Search("8")
			if got, want := ok, true; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := value, "8"; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
		})
	})
	t.Run("multi-leaf tree", func(t *testing.T) {
		t.Run("missing value", func(t *testing.T) {
			d, _ := NewStringTree(4)
			for i := 0; i < 15; i++ {
				if i != 13 {
					d.Insert(strconv.Itoa(i), strconv.Itoa(i))
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
					d.Insert(strconv.Itoa(i), strconv.Itoa(i))
				}
			}

			value, ok := d.Search("8")
			if got, want := ok, true; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := value, "8"; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
		})
	})
}

func TestStringTreeCursor(t *testing.T) {
	t.Run("empty tree", func(t *testing.T) {
		var count int

		d, _ := NewStringTree(4)
		c := d.NewScanner("0")
		for c.Scan() {
			count++
		}

		if got, want := count, 0; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
	})
	t.Run("single-leaf tree", func(t *testing.T) {
		t.Run("scan for zero-value element", func(t *testing.T) {
			var values []string

			d, _ := NewStringTree(16)
			for i := 0; i < 15; i++ {
				d.Insert(strconv.Itoa(i), strconv.Itoa(i))
			}

			c := d.NewScanner("0")
			for c.Scan() {
				_, v := c.Pair()
				values = append(values, v.(string))
			}

			expected := []string{"0", "1", "10", "11", "12", "13", "14", "2", "3", "4", "5", "6", "7", "8", "9"}

			for i := 0; i < len(values) && i < len(expected); i++ {
				if got, want := values[i], expected[i]; got != want {
					t.Errorf("GOT: %v; WANT: %v", got, want)
				}
			}
		})
		t.Run("scan for missing element", func(t *testing.T) {
			var values []string

			d, _ := NewStringTree(16)
			for i := 0; i < 15; i++ {
				if i != 13 {
					d.Insert(strconv.Itoa(i), strconv.Itoa(i))
				}
			}

			c := d.NewScanner("13")
			for c.Scan() {
				_, v := c.Pair()
				values = append(values, v.(string))
			}

			expected := []string{"14", "2", "3", "4", "5", "6", "7", "8", "9"}

			for i := 0; i < len(values) && i < len(expected); i++ {
				if got, want := values[i], expected[i]; got != want {
					t.Errorf("GOT: %v; WANT: %v", got, want)
				}
			}
		})
		t.Run("scan for existing element", func(t *testing.T) {
			var values []string

			d, _ := NewStringTree(16)
			for i := 0; i < 15; i++ {
				d.Insert(strconv.Itoa(i), strconv.Itoa(i))
			}

			c := d.NewScanner("13")
			for c.Scan() {
				_, v := c.Pair()
				values = append(values, v.(string))
			}

			expected := []string{"13", "14", "2", "3", "4", "5", "6", "7", "8", "9"}

			for i := 0; i < len(values) && i < len(expected); i++ {
				if got, want := values[i], expected[i]; got != want {
					t.Errorf("GOT: %v; WANT: %v", got, want)
				}
			}
		})
	})
	t.Run("multi-leaf tree", func(t *testing.T) {
		var values []string

		d, _ := NewStringTree(4)
		for i := 0; i < 15; i++ {
			d.Insert(strconv.Itoa(i), strconv.Itoa(i))
		}

		c := d.NewScanner("0")
		for c.Scan() {
			_, v := c.Pair()
			values = append(values, v.(string))
		}

		expected := []string{"0", "1", "10", "11", "12", "13", "14", "2", "3", "4", "5", "6", "7", "8", "9"}

		for i := 0; i < len(values) && i < len(expected); i++ {
			if got, want := values[i], expected[i]; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
		}
	})
}

func TestStringTreeUpdate(t *testing.T) {
	d, _ := NewStringTree(8)
	d.Update("1", func(value interface{}, ok bool) interface{} {
		if got, want := ok, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := value, error(nil); got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		return "first"
	})
	d.Update("1", func(value interface{}, ok bool) interface{} {
		if got, want := ok, true; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := value, "first"; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		return "second"
	})
	value, ok := d.Search("1")
	if got, want := ok, true; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := value, "second"; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	d.Insert("3", "3")
	d.Update("2", func(value interface{}, ok bool) interface{} {
		if got, want := ok, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := value, error(nil); got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		return "fourth"
	})
	value, ok = d.Search("2")
	if got, want := ok, true; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := value, "fourth"; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
}

func TestStringLeafNodeDelete(t *testing.T) {
	t.Run("still big enough", func(t *testing.T) {
		t.Run("key is missing", func(t *testing.T) {
			l := &stringLeafNode{
				runts:  []string{"11", "21", "31"},
				values: []interface{}{"11", "21", "31"},
			}
			tooSmall := l.deleteKey(2, "42")
			if got, want := tooSmall, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureStringNode(t, l, &stringLeafNode{
				runts:  []string{"11", "21", "31"},
				values: []interface{}{"11", "21", "31"},
			})
		})
		t.Run("key is first", func(t *testing.T) {
			l := &stringLeafNode{
				runts:  []string{"11", "21", "31"},
				values: []interface{}{"11", "21", "31"},
			}
			tooSmall := l.deleteKey(2, "11")
			if got, want := tooSmall, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureStringNode(t, l, &stringLeafNode{
				runts:  []string{"21", "31"},
				values: []interface{}{"21", "31"},
			})
		})
		t.Run("key is middle", func(t *testing.T) {
			l := &stringLeafNode{
				runts:  []string{"11", "21", "31"},
				values: []interface{}{"11", "21", "31"},
			}
			tooSmall := l.deleteKey(2, "21")
			if got, want := tooSmall, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureStringNode(t, l, &stringLeafNode{
				runts:  []string{"11", "31"},
				values: []interface{}{"11", "31"},
			})
		})
		t.Run("key is last", func(t *testing.T) {
			l := &stringLeafNode{
				runts:  []string{"11", "21", "31"},
				values: []interface{}{"11", "21", "31"},
			}
			tooSmall := l.deleteKey(2, "31")
			if got, want := tooSmall, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureStringNode(t, l, &stringLeafNode{
				runts:  []string{"11", "21"},
				values: []interface{}{"11", "21"},
			})
		})
	})
	t.Run("will be too small", func(t *testing.T) {
		l := stringLeafFrom(nil, "11", "21", "31", "41")
		tooSmall := l.deleteKey(4, "21")
		if got, want := tooSmall, true; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		ensureStringNode(t, l, stringLeafFrom(nil, "11", "31", "41"))
	})
}

func TestStringLeafNodeAdoptFrom(t *testing.T) {
	t.Run("left", func(t *testing.T) {
		r := stringLeafFrom(nil, "5", "6", "7")
		l := stringLeafFrom(r, "0", "1", "2", "3", "4")

		r.adoptFromLeft(l)

		ensureStringNode(t, l, stringLeafFrom(r, "0", "1", "2", "3"))
		ensureStringNode(t, r, stringLeafFrom(nil, "4", "5", "6", "7"))
	})
	t.Run("right", func(t *testing.T) {
		r := stringLeafFrom(nil, "3", "4", "5", "6", "7")
		l := stringLeafFrom(r, "0", "1", "2")

		l.adoptFromRight(r)

		ensureStringNode(t, l, stringLeafFrom(r, "0", "1", "2", "3"))
		ensureStringNode(t, r, stringLeafFrom(nil, "4", "5", "6", "7"))
	})
}

func TestStringInternalNodeAdoptFrom(t *testing.T) {
	t.Run("left", func(t *testing.T) {
		leafI := stringLeafFrom(nil, "90", "92", "94", "96", "98")
		leafH := stringLeafFrom(leafI, "80", "82", "84", "86", "88")
		leafG := stringLeafFrom(leafH, "70", "72", "74", "76", "78")
		leafF := stringLeafFrom(leafG, "60", "62", "64", "66", "68")
		leafE := stringLeafFrom(leafF, "50", "52", "54", "56", "58")
		leafD := stringLeafFrom(leafE, "40", "42", "44", "46", "48")
		leafC := stringLeafFrom(leafD, "30", "32", "34", "36", "38")
		leafB := stringLeafFrom(leafC, "20", "22", "24", "26", "28")
		leafA := stringLeafFrom(leafB, "10", "12", "14", "16", "18")

		left := stringInternalFrom(leafA, leafB, leafC, leafD, leafE, leafF)
		right := stringInternalFrom(leafG, leafH, leafI)

		right.adoptFromLeft(left)

		ensureStringInternal(t, left, stringInternalFrom(leafA, leafB, leafC, leafD, leafE))
		ensureStringInternal(t, right, stringInternalFrom(leafF, leafG, leafH, leafI))
	})
	t.Run("right", func(t *testing.T) {
		leafI := stringLeafFrom(nil, "90", "92", "94", "96", "98")
		leafH := stringLeafFrom(leafI, "80", "82", "84", "86", "88")
		leafG := stringLeafFrom(leafH, "70", "72", "74", "76", "78")
		leafF := stringLeafFrom(leafG, "60", "62", "64", "66", "68")
		leafE := stringLeafFrom(leafF, "50", "52", "54", "56", "58")
		leafD := stringLeafFrom(leafE, "40", "42", "44", "46", "48")
		leafC := stringLeafFrom(leafD, "30", "32", "34", "36", "38")
		leafB := stringLeafFrom(leafC, "20", "22", "24", "26", "28")
		leafA := stringLeafFrom(leafB, "10", "12", "14", "16", "18")

		left := stringInternalFrom(leafA, leafB, leafC)
		right := stringInternalFrom(leafD, leafE, leafF, leafG, leafH, leafI)

		left.adoptFromRight(right)

		ensureStringInternal(t, left, stringInternalFrom(leafA, leafB, leafC, leafD))
		ensureStringInternal(t, right, stringInternalFrom(leafE, leafF, leafG, leafH, leafI))
	})
}

func TestStringLeafNodeMergeWithRight(t *testing.T) {
	leafC := stringLeafFrom(nil, "6", "7", "8", "9")
	leafB := stringLeafFrom(leafC, "3", "4", "5")
	leafA := stringLeafFrom(leafB, "0", "1", "2")

	leafA.absorbRight(leafB)

	ensureStringNode(t, leafA, stringLeafFrom(leafC, "0", "1", "2", "3", "4", "5"))

	if got, want := len(leafB.runts), 0; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := len(leafB.values), 0; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := leafB.next, (*stringLeafNode)(nil); got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
}

func TestStringInternalNodeMergeWithRight(t *testing.T) {
	leafI := stringLeafFrom(nil, "90", "92", "94", "96", "98")
	leafH := stringLeafFrom(leafI, "80", "82", "84", "86", "88")
	leafG := stringLeafFrom(leafH, "70", "72", "74", "76", "78")
	leafF := stringLeafFrom(leafG, "60", "62", "64", "66", "68")
	leafE := stringLeafFrom(leafF, "50", "52", "54", "56", "58")
	leafD := stringLeafFrom(leafE, "40", "42", "44", "46", "48")
	leafC := stringLeafFrom(leafD, "30", "32", "34", "36", "38")
	leafB := stringLeafFrom(leafC, "20", "22", "24", "26", "28")
	leafA := stringLeafFrom(leafB, "10", "12", "14", "16", "18")

	left := stringInternalFrom(leafA, leafB, leafC)
	right := stringInternalFrom(leafD, leafE, leafF, leafG)

	left.absorbRight(right)

	ensureStringInternal(t, left, stringInternalFrom(leafA, leafB, leafC, leafD, leafE, leafF, leafG))

	if got, want := len(right.runts), 0; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := len(right.children), 0; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
}

func TestStringInternalNodeDeleteKey(t *testing.T) {
	t.Run("not too small", func(t *testing.T) {
		leafE := stringLeafFrom(nil, "50", "52", "54", "56", "58")
		leafD := stringLeafFrom(leafE, "40", "42", "44", "46", "48")
		leafC := stringLeafFrom(leafD, "30", "32", "34", "36", "38")
		leafB := stringLeafFrom(leafC, "20", "22", "24", "26", "28")
		leafA := stringLeafFrom(leafB, "10", "12", "14", "16", "18")

		child := stringInternalFrom(leafA, leafB, leafC, leafD)

		if got, want := child.deleteKey(4, "22"), false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
	})
	t.Run("child absorbs right when no left and skinny right", func(t *testing.T) {
		t.Run("child not too small", func(t *testing.T) {
			leafE := stringLeafFrom(nil, "50", "52", "54", "56", "58")
			leafD := stringLeafFrom(leafE, "40", "42", "44", "46", "48")
			leafC := stringLeafFrom(leafD, "30", "32", "34", "36", "38")
			leafB := stringLeafFrom(leafC, "20", "22", "24", "26")
			leafA := stringLeafFrom(leafB, "10", "12", "14", "16")

			child := stringInternalFrom(leafA, leafB, leafC, leafD, leafE)

			tooSmall := child.deleteKey(4, "12")
			if got, want := tooSmall, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}

			ensureStringLeaf(t, leafA, stringLeafFrom(leafC, "10", "14", "16", "20", "22", "24", "26"))
			if got, want := len(leafB.runts), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := len(leafB.values), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureStringLeaf(t, leafC, stringLeafFrom(leafD, "30", "32", "34", "36", "38"))
			ensureStringLeaf(t, leafD, stringLeafFrom(leafE, "40", "42", "44", "46", "48"))
			ensureStringLeaf(t, leafE, stringLeafFrom(nil, "50", "52", "54", "56", "58"))
		})
		t.Run("child too small", func(t *testing.T) {
			leafD := stringLeafFrom(nil, "40", "42", "44", "46", "48")
			leafC := stringLeafFrom(leafD, "30", "32", "34", "36", "38")
			leafB := stringLeafFrom(leafC, "20", "22", "24", "26")
			leafA := stringLeafFrom(leafB, "10", "12", "14", "16")

			child := stringInternalFrom(leafA, leafB, leafC, leafD)

			tooSmall := child.deleteKey(4, "12")
			if got, want := tooSmall, true; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}

			ensureStringLeaf(t, leafA, stringLeafFrom(leafC, "10", "14", "16", "20", "22", "24", "26"))
			if got, want := len(leafB.runts), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := len(leafB.values), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureStringLeaf(t, leafC, stringLeafFrom(leafD, "30", "32", "34", "36", "38"))
			ensureStringLeaf(t, leafD, stringLeafFrom(nil, "40", "42", "44", "46", "48"))
		})
	})
	t.Run("child adopts from right when no left and fat right", func(t *testing.T) {
		leafE := stringLeafFrom(nil, "50", "52", "54", "56", "58")
		leafD := stringLeafFrom(leafE, "40", "42", "44", "46", "48")
		leafC := stringLeafFrom(leafD, "30", "32", "34", "36", "38")
		leafB := stringLeafFrom(leafC, "20", "22", "24", "26", "28")
		leafA := stringLeafFrom(leafB, "10", "12", "14", "16")

		child := stringInternalFrom(leafA, leafB, leafC, leafD, leafE)

		tooSmall := child.deleteKey(4, "12")
		if got, want := tooSmall, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}

		ensureStringLeaf(t, leafA, stringLeafFrom(leafB, "10", "14", "16", "20"))
		ensureStringLeaf(t, leafB, stringLeafFrom(leafC, "22", "24", "26", "28"))
		ensureStringLeaf(t, leafC, stringLeafFrom(leafD, "30", "32", "34", "36", "38"))
		ensureStringLeaf(t, leafD, stringLeafFrom(leafE, "40", "42", "44", "46", "48"))
		ensureStringLeaf(t, leafE, stringLeafFrom(nil, "50", "52", "54", "56", "58"))
	})
	t.Run("left absorbs child when skinny left and no right", func(t *testing.T) {
		t.Run("too small", func(t *testing.T) {
			leafD := stringLeafFrom(nil, "40", "42", "44", "46")
			leafC := stringLeafFrom(leafD, "30", "32", "34", "36")
			leafB := stringLeafFrom(leafC, "20", "22", "24", "26")
			leafA := stringLeafFrom(leafB, "10", "12", "14", "16")

			child := stringInternalFrom(leafA, leafB, leafC, leafD)

			tooSmall := child.deleteKey(4, "42")
			if got, want := tooSmall, true; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}

			ensureStringLeaf(t, leafA, stringLeafFrom(leafB, "10", "12", "14", "16"))
			ensureStringLeaf(t, leafB, stringLeafFrom(leafC, "20", "22", "24", "26"))
			ensureStringLeaf(t, leafC, stringLeafFrom(nil, "30", "32", "34", "36", "40", "44", "46"))
			if got, want := len(leafD.runts), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := len(leafD.values), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
		})
		t.Run("not too small", func(t *testing.T) {
			leafE := stringLeafFrom(nil, "50", "52", "54", "56")
			leafD := stringLeafFrom(leafE, "40", "42", "44", "46")
			leafC := stringLeafFrom(leafD, "30", "32", "34", "36")
			leafB := stringLeafFrom(leafC, "20", "22", "24", "26")
			leafA := stringLeafFrom(leafB, "10", "12", "14", "16")

			child := stringInternalFrom(leafA, leafB, leafC, leafD, leafE)

			tooSmall := child.deleteKey(4, "52")
			if got, want := tooSmall, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}

			ensureStringLeaf(t, leafA, stringLeafFrom(leafB, "10", "12", "14", "16"))
			ensureStringLeaf(t, leafB, stringLeafFrom(leafC, "20", "22", "24", "26"))
			ensureStringLeaf(t, leafC, stringLeafFrom(leafD, "30", "32", "34", "36"))
			ensureStringLeaf(t, leafD, stringLeafFrom(nil, "40", "42", "44", "46", "50", "54", "56"))
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
			leafC := stringLeafFrom(nil, "30", "32", "34", "36")
			leafB := stringLeafFrom(leafC, "20", "22", "24", "26")
			leafA := stringLeafFrom(leafB, "10", "12", "14", "16")

			child := stringInternalFrom(leafA, leafB, leafC)

			tooSmall := child.deleteKey(4, "22")
			if got, want := tooSmall, true; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}

			ensureStringLeaf(t, leafA, stringLeafFrom(leafC, "10", "12", "14", "16", "20", "24", "26"))
			if got, want := len(leafB.runts), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := len(leafB.values), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureStringLeaf(t, leafC, stringLeafFrom(nil, "30", "32", "34", "36"))
		})
		t.Run("not too small", func(t *testing.T) {
			leafE := stringLeafFrom(nil, "50", "52", "54", "56")
			leafD := stringLeafFrom(leafE, "40", "42", "44", "46")
			leafC := stringLeafFrom(leafD, "30", "32", "34", "36")
			leafB := stringLeafFrom(leafC, "20", "22", "24", "26")
			leafA := stringLeafFrom(leafB, "10", "12", "14", "16")

			child := stringInternalFrom(leafA, leafB, leafC, leafD, leafE)

			tooSmall := child.deleteKey(4, "22")
			if got, want := tooSmall, false; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}

			ensureStringLeaf(t, leafA, stringLeafFrom(leafC, "10", "12", "14", "16", "20", "24", "26"))
			if got, want := len(leafB.runts), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			if got, want := len(leafB.values), 0; got != want {
				t.Errorf("GOT: %v; WANT: %v", got, want)
			}
			ensureStringLeaf(t, leafC, stringLeafFrom(leafD, "30", "32", "34", "36"))
			ensureStringLeaf(t, leafD, stringLeafFrom(leafE, "40", "42", "44", "46"))
		})
	})
	t.Run("child adopts from right when skinny left and fat right", func(t *testing.T) {
		leafE := stringLeafFrom(nil, "50", "52", "54", "56", "58")
		leafD := stringLeafFrom(leafE, "40", "42", "44", "46", "48")
		leafC := stringLeafFrom(leafD, "30", "32", "34", "36", "38")
		leafB := stringLeafFrom(leafC, "20", "22", "24", "26")
		leafA := stringLeafFrom(leafB, "10", "12", "14", "16")

		child := stringInternalFrom(leafA, leafB, leafC, leafD, leafE)

		tooSmall := child.deleteKey(4, "22")
		if got, want := tooSmall, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}

		ensureStringLeaf(t, leafA, stringLeafFrom(leafB, "10", "12", "14", "16"))
		ensureStringLeaf(t, leafB, stringLeafFrom(leafC, "20", "24", "26", "30"))
		ensureStringLeaf(t, leafC, stringLeafFrom(leafD, "32", "34", "36", "38"))
		ensureStringLeaf(t, leafD, stringLeafFrom(leafE, "40", "42", "44", "46", "48"))
		ensureStringLeaf(t, leafE, stringLeafFrom(nil, "50", "52", "54", "56", "58"))
	})
	t.Run("child adopts from left when fat left and no right", func(t *testing.T) {
		leafE := stringLeafFrom(nil, "50", "52", "54", "56")
		leafD := stringLeafFrom(leafE, "40", "42", "44", "46", "48")
		leafC := stringLeafFrom(leafD, "30", "32", "34", "36")
		leafB := stringLeafFrom(leafC, "20", "22", "24", "26")
		leafA := stringLeafFrom(leafB, "10", "12", "14", "16")

		child := stringInternalFrom(leafA, leafB, leafC, leafD, leafE)

		tooSmall := child.deleteKey(4, "52")
		if got, want := tooSmall, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}

		ensureStringLeaf(t, leafA, stringLeafFrom(leafB, "10", "12", "14", "16"))
		ensureStringLeaf(t, leafB, stringLeafFrom(leafC, "20", "22", "24", "26"))
		ensureStringLeaf(t, leafC, stringLeafFrom(leafD, "30", "32", "34", "36"))
		ensureStringLeaf(t, leafD, stringLeafFrom(leafE, "40", "42", "44", "46"))
		ensureStringLeaf(t, leafE, stringLeafFrom(nil, "48", "50", "54", "56"))
	})
	t.Run("child adopts from left when fat left and skinny right", func(t *testing.T) {
		leafE := stringLeafFrom(nil, "50", "52", "54", "56")
		leafD := stringLeafFrom(leafE, "40", "42", "44", "46")
		leafC := stringLeafFrom(leafD, "30", "32", "34", "36")
		leafB := stringLeafFrom(leafC, "20", "22", "24", "26", "28")
		leafA := stringLeafFrom(leafB, "10", "12", "14", "16")

		child := stringInternalFrom(leafA, leafB, leafC, leafD, leafE)

		tooSmall := child.deleteKey(4, "32")
		if got, want := tooSmall, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}

		ensureStringLeaf(t, leafA, stringLeafFrom(leafB, "10", "12", "14", "16"))
		ensureStringLeaf(t, leafB, stringLeafFrom(leafC, "20", "22", "24", "26"))
		ensureStringLeaf(t, leafC, stringLeafFrom(leafD, "28", "30", "34", "36"))
		ensureStringLeaf(t, leafD, stringLeafFrom(leafE, "40", "42", "44", "46"))
		ensureStringLeaf(t, leafE, stringLeafFrom(nil, "50", "52", "54", "56"))
	})
	t.Run("child adopts from right when fat left and fat right", func(t *testing.T) {
		leafE := stringLeafFrom(nil, "50", "52", "54", "56", "58")
		leafD := stringLeafFrom(leafE, "40", "42", "44", "46", "48")
		leafC := stringLeafFrom(leafD, "30", "32", "34", "36")
		leafB := stringLeafFrom(leafC, "20", "22", "24", "26", "28")
		leafA := stringLeafFrom(leafB, "10", "12", "14", "16", "18")

		child := stringInternalFrom(leafA, leafB, leafC, leafD, leafE)

		tooSmall := child.deleteKey(4, "32")
		if got, want := tooSmall, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}

		ensureStringLeaf(t, leafA, stringLeafFrom(leafB, "10", "12", "14", "16", "18"))
		ensureStringLeaf(t, leafB, stringLeafFrom(leafC, "20", "22", "24", "26", "28"))
		ensureStringLeaf(t, leafC, stringLeafFrom(leafD, "30", "34", "36", "40"))
		ensureStringLeaf(t, leafD, stringLeafFrom(leafE, "42", "44", "46", "48"))
		ensureStringLeaf(t, leafE, stringLeafFrom(nil, "50", "52", "54", "56", "58"))
	})
}

func TestStringDelete(t *testing.T) {
	const order = 32

	d, err := NewStringTree(order)
	if err != nil {
		t.Fatal(err)
	}

	for _, v := range randomizedValues {
		d.Insert(strconv.Itoa(v), strconv.Itoa(v))
	}

	for _, v := range randomizedValues {
		if _, ok := d.Search(strconv.Itoa(v)); !ok {
			t.Fatalf("GOT: %v; WANT: %v", ok, true)
		}
	}

	for _, v := range randomizedValues {
		d.Delete(strconv.Itoa(v))
	}

	t.Run("empty", func(t *testing.T) {
		d.Delete(strconv.Itoa(13))
	})
}
