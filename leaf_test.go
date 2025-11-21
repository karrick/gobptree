package gobptree

import "testing"

func TestGenericLeafNodeAbsorbFromRight(t *testing.T) {
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
}

func TestGenericLeafNodeAdoptFromLeft(t *testing.T) {
	// NOTE: For these tests there is no specific order of the nodes, because
	// the adopt methods ignore the order when pulling in a single element
	// from the sibling into the node.

	t.Run("when left empty node", func(t *testing.T) {
		t.Run("when right empty", func(t *testing.T) {
			right := &leafNode[int, int]{Runts: []int{}, Values: []int{}}
			left := &leafNode[int, int]{Runts: []int{}, Values: []int{}, Next: right}

			right.adoptFromLeft(left)

			t.Run("left", func(t *testing.T) {
				ensureLeafNodesMatch(t, left, &leafNode[int, int]{
					Runts:  []int{},
					Values: []int{},
					Next:   right,
				})
			})
			t.Run("right", func(t *testing.T) {
				ensureLeafNodesMatch(t, right, &leafNode[int, int]{
					Runts:  []int{},
					Values: []int{},
				})
			})
		})
		t.Run("when right single node", func(t *testing.T) {
			right := &leafNode[int, int]{Runts: []int{11}, Values: []int{11}}
			left := &leafNode[int, int]{Runts: []int{}, Values: []int{}, Next: right}

			right.adoptFromLeft(left)

			t.Run("left", func(t *testing.T) {
				ensureLeafNodesMatch(t, left, &leafNode[int, int]{
					Runts:  []int{},
					Values: []int{},
					Next:   right,
				})
			})
			t.Run("right", func(t *testing.T) {
				ensureLeafNodesMatch(t, right, &leafNode[int, int]{
					Runts:  []int{11},
					Values: []int{11},
				})
			})
		})
		t.Run("when right multiple nodes", func(t *testing.T) {
			right := &leafNode[int, int]{Runts: []int{11, 22}, Values: []int{11, 22}}
			left := &leafNode[int, int]{Runts: []int{}, Values: []int{}, Next: right}

			right.adoptFromLeft(left)

			t.Run("left", func(t *testing.T) {
				ensureLeafNodesMatch(t, left, &leafNode[int, int]{
					Runts:  []int{},
					Values: []int{},
					Next:   right,
				})
			})
			t.Run("right", func(t *testing.T) {
				ensureLeafNodesMatch(t, right, &leafNode[int, int]{
					Runts:  []int{11, 22},
					Values: []int{11, 22},
				})
			})
		})
	})

	t.Run("when left single node", func(t *testing.T) {
		t.Run("when right empty", func(t *testing.T) {
			right := &leafNode[int, int]{}
			left := &leafNode[int, int]{Runts: []int{11}, Values: []int{11}, Next: right}

			right.adoptFromLeft(left)

			t.Run("left", func(t *testing.T) {
				ensureLeafNodesMatch(t, left, &leafNode[int, int]{
					Runts:  []int{},
					Values: []int{},
					Next:   right,
				})
			})
			t.Run("right", func(t *testing.T) {
				ensureLeafNodesMatch(t, right, &leafNode[int, int]{
					Runts:  []int{11},
					Values: []int{11},
				})
			})
		})
		t.Run("when right single node", func(t *testing.T) {
			right := &leafNode[int, int]{Runts: []int{22}, Values: []int{22}}
			left := &leafNode[int, int]{Runts: []int{11}, Values: []int{11}, Next: right}

			right.adoptFromLeft(left)

			t.Run("left", func(t *testing.T) {
				ensureLeafNodesMatch(t, left, &leafNode[int, int]{
					Runts:  []int{},
					Values: []int{},
					Next:   right,
				})
			})
			t.Run("right", func(t *testing.T) {
				ensureLeafNodesMatch(t, right, &leafNode[int, int]{
					Runts:  []int{11, 22},
					Values: []int{11, 22},
				})
			})
		})
		t.Run("when right multiple nodes", func(t *testing.T) {
			right := &leafNode[int, int]{Runts: []int{22, 33}, Values: []int{22, 33}}
			left := &leafNode[int, int]{Runts: []int{11}, Values: []int{11}, Next: right}

			right.adoptFromLeft(left)

			t.Run("left", func(t *testing.T) {
				ensureLeafNodesMatch(t, left, &leafNode[int, int]{
					Runts:  []int{},
					Values: []int{},
					Next:   right,
				})
			})
			t.Run("right", func(t *testing.T) {
				ensureLeafNodesMatch(t, right, &leafNode[int, int]{
					Runts:  []int{11, 22, 33},
					Values: []int{11, 22, 33},
				})
			})
		})
	})

	t.Run("when left multiple nodes", func(t *testing.T) {
		t.Run("when right empty", func(t *testing.T) {
			right := &leafNode[int, int]{}
			left := &leafNode[int, int]{Runts: []int{11, 22}, Values: []int{11, 22}, Next: right}

			right.adoptFromLeft(left)

			t.Run("left", func(t *testing.T) {
				ensureLeafNodesMatch(t, left, &leafNode[int, int]{
					Runts:  []int{11},
					Values: []int{11},
					Next:   right,
				})
			})
			t.Run("right", func(t *testing.T) {
				ensureLeafNodesMatch(t, right, &leafNode[int, int]{
					Runts:  []int{22},
					Values: []int{22},
				})
			})
		})
		t.Run("when right single node", func(t *testing.T) {
			right := &leafNode[int, int]{Runts: []int{33}, Values: []int{33}}
			left := &leafNode[int, int]{Runts: []int{11, 22}, Values: []int{11, 22}, Next: right}

			right.adoptFromLeft(left)

			t.Run("left", func(t *testing.T) {
				ensureLeafNodesMatch(t, left, &leafNode[int, int]{
					Runts:  []int{11},
					Values: []int{11},
					Next:   right,
				})
			})
			t.Run("right", func(t *testing.T) {
				ensureLeafNodesMatch(t, right, &leafNode[int, int]{
					Runts:  []int{22, 33},
					Values: []int{22, 33},
				})
			})
		})
		t.Run("when right multiple nodes", func(t *testing.T) {
			right := &leafNode[int, int]{Runts: []int{33, 44}, Values: []int{33, 44}}
			left := &leafNode[int, int]{Runts: []int{11, 22}, Values: []int{11, 22}, Next: right}

			right.adoptFromLeft(left)

			t.Run("left", func(t *testing.T) {
				ensureLeafNodesMatch(t, left, &leafNode[int, int]{
					Runts:  []int{11},
					Values: []int{11},
					Next:   right,
				})
			})
			t.Run("right", func(t *testing.T) {
				ensureLeafNodesMatch(t, right, &leafNode[int, int]{
					Runts:  []int{22, 33, 44},
					Values: []int{22, 33, 44},
				})
			})
		})
	})
}

func TestGenericLeafNodeAdoptFromRight(t *testing.T) {
	// NOTE: For these tests there is no specific order of the nodes, because
	// the adopt methods ignore the order when pulling in a single element
	// from the sibling into the node.

	t.Run("when right single node", func(t *testing.T) {
		t.Run("when left empty", func(t *testing.T) {
			right := &leafNode[int, int]{Runts: []int{11}, Values: []int{11}}
			left := &leafNode[int, int]{Next: right}

			left.adoptFromRight(right)

			t.Run("left", func(t *testing.T) {
				ensureLeafNodesMatch(t, left, &leafNode[int, int]{
					Runts:  []int{11},
					Values: []int{11},
					Next:   right,
				})
			})
			t.Run("right", func(t *testing.T) {
				ensureLeafNodesMatch(t, right, &leafNode[int, int]{
					Runts:  []int{},
					Values: []int{},
				})
			})
		})
		t.Run("when left single node", func(t *testing.T) {
			right := &leafNode[int, int]{Runts: []int{22}, Values: []int{22}}
			left := &leafNode[int, int]{Runts: []int{11}, Values: []int{11}, Next: right}

			left.adoptFromRight(right)

			t.Run("left", func(t *testing.T) {
				ensureLeafNodesMatch(t, left, &leafNode[int, int]{
					Runts:  []int{11, 22},
					Values: []int{11, 22},
					Next:   right,
				})
			})
			t.Run("right", func(t *testing.T) {
				ensureLeafNodesMatch(t, right, &leafNode[int, int]{
					Runts:  []int{},
					Values: []int{},
				})
			})
		})
		t.Run("when left multiple nodes", func(t *testing.T) {
			right := &leafNode[int, int]{Runts: []int{33}, Values: []int{33}}
			left := &leafNode[int, int]{Runts: []int{11, 22}, Values: []int{11, 22}, Next: right}

			left.adoptFromRight(right)

			t.Run("left", func(t *testing.T) {
				ensureLeafNodesMatch(t, left, &leafNode[int, int]{
					Runts:  []int{11, 22, 33},
					Values: []int{11, 22, 33},
					Next:   right,
				})
			})
			t.Run("right", func(t *testing.T) {
				ensureLeafNodesMatch(t, right, &leafNode[int, int]{
					Runts:  []int{},
					Values: []int{},
				})
			})
		})
	})

	t.Run("when right multiple nodes", func(t *testing.T) {
		t.Run("when left empty", func(t *testing.T) {
			right := &leafNode[int, int]{Runts: []int{11, 22}, Values: []int{11, 22}}
			left := &leafNode[int, int]{Next: right}

			left.adoptFromRight(right)

			t.Run("left", func(t *testing.T) {
				ensureLeafNodesMatch(t, left, &leafNode[int, int]{
					Runts:  []int{11},
					Values: []int{11},
					Next:   right,
				})
			})
			t.Run("right", func(t *testing.T) {
				ensureLeafNodesMatch(t, right, &leafNode[int, int]{
					Runts:  []int{22},
					Values: []int{22},
				})
			})
		})
		t.Run("when left single node", func(t *testing.T) {
			right := &leafNode[int, int]{Runts: []int{22, 33}, Values: []int{22, 33}}
			left := &leafNode[int, int]{Runts: []int{11}, Values: []int{11}, Next: right}

			left.adoptFromRight(right)

			t.Run("left", func(t *testing.T) {
				ensureLeafNodesMatch(t, left, &leafNode[int, int]{
					Runts:  []int{11, 22},
					Values: []int{11, 22},
					Next:   right,
				})
			})
			t.Run("right", func(t *testing.T) {
				ensureLeafNodesMatch(t, right, &leafNode[int, int]{
					Runts:  []int{33},
					Values: []int{33},
				})
			})
		})
		t.Run("when left multiple nodes", func(t *testing.T) {
			right := &leafNode[int, int]{Runts: []int{33, 44}, Values: []int{33, 44}}
			left := &leafNode[int, int]{Runts: []int{11, 22}, Values: []int{11, 22}, Next: right}

			left.adoptFromRight(right)

			t.Run("left", func(t *testing.T) {
				ensureLeafNodesMatch(t, left, &leafNode[int, int]{
					Runts:  []int{11, 22, 33},
					Values: []int{11, 22, 33},
					Next:   right,
				})
			})
			t.Run("right", func(t *testing.T) {
				ensureLeafNodesMatch(t, right, &leafNode[int, int]{
					Runts:  []int{44},
					Values: []int{44},
				})
			})
		})
	})
}

func TestGenericLeafNodeDeleteKey(t *testing.T) {
	t.Run("before first key", func(t *testing.T) {
		leafA := &leafNode[int, int]{
			Runts:  []int{11, 33, 55},
			Values: []int{11, 33, 55},
		}

		size, smallest := leafA.deleteKey(insertionIndexUsingCompare[int], 2, 0)

		if got, want := size, 3; got != want {
			t.Errorf("size: GOT: %v; WANT: %v", got, want)
		}
		if got, want := smallest, 11; got != want {
			t.Errorf("smallest: GOT: %v; WANT: %v", got, want)
		}
		t.Run("leafA", func(t *testing.T) {
			ensureLeafNodesMatch(t, leafA, &leafNode[int, int]{
				Runts:  []int{11, 33, 55},
				Values: []int{11, 33, 55},
			})
		})
	})

	t.Run("first key", func(t *testing.T) {
		leafA := &leafNode[int, int]{
			Runts:  []int{11, 33, 55},
			Values: []int{11, 33, 55},
		}

		size, smallest := leafA.deleteKey(insertionIndexUsingCompare[int], 3, 11)

		if got, want := size, 2; got != want {
			t.Errorf("size: GOT: %v; WANT: %v", got, want)
		}
		if got, want := smallest, 33; got != want {
			t.Errorf("smallest: GOT: %v; WANT: %v", got, want)
		}
		t.Run("leafA", func(t *testing.T) {
			ensureLeafNodesMatch(t, leafA, &leafNode[int, int]{
				Runts:  []int{33, 55},
				Values: []int{33, 55},
			})
		})
	})

	t.Run("between first and second key", func(t *testing.T) {
		leafA := &leafNode[int, int]{
			Runts:  []int{11, 33, 55},
			Values: []int{11, 33, 55},
		}

		size, smallest := leafA.deleteKey(insertionIndexUsingCompare[int], 2, 22)

		if got, want := size, 3; got != want {
			t.Errorf("size: GOT: %v; WANT: %v", got, want)
		}
		if got, want := smallest, 11; got != want {
			t.Errorf("smallest: GOT: %v; WANT: %v", got, want)
		}
		t.Run("leafA", func(t *testing.T) {
			ensureLeafNodesMatch(t, leafA, &leafNode[int, int]{
				Runts:  []int{11, 33, 55},
				Values: []int{11, 33, 55},
			})
		})
	})

	t.Run("second key", func(t *testing.T) {
		leafA := &leafNode[int, int]{
			Runts:  []int{11, 33, 55},
			Values: []int{11, 33, 55},
		}

		size, smallest := leafA.deleteKey(insertionIndexUsingCompare[int], 3, 33)

		if got, want := size, 2; got != want {
			t.Errorf("size: GOT: %v; WANT: %v", got, want)
		}
		if got, want := smallest, 11; got != want {
			t.Errorf("smallest: GOT: %v; WANT: %v", got, want)
		}
		t.Run("leafA", func(t *testing.T) {
			ensureLeafNodesMatch(t, leafA, &leafNode[int, int]{
				Runts:  []int{11, 55},
				Values: []int{11, 55},
			})
		})
	})

	t.Run("between second and third key", func(t *testing.T) {
		leafA := &leafNode[int, int]{
			Runts:  []int{11, 33, 55},
			Values: []int{11, 33, 55},
		}

		size, smallest := leafA.deleteKey(insertionIndexUsingCompare[int], 3, 44)

		if got, want := size, 3; got != want {
			t.Errorf("size: GOT: %v; WANT: %v", got, want)
		}
		if got, want := smallest, 11; got != want {
			t.Errorf("smallest: GOT: %v; WANT: %v", got, want)
		}
		t.Run("leafA", func(t *testing.T) {
			ensureLeafNodesMatch(t, leafA, &leafNode[int, int]{
				Runts:  []int{11, 33, 55},
				Values: []int{11, 33, 55},
			})
		})
	})

	t.Run("third key", func(t *testing.T) {
		leafA := &leafNode[int, int]{
			Runts:  []int{11, 33, 55},
			Values: []int{11, 33, 55},
		}
		size, smallest := leafA.deleteKey(insertionIndexUsingCompare[int], 3, 55)

		if got, want := size, 2; got != want {
			t.Errorf("size: GOT: %v; WANT: %v", got, want)
		}
		if got, want := smallest, 11; got != want {
			t.Errorf("smallest: GOT: %v; WANT: %v", got, want)
		}
		t.Run("leafA", func(t *testing.T) {
			ensureLeafNodesMatch(t, leafA, &leafNode[int, int]{
				Runts:  []int{11, 33},
				Values: []int{11, 33},
			})
		})
	})

	t.Run("after third key", func(t *testing.T) {
		leafA := &leafNode[int, int]{
			Runts:  []int{11, 33, 55},
			Values: []int{11, 33, 55},
		}
		size, smallest := leafA.deleteKey(insertionIndexUsingCompare[int], 3, 66)

		if got, want := size, 3; got != want {
			t.Errorf("size: GOT: %v; WANT: %v", got, want)
		}
		if got, want := smallest, 11; got != want {
			t.Errorf("smallest: GOT: %v; WANT: %v", got, want)
		}
		t.Run("leafA", func(t *testing.T) {
			ensureLeafNodesMatch(t, leafA, &leafNode[int, int]{
				Runts:  []int{11, 33, 55},
				Values: []int{11, 33, 55},
			})
		})
	})
}

func TestGenericLeafNodeSplit(t *testing.T) {
	leafB := &leafNode[int, int]{
		Runts:  []int{21, 23, 25, 27},
		Values: []int{21, 23, 25, 27},
	}
	leafA := &leafNode[int, int]{
		Runts:  []int{11, 13, 15, 17},
		Values: []int{11, 13, 15, 17},
		Next:   leafB,
	}

	newSibling := leafA.split(4)

	t.Run("leafA", func(t *testing.T) {
		ensureNodesMatch(t, leafA, &leafNode[int, int]{
			Runts:  []int{11, 13},
			Values: []int{11, 13},
			Next:   newSibling,
		})
	})
	t.Run("newSibling", func(t *testing.T) {
		ensureNodesMatch(t, newSibling, &leafNode[int, int]{
			Runts:  []int{15, 17},
			Values: []int{15, 17},
			Next:   leafB,
		})
	})
	t.Run("leafB", func(t *testing.T) {
		ensureNodesMatch(t, leafB, &leafNode[int, int]{
			Runts:  []int{21, 23, 25, 27},
			Values: []int{21, 23, 25, 27},
		})
	})
}

func TestGenericLeafNodeUpdateKey(t *testing.T) {
	const order = 4

	// TODO: add tests to handle when callback returns error.

	insertionIndex := insertionIndexSelect[int]()

	t.Run("no split", func(t *testing.T) {
		t.Run("key absent", func(t *testing.T) {
			const knownPresentFalse = false

			t.Run("before first runt", func(t *testing.T) {
				leafA := &leafNode[int, int]{
					Runts:  []int{11, 13, 15},
					Values: []int{11, 13, 15},
				}

				newSibling, err := leafA.updateKey(insertionIndex, 10, order, knownPresentFalse, callbackExpects(t, 0, false))
				ensureError(t, err)

				t.Run("leafA", func(t *testing.T) {
					ensureNodesMatch(t, leafA, &leafNode[int, int]{
						Runts:  []int{10, 11, 13, 15},
						Values: []int{-1, 11, 13, 15},
					})
				})
				t.Run("newSibling", func(t *testing.T) {
					ensureSame(t, newSibling, nil)
				})
			})

			t.Run("between first and second runt", func(t *testing.T) {
				leafA := &leafNode[int, int]{
					Runts:  []int{11, 13, 15},
					Values: []int{11, 13, 15},
				}

				newSibling, err := leafA.updateKey(insertionIndex, 12, order, knownPresentFalse, callbackExpects(t, 0, false))
				ensureError(t, err)

				t.Run("leafA", func(t *testing.T) {
					ensureNodesMatch(t, leafA, &leafNode[int, int]{
						Runts:  []int{11, 12, 13, 15},
						Values: []int{11, -1, 13, 15},
					})
				})
				t.Run("newSibling", func(t *testing.T) {
					ensureSame(t, newSibling, nil)
				})
			})

			t.Run("between second and third runt", func(t *testing.T) {
				leafA := &leafNode[int, int]{
					Runts:  []int{11, 13, 15},
					Values: []int{11, 13, 15},
				}

				newSibling, err := leafA.updateKey(insertionIndex, 14, order, knownPresentFalse, callbackExpects(t, 0, false))
				ensureError(t, err)

				t.Run("leafA", func(t *testing.T) {
					ensureNodesMatch(t, leafA, &leafNode[int, int]{
						Runts:  []int{11, 13, 14, 15},
						Values: []int{11, 13, -1, 15},
					})
				})
				t.Run("newSibling", func(t *testing.T) {
					ensureSame(t, newSibling, nil)
				})
			})

			t.Run("after third runt", func(t *testing.T) {
				leafA := &leafNode[int, int]{
					Runts:  []int{11, 13, 15},
					Values: []int{11, 13, 15},
				}

				newSibling, err := leafA.updateKey(insertionIndex, 16, order, knownPresentFalse, callbackExpects(t, 0, false))
				ensureError(t, err)

				t.Run("leafA", func(t *testing.T) {
					ensureNodesMatch(t, leafA, &leafNode[int, int]{
						Runts:  []int{11, 13, 15, 16},
						Values: []int{11, 13, 15, -1},
					})
				})
				t.Run("newSibling", func(t *testing.T) {
					ensureSame(t, newSibling, nil)
				})
			})
		})

		t.Run("key present", func(t *testing.T) {
			t.Run("known present true", func(t *testing.T) {
				const knownPresentTrue = true

				leafA := &leafNode[int, int]{
					Runts:  []int{11, 13, 15, 17},
					Values: []int{11, 13, 15, 17},
				}

				newSibling, err := leafA.updateKey(insertionIndex, 11, order, knownPresentTrue, callbackExpects(t, 11, true))
				ensureError(t, err)

				t.Run("leafA", func(t *testing.T) {
					ensureNodesMatch(t, leafA, &leafNode[int, int]{
						Runts:  []int{11, 13, 15, 17},
						Values: []int{-1, 13, 15, 17},
					})
				})
				t.Run("newSibling", func(t *testing.T) {
					ensureSame(t, newSibling, nil)
				})
			})

			t.Run("known present false/mid item", func(t *testing.T) {
				const knownPresentFalse = false

				leafA := &leafNode[int, int]{
					Runts:  []int{11, 13, 15, 17},
					Values: []int{11, 13, 15, 17},
				}

				newSibling, err := leafA.updateKey(insertionIndex, 15, order, knownPresentFalse, callbackExpects(t, 15, true))
				ensureError(t, err)

				t.Run("leafA", func(t *testing.T) {
					ensureNodesMatch(t, leafA, &leafNode[int, int]{
						Runts:  []int{11, 13, 15, 17},
						Values: []int{11, 13, -1, 17},
					})
				})
				t.Run("newSibling", func(t *testing.T) {
					ensureSame(t, newSibling, nil)
				})
			})

			t.Run("known present false/final item", func(t *testing.T) {
				const knownPresentFalse = false

				leafA := &leafNode[int, int]{
					Runts:  []int{11, 13, 15, 17},
					Values: []int{11, 13, 15, 17},
				}

				newSibling, err := leafA.updateKey(insertionIndex, 17, order, knownPresentFalse, callbackExpects(t, 17, true))
				ensureError(t, err)

				t.Run("leafA", func(t *testing.T) {
					ensureNodesMatch(t, leafA, &leafNode[int, int]{
						Runts:  []int{11, 13, 15, 17},
						Values: []int{11, 13, 15, -1},
					})
				})
				t.Run("newSibling", func(t *testing.T) {
					ensureSame(t, newSibling, nil)
				})
			})
		})
	})

	t.Run("node split", func(t *testing.T) {
		const knownPresentFalse = false

		t.Run("key inserted before first value of left side", func(t *testing.T) {
			leafB := &leafNode[int, int]{
				Runts:  []int{21, 23, 25, 27},
				Values: []int{21, 23, 25, 27},
			}
			leafA := &leafNode[int, int]{
				Runts:  []int{11, 13, 15, 17},
				Values: []int{11, 13, 15, 17},
				Next:   leafB,
			}

			newSibling, err := leafA.updateKey(insertionIndex, 10, order, knownPresentFalse, callbackExpects(t, 0, false))
			ensureError(t, err)

			t.Run("leafA", func(t *testing.T) {
				ensureNodesMatch(t, leafA, &leafNode[int, int]{
					Runts:  []int{10, 11, 13},
					Values: []int{-1, 11, 13},
					Next:   newSibling.(*leafNode[int, int]),
				})
			})
			t.Run("newSibling", func(t *testing.T) {
				ensureNodesMatch(t, newSibling, &leafNode[int, int]{
					Runts:  []int{15, 17},
					Values: []int{15, 17},
					Next:   leafB,
				})
			})
			t.Run("leafB", func(t *testing.T) {
				ensureNodesMatch(t, leafB, &leafNode[int, int]{
					Runts:  []int{21, 23, 25, 27},
					Values: []int{21, 23, 25, 27},
				})
			})
		})

		t.Run("key inserted between values on left side", func(t *testing.T) {
			leafA := &leafNode[int, int]{
				Runts:  []int{11, 13, 15, 17},
				Values: []int{11, 13, 15, 17},
			}

			newSibling, err := leafA.updateKey(insertionIndex, 12, order, knownPresentFalse, callbackExpects(t, 0, false))
			ensureError(t, err)

			t.Run("leafA", func(t *testing.T) {
				ensureNodesMatch(t, leafA, &leafNode[int, int]{
					Runts:  []int{11, 12, 13},
					Values: []int{11, -1, 13},
					Next:   newSibling.(*leafNode[int, int]),
				})
			})
			t.Run("newSibling", func(t *testing.T) {
				ensureNodesMatch(t, newSibling, &leafNode[int, int]{
					Runts:  []int{15, 17},
					Values: []int{15, 17},
				})
			})
		})

		t.Run("key inserted after values on left side", func(t *testing.T) {
			leafA := &leafNode[int, int]{
				Runts:  []int{11, 13, 15, 17},
				Values: []int{11, 13, 15, 17},
			}

			newSibling, err := leafA.updateKey(insertionIndex, 14, order, knownPresentFalse, callbackExpects(t, 0, false))
			ensureError(t, err)

			t.Run("leafA", func(t *testing.T) {
				ensureNodesMatch(t, leafA, &leafNode[int, int]{
					Runts:  []int{11, 13, 14},
					Values: []int{11, 13, -1},
					Next:   newSibling.(*leafNode[int, int]),
				})
			})
			t.Run("newSibling", func(t *testing.T) {
				ensureNodesMatch(t, newSibling, &leafNode[int, int]{
					Runts:  []int{15, 17},
					Values: []int{15, 17},
				})
			})
		})

		t.Run("key inserted between values on right side", func(t *testing.T) {
			leafA := &leafNode[int, int]{
				Runts:  []int{11, 13, 15, 17},
				Values: []int{11, 13, 15, 17},
			}

			newSibling, err := leafA.updateKey(insertionIndex, 16, order, knownPresentFalse, callbackExpects(t, 0, false))
			ensureError(t, err)

			t.Run("leafA", func(t *testing.T) {
				ensureNodesMatch(t, leafA, &leafNode[int, int]{
					Runts:  []int{11, 13},
					Values: []int{11, 13},
					Next:   newSibling.(*leafNode[int, int]),
				})
			})
			t.Run("newSibling", func(t *testing.T) {
				ensureNodesMatch(t, newSibling, &leafNode[int, int]{
					Runts:  []int{15, 16, 17},
					Values: []int{15, -1, 17},
				})
			})
		})

		t.Run("key inserted after values on right side", func(t *testing.T) {
			leafA := &leafNode[int, int]{
				Runts:  []int{11, 13, 15, 17},
				Values: []int{11, 13, 15, 17},
			}

			newSibling, err := leafA.updateKey(insertionIndex, 18, order, knownPresentFalse, callbackExpects(t, 0, false))
			ensureError(t, err)

			t.Run("leafA", func(t *testing.T) {
				ensureNodesMatch(t, leafA, &leafNode[int, int]{
					Runts:  []int{11, 13},
					Values: []int{11, 13},
					Next:   newSibling.(*leafNode[int, int]),
				})
			})
			t.Run("newSibling", func(t *testing.T) {
				ensureNodesMatch(t, newSibling, &leafNode[int, int]{
					Runts:  []int{15, 17, 18},
					Values: []int{15, 17, -1},
				})
			})
		})
	})
}
