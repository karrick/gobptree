package gobptree

import "testing"

func TestGenericInternalNodeAbsorbFromRight(t *testing.T) {
	// NOTE: Must create nodes in descending order in order to be able to
	// set the next field of each node.
	leafD := &leafNode[int, int]{
		Runts:  []int{41, 43, 45},
		Values: []int{41, 43, 45},
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

	t.Run("when right empty node", func(t *testing.T) {
		t.Run("when left empty", func(t *testing.T) {
			left := &internalNode[int, int]{}
			right := &internalNode[int, int]{}

			left.absorbFromRight(right)

			t.Run("left", func(t *testing.T) {
				ensureInternalNodesMatch(t, left, &internalNode[int, int]{})
			})
			t.Run("right", func(t *testing.T) {
				ensureInternalNodesMatch(t, right, &internalNode[int, int]{})
			})
		})
		t.Run("when left single node", func(t *testing.T) {
			left := newInternal(leafA)
			right := &internalNode[int, int]{}

			left.absorbFromRight(right)

			t.Run("left", func(t *testing.T) {
				ensureInternalNodesMatch(t, left, newInternal(leafA))
			})
			t.Run("right", func(t *testing.T) {
				ensureInternalNodesMatch(t, right, &internalNode[int, int]{})
			})
		})
		t.Run("when left multiple nodes", func(t *testing.T) {
			left := newInternal(leafA, leafB)
			right := &internalNode[int, int]{}

			left.absorbFromRight(right)

			t.Run("left", func(t *testing.T) {
				ensureInternalNodesMatch(t, left, newInternal(leafA, leafB))
			})
			t.Run("right", func(t *testing.T) {
				ensureInternalNodesMatch(t, right, &internalNode[int, int]{})
			})
		})
	})

	t.Run("when right single node", func(t *testing.T) {
		t.Run("when left empty", func(t *testing.T) {
			left := &internalNode[int, int]{}
			right := newInternal(leafA)

			left.absorbFromRight(right)

			t.Run("left", func(t *testing.T) {
				ensureInternalNodesMatch(t, left, newInternal(leafA))
			})
			t.Run("right", func(t *testing.T) {
				ensureInternalNodesMatch(t, right, &internalNode[int, int]{})
			})
		})
		t.Run("when left single node", func(t *testing.T) {
			left := newInternal(leafA)
			right := newInternal(leafB)

			left.absorbFromRight(right)

			t.Run("left", func(t *testing.T) {
				ensureInternalNodesMatch(t, left, newInternal(leafA, leafB))
			})
			t.Run("right", func(t *testing.T) {
				ensureInternalNodesMatch(t, right, &internalNode[int, int]{})
			})
		})
		t.Run("when left multiple nodes", func(t *testing.T) {
			left := newInternal(leafA, leafB)
			right := newInternal(leafC)

			left.absorbFromRight(right)

			t.Run("left", func(t *testing.T) {
				ensureInternalNodesMatch(t, left, newInternal(leafA, leafB, leafC))
			})
			t.Run("right", func(t *testing.T) {
				ensureInternalNodesMatch(t, right, &internalNode[int, int]{})
			})
		})
	})

	t.Run("when right multiple nodes", func(t *testing.T) {
		t.Run("when left empty", func(t *testing.T) {
			left := &internalNode[int, int]{}
			right := newInternal(leafA, leafB)

			left.absorbFromRight(right)

			t.Run("left", func(t *testing.T) {
				ensureInternalNodesMatch(t, left, newInternal(leafA, leafB))
			})
			t.Run("right", func(t *testing.T) {
				ensureInternalNodesMatch(t, right, &internalNode[int, int]{})
			})
		})
		t.Run("when left single node", func(t *testing.T) {
			left := newInternal(leafA)
			right := newInternal(leafB, leafC)

			left.absorbFromRight(right)

			t.Run("left", func(t *testing.T) {
				ensureInternalNodesMatch(t, left, newInternal(leafA, leafB, leafC))
			})
			t.Run("right", func(t *testing.T) {
				ensureInternalNodesMatch(t, right, &internalNode[int, int]{})
			})
		})
		t.Run("when left multiple nodes", func(t *testing.T) {
			left := newInternal(leafA, leafB)
			right := newInternal(leafC, leafD)

			left.absorbFromRight(right)

			t.Run("left", func(t *testing.T) {
				ensureInternalNodesMatch(t, left, newInternal(leafA, leafB, leafC, leafD))
			})
			t.Run("right", func(t *testing.T) {
				ensureInternalNodesMatch(t, right, &internalNode[int, int]{})
			})
		})
	})
}

func TestGenericInternalNodeAdoptFromLeft(t *testing.T) {
	// NOTE: Must create nodes in descending order in order to be able to
	// set the next field of each node.
	leafD := &leafNode[int, int]{
		Runts:  []int{41, 43, 45},
		Values: []int{41, 43, 45},
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

	// NOTE: Cannot adopt from empty node, therefore no test cases for
	// adopting from left node when it is empty.

	t.Run("when left empty node", func(t *testing.T) {
		t.Run("when right empty", func(t *testing.T) {
			left := &internalNode[int, int]{Runts: []int{}}
			right := &internalNode[int, int]{Runts: []int{}}

			right.adoptFromLeft(left)

			t.Run("left", func(t *testing.T) {
				ensureInternalNodesMatch(t, left, &internalNode[int, int]{Runts: []int{}})
			})
			t.Run("right", func(t *testing.T) {
				ensureInternalNodesMatch(t, right, &internalNode[int, int]{Runts: []int{}})
			})
		})
		t.Run("when right single node", func(t *testing.T) {
			left := &internalNode[int, int]{Runts: []int{}}
			right := newInternal(leafA)

			right.adoptFromLeft(left)

			t.Run("left", func(t *testing.T) {
				ensureInternalNodesMatch(t, left, &internalNode[int, int]{Runts: []int{}})
			})
			t.Run("right", func(t *testing.T) {
				ensureInternalNodesMatch(t, right, newInternal(leafA))
			})
		})
		t.Run("when right multiple nodes", func(t *testing.T) {
			left := &internalNode[int, int]{Runts: []int{}}
			right := newInternal(leafA, leafB)

			right.adoptFromLeft(left)

			t.Run("left", func(t *testing.T) {
				ensureInternalNodesMatch(t, left, &internalNode[int, int]{Runts: []int{}})
			})
			t.Run("right", func(t *testing.T) {
				ensureInternalNodesMatch(t, right, newInternal(leafA, leafB))
			})
		})
	})

	t.Run("when left single node", func(t *testing.T) {
		t.Run("when right empty", func(t *testing.T) {
			left := newInternal(leafA)
			right := &internalNode[int, int]{}

			right.adoptFromLeft(left)

			t.Run("left", func(t *testing.T) {
				ensureInternalNodesMatch(t, left, &internalNode[int, int]{Runts: []int{}})
			})
			t.Run("right", func(t *testing.T) {
				ensureInternalNodesMatch(t, right, newInternal(leafA))
			})
		})
		t.Run("when right single node", func(t *testing.T) {
			left := newInternal(leafA)
			right := newInternal(leafB)

			right.adoptFromLeft(left)

			t.Run("left", func(t *testing.T) {
				ensureInternalNodesMatch(t, left, &internalNode[int, int]{Runts: []int{}})
			})
			t.Run("right", func(t *testing.T) {
				ensureInternalNodesMatch(t, right, newInternal(leafA, leafB))
			})
		})
		t.Run("when right multiple nodes", func(t *testing.T) {
			left := newInternal(leafA)
			right := newInternal(leafB, leafC)

			right.adoptFromLeft(left)

			t.Run("left", func(t *testing.T) {
				ensureInternalNodesMatch(t, left, &internalNode[int, int]{Runts: []int{}})
			})
			t.Run("right", func(t *testing.T) {
				ensureInternalNodesMatch(t, right, newInternal(leafA, leafB, leafC))
			})
		})
	})

	t.Run("when left multiple nodes", func(t *testing.T) {
		t.Run("when right empty", func(t *testing.T) {
			left := newInternal(leafA, leafB)
			right := &internalNode[int, int]{}

			right.adoptFromLeft(left)

			t.Run("left", func(t *testing.T) {
				ensureInternalNodesMatch(t, left, newInternal(leafA))
			})
			t.Run("right", func(t *testing.T) {
				ensureInternalNodesMatch(t, right, newInternal(leafB))
			})
		})
		t.Run("when right single node", func(t *testing.T) {
			left := newInternal(leafA, leafB)
			right := newInternal(leafC)

			right.adoptFromLeft(left)

			t.Run("left", func(t *testing.T) {
				ensureInternalNodesMatch(t, left, newInternal(leafA))
			})
			t.Run("right", func(t *testing.T) {
				ensureInternalNodesMatch(t, right, newInternal(leafB, leafC))
			})
		})
		t.Run("when right multiple nodes", func(t *testing.T) {
			left := newInternal(leafA, leafB)
			right := newInternal(leafC, leafD)

			right.adoptFromLeft(left)

			t.Run("left", func(t *testing.T) {
				ensureInternalNodesMatch(t, left, newInternal(leafA))
			})
			t.Run("right", func(t *testing.T) {
				ensureInternalNodesMatch(t, right, newInternal(leafB, leafC, leafD))
			})
		})
	})
}

func TestGenericInternalNodeAdoptFromRight(t *testing.T) {
	// NOTE: Must create nodes in descending order in order to be able
	// to set the next field of each node.
	leafD := &leafNode[int, int]{
		Runts:  []int{41, 43, 45},
		Values: []int{41, 43, 45},
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

	// NOTE: Cannot adopt from empty node, therefore no test cases for
	// adopting from right node when it is empty.

	t.Run("when right single node", func(t *testing.T) {
		t.Run("when left empty", func(t *testing.T) {
			left := &internalNode[int, int]{}
			right := newInternal(leafA)

			left.adoptFromRight(right)

			t.Run("left", func(t *testing.T) {
				ensureInternalNodesMatch(t, left, newInternal(leafA))
			})
			t.Run("right", func(t *testing.T) {
				ensureInternalNodesMatch(t, right, &internalNode[int, int]{Runts: []int{}})
			})
		})
		t.Run("when left single node", func(t *testing.T) {
			left := newInternal(leafA)
			right := newInternal(leafB)

			left.adoptFromRight(right)

			t.Run("left", func(t *testing.T) {
				ensureInternalNodesMatch(t, left, newInternal(leafA, leafB))
			})
			t.Run("right", func(t *testing.T) {
				ensureInternalNodesMatch(t, right, &internalNode[int, int]{Runts: []int{}})
			})
		})
		t.Run("when left multiple nodes", func(t *testing.T) {
			left := newInternal(leafA, leafB)
			right := newInternal(leafC)

			left.adoptFromRight(right)

			t.Run("left", func(t *testing.T) {
				ensureInternalNodesMatch(t, left, newInternal(leafA, leafB, leafC))
			})
			t.Run("right", func(t *testing.T) {
				ensureInternalNodesMatch(t, right, &internalNode[int, int]{Runts: []int{}})
			})
		})
	})

	t.Run("when right multiple nodes", func(t *testing.T) {
		t.Run("when left empty", func(t *testing.T) {
			left := &internalNode[int, int]{}
			right := newInternal(leafA, leafB)

			left.adoptFromRight(right)

			t.Run("left", func(t *testing.T) {
				ensureInternalNodesMatch(t, left, newInternal(leafA))
			})
			t.Run("right", func(t *testing.T) {
				ensureInternalNodesMatch(t, right, newInternal(leafB))
			})
		})
		t.Run("when left single node", func(t *testing.T) {
			left := newInternal(leafA)
			right := newInternal(leafB, leafC)

			left.adoptFromRight(right)

			t.Run("left", func(t *testing.T) {
				ensureInternalNodesMatch(t, left, newInternal(leafA, leafB))
			})
			t.Run("right", func(t *testing.T) {
				ensureInternalNodesMatch(t, right, newInternal(leafC))
			})
		})
		t.Run("when left multiple nodes", func(t *testing.T) {
			left := newInternal(leafA, leafB)
			right := newInternal(leafC, leafD)

			left.adoptFromRight(right)

			t.Run("left", func(t *testing.T) {
				ensureInternalNodesMatch(t, left, newInternal(leafA, leafB, leafC))
			})
			t.Run("right", func(t *testing.T) {
				ensureInternalNodesMatch(t, right, newInternal(leafD))
			})
		})
	})
}

func TestGenericInternalNodeDeleteKey(t *testing.T) {
	insertionIndex := insertionIndexSelect[int]()

	t.Run("child still has min size", func(t *testing.T) {
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
		internalA := newInternal(leafA, leafB, leafC, leafD, leafE)

		size, smallest := internalA.deleteKey(insertionIndex, 4, 35)

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
		ensureStructure(t, internalA,
			newInternal(
				leafA,
				leafB,
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
		internalA := newInternal(leafA, leafB, leafC, leafD, leafE)

		size, smallest := internalA.deleteKey(insertionIndex, 4, 35)

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
		ensureStructure(t, internalA,
			newInternal(
				leafA,
				leafB,
				&leafNode[int, int]{
					Runts:  []int{31, 33, 37, 41},
					Values: []int{31, 33, 37, 41},
				},
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
		internalA := newInternal(leafA, leafB, leafC, leafD, leafE)

		size, smallest := internalA.deleteKey(insertionIndex, 4, 35)

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
		ensureStructure(t, internalA,
			newInternal(
				leafA,
				&leafNode[int, int]{
					Runts:  []int{21, 23, 25, 27},
					Values: []int{21, 23, 25, 27},
				},
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
		internalA := newInternal(leafA, leafB, leafC, leafD, leafE)

		size, smallest := internalA.deleteKey(insertionIndex, 4, 35)

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
		ensureStructure(t, internalA,
			newInternal(
				leafA,
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
		internalA := newInternal(leafA, leafB, leafC, leafD, leafE)

		size, smallest := internalA.deleteKey(insertionIndex, 4, 15)

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
		ensureStructure(t, internalA,
			newInternal(
				&leafNode[int, int]{
					Runts:  []int{11, 13, 17, 21, 23, 25, 27},
					Values: []int{11, 13, 17, 21, 23, 25, 27},
				},
				// leaf b removed
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
		internalA := newInternal(leafA)

		size, smallest := internalA.deleteKey(insertionIndex, 4, 15)

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
		ensureStructure(t, internalA,
			newInternal(
				&leafNode[int, int]{
					Runts:  []int{11, 13, 17},
					Values: []int{11, 13, 17},
				},
			),
		)
	})
}

func TestGenericInternalNodeSplit(t *testing.T) {
	t.Run("full internal splits", func(t *testing.T) {
		leafD := &leafNode[int, int]{
			Runts:  []int{41, 43, 45, 47},
			Values: []int{41, 43, 45, 47},
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
		internalA := newInternal(leafA, leafB, leafC, leafD)

		newSibling := internalA.split(4)

		t.Run("internalA", func(t *testing.T) {
			ensureStructure(t, internalA, newInternal(leafA, leafB))
		})
		t.Run("newSibling", func(t *testing.T) {
			ensureStructure(t, newSibling, newInternal(leafC, leafD))
		})
	})
}

func TestGenericInternalNodeUpdateKey(t *testing.T) {
	const order = 4

	insertionIndex := insertionIndexSelect[int]()

	t.Run("node no split; child no split", func(t *testing.T) {
		t.Run("key absent", func(t *testing.T) {
			t.Run("key before first key of first node", func(t *testing.T) {
				leafC := &leafNode[int, int]{
					Runts:  []int{31, 33, 35},
					Values: []int{31, 33, 35},
				}
				leafB := &leafNode[int, int]{
					Runts:  []int{21, 23, 25},
					Values: []int{21, 23, 25},
					Next:   leafC,
				}
				leafA := &leafNode[int, int]{
					Runts:  []int{11, 13, 15},
					Values: []int{11, 13, 15},
					Next:   leafB,
				}
				internalA := newInternal(leafA, leafB, leafC)

				newSibling := internalA.updateKey(insertionIndex, 10, order, false, callbackExpects(t, 0, false))

				t.Run("internalA", func(t *testing.T) {
					ensureStructure(t, internalA,
						newInternal(
							&leafNode[int, int]{
								Runts:  []int{10, 11, 13, 15},
								Values: []int{-1, 11, 13, 15},
							},
							leafB,
							leafC,
						))
				})
				t.Run("newSibling", func(t *testing.T) {
					ensureSame(t, newSibling, nil)
				})
			})
			t.Run("key between first and second key of first leaf", func(t *testing.T) {
				leafC := &leafNode[int, int]{
					Runts:  []int{31, 33, 35},
					Values: []int{31, 33, 35},
				}
				leafB := &leafNode[int, int]{
					Runts:  []int{21, 23, 25},
					Values: []int{21, 23, 25},
					Next:   leafC,
				}
				leafA := &leafNode[int, int]{
					Runts:  []int{11, 13, 15},
					Values: []int{11, 13, 15},
					Next:   leafB,
				}
				internalA := newInternal(leafA, leafB, leafC)

				newSibling := internalA.updateKey(insertionIndex, 12, order, false, callbackExpects(t, 0, false))

				t.Run("internalA", func(t *testing.T) {
					ensureStructure(t, internalA,
						newInternal(
							&leafNode[int, int]{
								Runts:  []int{11, 12, 13, 15},
								Values: []int{11, -1, 13, 15},
							},
							leafB,
							leafC,
						))
				})
				t.Run("newSibling", func(t *testing.T) {
					ensureSame(t, newSibling, nil)
				})
			})
			t.Run("key after final key of first leaf", func(t *testing.T) {
				leafC := &leafNode[int, int]{
					Runts:  []int{31, 33, 35},
					Values: []int{31, 33, 35},
				}
				leafB := &leafNode[int, int]{
					Runts:  []int{21, 23, 25},
					Values: []int{21, 23, 25},
					Next:   leafC,
				}
				leafA := &leafNode[int, int]{
					Runts:  []int{11, 13, 15},
					Values: []int{11, 13, 15},
					Next:   leafB,
				}
				internalA := newInternal(leafA, leafB, leafC)

				newSibling := internalA.updateKey(insertionIndex, 17, order, false, callbackExpects(t, 0, false))

				t.Run("internalA", func(t *testing.T) {
					ensureStructure(t, internalA,
						newInternal(
							&leafNode[int, int]{
								Runts:  []int{11, 13, 15, 17},
								Values: []int{11, 13, 15, -1},
							},
							leafB,
							leafC,
						))
				})
				t.Run("newSibling", func(t *testing.T) {
					ensureSame(t, newSibling, nil)
				})
			})
			t.Run("key after final key of middle leaf", func(t *testing.T) {
				leafC := &leafNode[int, int]{
					Runts:  []int{31, 33, 35},
					Values: []int{31, 33, 35},
				}
				leafB := &leafNode[int, int]{
					Runts:  []int{21, 23, 25},
					Values: []int{21, 23, 25},
					Next:   leafC,
				}
				leafA := &leafNode[int, int]{
					Runts:  []int{11, 13, 15},
					Values: []int{11, 13, 15},
					Next:   leafB,
				}
				internalA := newInternal(leafA, leafB, leafC)

				newSibling := internalA.updateKey(insertionIndex, 27, order, false, callbackExpects(t, 0, false))

				t.Run("internalA", func(t *testing.T) {
					ensureStructure(t, internalA,
						newInternal(
							leafA,
							&leafNode[int, int]{
								Runts:  []int{21, 23, 25, 27},
								Values: []int{21, 23, 25, -1},
							},
							leafC,
						))
				})
				t.Run("newSibling", func(t *testing.T) {
					ensureSame(t, newSibling, nil)
				})
			})
			t.Run("key after final key of final leaf", func(t *testing.T) {
				leafC := &leafNode[int, int]{
					Runts:  []int{31, 33, 35},
					Values: []int{31, 33, 35},
				}
				leafB := &leafNode[int, int]{
					Runts:  []int{21, 23, 25},
					Values: []int{21, 23, 25},
					Next:   leafC,
				}
				leafA := &leafNode[int, int]{
					Runts:  []int{11, 13, 15},
					Values: []int{11, 13, 15},
					Next:   leafB,
				}
				internalA := newInternal(leafA, leafB, leafC)

				newSibling := internalA.updateKey(insertionIndex, 37, order, false, callbackExpects(t, 0, false))

				t.Run("internalA", func(t *testing.T) {
					ensureStructure(t, internalA,
						newInternal(
							leafA,
							leafB,
							&leafNode[int, int]{
								Runts:  []int{31, 33, 35, 37},
								Values: []int{31, 33, 35, -1},
							},
						))
				})
				t.Run("newSibling", func(t *testing.T) {
					ensureSame(t, newSibling, nil)
				})
			})
		})
		t.Run("key known present", func(t *testing.T) {
			// Implies key is also the first Runt of this and every node
			// below.
			leafC := &leafNode[int, int]{
				Runts:  []int{31, 33, 35, 37},
				Values: []int{31, 33, 35, 37},
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
			internalA := newInternal(leafA, leafB, leafC)

			newSibling := internalA.updateKey(insertionIndex, 11, order, true, callbackExpects(t, 11, true))

			t.Run("internalA", func(t *testing.T) {
				ensureStructure(t, internalA,
					newInternal(
						&leafNode[int, int]{
							Runts:  []int{11, 13, 15, 17},
							Values: []int{-1, 13, 15, 17},
						},
						leafB,
						leafC,
					))
			})
			t.Run("newSibling", func(t *testing.T) {
				ensureSame(t, newSibling, nil)
			})
		})
		t.Run("key is some middle runt", func(t *testing.T) {
			leafC := &leafNode[int, int]{
				Runts:  []int{31, 33, 35, 37},
				Values: []int{31, 33, 35, 37},
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
			internalA := newInternal(leafA, leafB, leafC)

			newSibling := internalA.updateKey(insertionIndex, 21, order, false, callbackExpects(t, 21, true))

			t.Run("internalA", func(t *testing.T) {
				ensureStructure(t, internalA,
					newInternal(
						leafA,
						&leafNode[int, int]{
							Runts:  []int{21, 23, 25, 27},
							Values: []int{-1, 23, 25, 27},
						},
						leafC,
					))
			})
			t.Run("newSibling", func(t *testing.T) {
				ensureSame(t, newSibling, nil)
			})
		})
		t.Run("key is final runt", func(t *testing.T) {
			leafC := &leafNode[int, int]{
				Runts:  []int{31, 33, 35, 37},
				Values: []int{31, 33, 35, 37},
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
			internalA := newInternal(leafA, leafB, leafC)

			newSibling := internalA.updateKey(insertionIndex, 31, order, false, callbackExpects(t, 31, true))

			t.Run("internalA", func(t *testing.T) {
				ensureStructure(t, internalA,
					newInternal(
						leafA,
						leafB,
						&leafNode[int, int]{
							Runts:  []int{31, 33, 35, 37},
							Values: []int{-1, 33, 35, 37},
						},
					))
			})
			t.Run("newSibling", func(t *testing.T) {
				ensureSame(t, newSibling, nil)
			})
		})
		t.Run("key present but not runt", func(t *testing.T) {
			leafC := &leafNode[int, int]{
				Runts:  []int{31, 33, 35, 37},
				Values: []int{31, 33, 35, 37},
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
			internalA := newInternal(leafA, leafB, leafC)

			newSibling := internalA.updateKey(insertionIndex, 23, order, false, callbackExpects(t, 23, true))

			t.Run("internalA", func(t *testing.T) {
				ensureStructure(t, internalA,
					newInternal(
						leafA,
						&leafNode[int, int]{
							Runts:  []int{21, 23, 25, 27},
							Values: []int{21, -1, 25, 27},
						},
						leafC,
					))
			})
			t.Run("newSibling", func(t *testing.T) {
				ensureSame(t, newSibling, nil)
			})
		})
	})

	t.Run("node no split; child splits", func(t *testing.T) {
		t.Run("key absent", func(t *testing.T) {
			t.Run("key before first key of first node", func(t *testing.T) {
				leafB := &leafNode[int, int]{
					Runts:  []int{21, 23, 25, 27},
					Values: []int{21, 23, 25, 27},
				}
				leafA := &leafNode[int, int]{
					Runts:  []int{11, 13, 15, 17},
					Values: []int{11, 13, 15, 17},
					Next:   leafB,
				}
				internalA := newInternal(leafA, leafB)

				newSibling := internalA.updateKey(insertionIndex, 10, order, false, callbackExpects(t, 0, false))

				t.Run("internalA", func(t *testing.T) {
					ensureStructure(t, internalA,
						newInternal(
							&leafNode[int, int]{
								Runts:  []int{10, 11, 13},
								Values: []int{-1, 11, 13},
							},
							&leafNode[int, int]{
								Runts:  []int{15, 17},
								Values: []int{15, 17},
							},
							leafB,
						),
					)
				})
				t.Run("newSibling", func(t *testing.T) {
					ensureSame(t, newSibling, nil)
				})
			})
			t.Run("key between first and second key of first leaf", func(t *testing.T) {
				leafC := &leafNode[int, int]{
					Runts:  []int{31, 33, 35},
					Values: []int{31, 33, 35},
				}
				leafB := &leafNode[int, int]{
					Runts:  []int{21, 23, 25},
					Values: []int{21, 23, 25},
					Next:   leafC,
				}
				leafA := &leafNode[int, int]{
					Runts:  []int{11, 13, 15, 17},
					Values: []int{11, 13, 15, 17},
					Next:   leafB,
				}
				internalA := newInternal(leafA, leafB, leafC)

				newSibling := internalA.updateKey(insertionIndex, 12, order, false, callbackExpects(t, 0, false))

				t.Run("internalA", func(t *testing.T) {
					ensureStructure(t, internalA,
						newInternal(
							&leafNode[int, int]{
								Runts:  []int{11, 12, 13},
								Values: []int{11, -1, 13},
							},
							&leafNode[int, int]{
								Runts:  []int{15, 17},
								Values: []int{15, 17},
							},
							leafB,
							leafC,
						))
				})
				t.Run("newSibling", func(t *testing.T) {
					ensureSame(t, newSibling, nil)
				})
			})
			t.Run("key after final key of first leaf", func(t *testing.T) {
				leafC := &leafNode[int, int]{
					Runts:  []int{31, 33, 35},
					Values: []int{31, 33, 35},
				}
				leafB := &leafNode[int, int]{
					Runts:  []int{21, 23, 25},
					Values: []int{21, 23, 25},
					Next:   leafC,
				}
				leafA := &leafNode[int, int]{
					Runts:  []int{11, 13, 15, 17},
					Values: []int{11, 13, 15, 17},
					Next:   leafB,
				}
				internalA := newInternal(leafA, leafB, leafC)

				newSibling := internalA.updateKey(insertionIndex, 19, order, false, callbackExpects(t, 0, false))

				t.Run("internalA", func(t *testing.T) {
					ensureStructure(t, internalA,
						newInternal(
							&leafNode[int, int]{
								Runts:  []int{11, 13},
								Values: []int{11, 13},
							},
							&leafNode[int, int]{
								Runts:  []int{15, 17, 19},
								Values: []int{15, 17, -1},
							},
							leafB,
							leafC,
						))
				})
				t.Run("newSibling", func(t *testing.T) {
					ensureSame(t, newSibling, nil)
				})
			})
		})
	})

	t.Run("node splits; child splits", func(t *testing.T) {
		t.Run("key absent", func(t *testing.T) {
			t.Run("key before first key of first node", func(t *testing.T) {
				leafD := &leafNode[int, int]{
					Runts:  []int{41, 43, 45, 47},
					Values: []int{41, 43, 45, 47},
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
				internalA := newInternal(leafA, leafB, leafC, leafD)

				newSibling := internalA.updateKey(insertionIndex, 10, order, false, callbackExpects(t, 0, false))

				// NOTE: Because we do not have a single structure to compare
				// against, but rather have the original internal node and its new
				// sibling, we cannot use ensureStructure. Because ensureStructure
				// would handle stitching together next leaf fields, we have to do
				// that manually below.
				leafA2 := &leafNode[int, int]{
					Runts:  []int{15, 17},
					Values: []int{15, 17},
					Next:   leafB,
				}
				leafA1 := &leafNode[int, int]{
					Runts:  []int{10, 11, 13},
					Values: []int{-1, 11, 13},
					Next:   leafA2,
				}

				t.Run("internalA", func(t *testing.T) {
					ensureNodesMatch(t, internalA,
						newInternal(
							leafA1,
							leafA2,
							leafB,
						),
					)
				})
				t.Run("newSibling", func(t *testing.T) {
					ensureNodesMatch(t, newSibling,
						newInternal(
							leafC,
							leafD,
						),
					)
				})
			})
			t.Run("key between first and second key of first leaf", func(t *testing.T) {
				leafD := &leafNode[int, int]{
					Runts:  []int{41, 43, 45, 47},
					Values: []int{41, 43, 45, 47},
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
				internalA := newInternal(leafA, leafB, leafC, leafD)

				newSibling := internalA.updateKey(insertionIndex, 12, order, false, callbackExpects(t, 0, false))

				// NOTE: Because we do not have a single structure to compare
				// against, but rather have the original internal node and its new
				// sibling, we cannot use ensureStructure. Because ensureStructure
				// would handle stitching together next leaf fields, we have to do
				// that manually below.
				leafA2 := &leafNode[int, int]{
					Runts:  []int{15, 17},
					Values: []int{15, 17},
					Next:   leafB,
				}
				leafA1 := &leafNode[int, int]{
					Runts:  []int{11, 12, 13},
					Values: []int{11, -1, 13},
					Next:   leafA2,
				}

				t.Run("internalA", func(t *testing.T) {
					ensureNodesMatch(t, internalA,
						newInternal(
							leafA1,
							leafA2,
							leafB,
						),
					)
				})
				t.Run("newSibling", func(t *testing.T) {
					ensureNodesMatch(t, newSibling,
						newInternal(
							leafC,
							leafD,
						),
					)
				})
			})
			t.Run("key after final key of first leaf", func(t *testing.T) {
				leafC := &leafNode[int, int]{
					Runts:  []int{31, 33, 35},
					Values: []int{31, 33, 35},
				}
				leafB := &leafNode[int, int]{
					Runts:  []int{21, 23, 25},
					Values: []int{21, 23, 25},
					Next:   leafC,
				}
				leafA := &leafNode[int, int]{
					Runts:  []int{11, 13, 15, 17},
					Values: []int{11, 13, 15, 17},
					Next:   leafB,
				}
				internalA := newInternal(leafA, leafB, leafC)

				newSibling := internalA.updateKey(insertionIndex, 19, order, false, callbackExpects(t, 0, false))

				t.Run("internalA", func(t *testing.T) {
					ensureStructure(t, internalA,
						newInternal(
							&leafNode[int, int]{
								Runts:  []int{11, 13},
								Values: []int{11, 13},
							},
							&leafNode[int, int]{
								Runts:  []int{15, 17, 19},
								Values: []int{15, 17, -1},
							},
							leafB,
							leafC,
						))
				})
				t.Run("newSibling", func(t *testing.T) {
					ensureSame(t, newSibling, nil)
				})
			})
		})
	})

	t.Run("failing on tree insert", func(t *testing.T) {
		const order = 2

		leafB := &leafNode[int, int]{
			Runts:  []int{2, 3},
			Values: []int{2, 3},
		}
		leafA := &leafNode[int, int]{
			Runts:  []int{1},
			Values: []int{1},
			Next:   leafB,
		}
		internalA := newInternal(leafA, leafB)

		newSibling := internalA.updateKey(insertionIndex, 4, order, false, callbackExpects(t, 0, false))

		// internalA.render(os.Stderr, "AFTER INSERT 4 internalA : ")
		// newSibling.render(os.Stderr, "AFTER INSERT 4 newSibling: ")

		charlie := &leafNode[int, int]{
			Runts:  []int{3, 4},
			Values: []int{3, -1},
		}
		bravo := &leafNode[int, int]{
			Runts:  []int{2},
			Values: []int{2},
			Next:   charlie,
		}
		alfa := &leafNode[int, int]{
			Runts:  []int{1},
			Values: []int{1},
			Next:   bravo,
		}

		t.Run("internalA", func(t *testing.T) {
			ensureNodesMatch(t, internalA,
				newInternal(
					alfa,
				),
			)
		})

		t.Run("newSibling", func(t *testing.T) {
			ensureNodesMatch(t, newSibling,
				newInternal(
					bravo,
					charlie,
				),
			)
		})
	})
}
