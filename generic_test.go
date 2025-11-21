package gobptree

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"testing"
)

func TestGenericTreeNew(t *testing.T) {
	for _, v := range []int{0, -1, 1, 3, 11} {
		_, err := NewGenericTree[int, int](v)
		if err == nil {
			ensureError(t, err, fmt.Sprintf("multiple of 2: %d", v))
		}
	}
}

func TestGenericTreeDelete(t *testing.T) {
	t.Run("order 2", func(t *testing.T) {
		t.Skip("FIXME: order of 2 panics")

		tree, err := NewGenericTree[int, int](2)
		ensureError(t, err)
		ensureTreeValues(t, tree, nil)

		tree.render(os.Stderr, "EMPTY ")

		values := rand.Perm(8)

		// DEBUG make insertion order deterministic for debugging
		const sortedInsertion = true
		if sortedInsertion {
			sort.Ints(values)
		}

		for _, v := range values {
			tree.Insert(v, v)
			tree.render(os.Stderr, fmt.Sprintf("AFTER Insert(%v) ", v))
		}

		// Ensure all values can be found in the tree.
		ensureTreeValues(t, tree, []int{0, 1, 2, 3, 4, 5, 6, 7})

		// NOTE: Only delete up to but not including the final value, so can
		// verify when only a single datum remaining, the root should point to
		// a leaf node.

		t.Run("delete from non empty tree", func(t *testing.T) {
			for _, v := range values[:len(values)-1] {
				tree.Delete(v)
				tree.render(os.Stderr, fmt.Sprintf("AFTER Remove(%v) ", v))
			}
		})

		final := values[len(values)-1]
		ensureTreeValues(t, tree, []int{final})

		ensureStructure(t, tree.root, &leafNode[int, int]{
			Runts:  []int{final},
			Values: []int{final},
		})

		// NOTE: Now delete the final node, and ensure the root points to an
		// empty leaf node.
		tree.Delete(final)

		tree.render(os.Stderr, fmt.Sprintf("AFTER Remove(%v) ", final))

		ensureStructure(t, tree.root, &leafNode[int, int]{
			Runts:  []int{},
			Values: []int{},
		})

		// NOTE: Should be able to delete from an empty tree without
		// consequence.
		t.Run("delete from empty tree", func(t *testing.T) {
			tree.Delete(final)
		})
	})

	t.Run("order 4", func(t *testing.T) {
		t.Skip("FIXME")
		const order = 4

		tree, err := NewGenericTree[int, int](order)
		ensureError(t, err)

		t.Run("before insertion tree is empty", func(t *testing.T) {
			ensureTreeValues(t, tree, nil)
		})

		tree.render(os.Stderr, "BEFORE ")

		values := rand.Perm(16)

		if false {
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
			tree.Insert(v, v)
		}

		tree.render(os.Stderr, "BEFORE ")

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
				tree.render(os.Stderr, fmt.Sprintf("AFTER Delete(%v) ", v))
			}
		})

		final := values[len(values)-1]
		ensureTreeValues(t, tree, []int{final})

		ensureStructure(t, tree.root, &leafNode[int, int]{
			Runts:  []int{final},
			Values: []int{final},
			Next:   nil,
		})

		// NOTE: Now delete the final node, and ensure the root points to
		// an empty leaf node.
		tree.Delete(final)

		tree.render(os.Stderr, fmt.Sprintf("AFTER Delete(%v) ", final))

		ensureStructure(t, tree.root, &leafNode[int, int]{
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

	t.Run("order 32", func(t *testing.T) {
		t.Skip("FIXME")
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

		ensureStructure(t, tree.root, &leafNode[int, int]{
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
}

func TestGenericTreeInsert(t *testing.T) {
	t.Run("order 2", func(t *testing.T) {
		// t.Skip("FIXME")
		tree, err := NewGenericTree[int, int](2)
		ensureError(t, err)

		t.Run("insert 1", func(t *testing.T) {
			tree.Insert(1, 1)

			ensureStructure(t, tree.root,
				&leafNode[int, int]{
					Runts:  []int{1},
					Values: []int{1},
				},
			)
		})

		t.Run("insert 2", func(t *testing.T) {
			tree.Insert(2, 2)

			ensureStructure(t, tree.root,
				&leafNode[int, int]{
					Runts:  []int{1, 2},
					Values: []int{1, 2},
				},
			)
		})

		t.Run("insert 3", func(t *testing.T) {
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

		const attemptAdoption = false

		t.Run("insert 4", func(t *testing.T) {
			tree.Insert(4, 4)

			tree.render(os.Stderr, "AFTER 4: ")

			if attemptAdoption {
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
					),
				)
			} else {
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
			}
		})

		if attemptAdoption {
			t.Skip("TODO")
			t.Run("insert 5", func(t *testing.T) {
				tree.Insert(5, 5)

				tree.render(os.Stderr, "AFTER 5: ")

				// ensureStructure(t, tree.root,
				// 	newInternal(
				// 		newInternal(
				// 			&leafNode[int, int]{
				// 				Runts:  []int{1, 2},
				// 				Values: []int{1, 2},
				// 			},
				// 		),
				// 		newInternal(
				// 			&leafNode[int, int]{
				// 				Runts:  []int{3},
				// 				Values: []int{3},
				// 			},
				// 			&leafNode[int, int]{
				// 				Runts:  []int{4, 5},
				// 				Values: []int{4, 5},
				// 			},
				// 		),
				// 	),
				// )
				if t.Failed() {
					tree.render(os.Stderr, "AFTER 5: ")
				}
			})

			t.Run("insert 6", func(t *testing.T) {
				t.Skip("FIXME")
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
		} else {
			t.Skip("TODO: tests when adoption is disabled")
		}
	})

	t.Run("order 4", func(t *testing.T) {
		t.Skip("FIXME")
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
		t.Skip("FIXME")
		t.Run("when fewer than order elements", func(t *testing.T) {
			t.Run("when empty", func(t *testing.T) {
				tree, err := NewGenericTree[int, int](4)
				ensureError(t, err)

				leafA, ok := tree.root.(*leafNode[int, int])
				if !ok {
					t.Fatalf("GOT: %v; WANT: %v", ok, false)
				}

				tree.Insert(30, 30)

				ensureLeafNodesMatch(t, leafA, &leafNode[int, int]{
					Runts:  []int{30},
					Values: []int{30},
				})
			})
			t.Run("when less than first runt", func(t *testing.T) {
				tree, err := NewGenericTree[int, int](4)
				ensureError(t, err)

				leafA, ok := tree.root.(*leafNode[int, int])
				if !ok {
					t.Fatalf("GOT: %v; WANT: %v", ok, false)
				}

				tree.Insert(30, 30)
				tree.Insert(10, 10)

				ensureStructure(t, leafA, &leafNode[int, int]{
					Runts:  []int{10, 30},
					Values: []int{10, 30},
				})
			})
			t.Run("when update value", func(t *testing.T) {
				tree, err := NewGenericTree[int, int](4)
				ensureError(t, err)

				leafA, ok := tree.root.(*leafNode[int, int])
				if !ok {
					t.Fatalf("GOT: %v; WANT: %v", ok, false)
				}

				tree.Insert(30, 30)
				tree.Insert(10, 10)
				tree.Insert(30, 333)

				ensureStructure(t, leafA, &leafNode[int, int]{
					Runts:  []int{10, 30},
					Values: []int{10, 333},
				})
			})
			t.Run("when between first and final runt", func(t *testing.T) {
				tree, err := NewGenericTree[int, int](4)
				ensureError(t, err)

				leafA, ok := tree.root.(*leafNode[int, int])
				if !ok {
					t.Fatalf("GOT: %v; WANT: %v", ok, false)
				}

				tree.Insert(30, 30)
				tree.Insert(10, 10)
				tree.Insert(20, 20)

				ensureStructure(t, leafA, &leafNode[int, int]{
					Runts:  []int{10, 20, 30},
					Values: []int{10, 20, 30},
				})
			})
			t.Run("when after final runt", func(t *testing.T) {
				tree, err := NewGenericTree[int, int](4)
				ensureError(t, err)

				leafA, ok := tree.root.(*leafNode[int, int])
				if !ok {
					t.Fatalf("GOT: %v; WANT: %v", ok, false)
				}

				tree.Insert(30, 30)
				tree.Insert(10, 10)
				tree.Insert(20, 20)
				tree.Insert(40, 40)

				ensureStructure(t, leafA, &leafNode[int, int]{
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

				ensureStructure(t, root.Children[0], &leafNode[int, int]{
					Runts:  []int{0, 10, 20},
					Values: []int{0, 10, 20},
					Next:   root.Children[1].(*leafNode[int, int]),
				})

				if got, want := root.Runts[1], 30; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}

				ensureStructure(t, root.Children[1], &leafNode[int, int]{
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

				ensureStructure(t, root.Children[0], &leafNode[int, int]{
					Runts:  []int{10, 20, 25},
					Values: []int{10, 20, 25},
					Next:   root.Children[1].(*leafNode[int, int]),
				})

				if got, want := root.Runts[1], 30; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}

				ensureStructure(t, root.Children[1], &leafNode[int, int]{
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

				ensureStructure(t, root.Children[0], &leafNode[int, int]{
					Runts:  []int{10, 20},
					Values: []int{10, 20},
					Next:   root.Children[1].(*leafNode[int, int]),
				})

				if got, want := root.Runts[1], 30; got != want {
					t.Fatalf("GOT: %v; WANT: %v", got, want)
				}

				ensureStructure(t, root.Children[1], &leafNode[int, int]{
					Runts:  []int{30, 40, 50},
					Values: []int{30, 40, 50},
				})
			})
		})
	})

	t.Run("insert elements in an order that used to cause failure", func(t *testing.T) {
		const order = 4

		values := []int{
			0,
			6,
			5,
			9,
			7,
			12,
			11,
			2,
			13,
			15,
		}

		tree, err := NewGenericTree[int, int](order)
		ensureError(t, err)

		for _, v := range values {
			tree.Insert(v, v)
			// tree.render(os.Stderr, fmt.Sprintf("AFTER Insert(%v) ", v))
		}

		ensureStructure(t, tree.root,
			newInternal(
				&leafNode[int, int]{
					Runts:  []int{0, 2, 5},
					Values: []int{0, 2, 5},
				},
				&leafNode[int, int]{
					Runts:  []int{6, 7},
					Values: []int{6, 7},
				},
				&leafNode[int, int]{
					Runts:  []int{9, 11},
					Values: []int{9, 11},
				},
				&leafNode[int, int]{
					Runts:  []int{12, 13, 15},
					Values: []int{12, 13, 15},
				},
			))

		if t.Failed() {
			tree.render(os.Stderr, "AFTER ")
		}
	})

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
		internalA := newInternal(leafA, leafB)

		tree := &GenericTree[int, int]{
			root:           internalA,
			insertionIndex: insertionIndexSelect[int](),
			order:          4,
			minSize:        2,
		}

		tree.Insert(11, 11)

		if got, want := internalA.Runts[0], 11; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
	})
}

func TestGenericTreeRebalance(t *testing.T) {
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
}

func TestGenericTreeSearch(t *testing.T) {
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
}

func TestGenericTreeUpdate(t *testing.T) {
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
}

func TestGenericTreeScanner(t *testing.T) {
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
}
