package gobptree

import (
	"sort"
	"testing"
)

var lotsOfInts []int

var lotsOfStrings = []string{
	"alfa",
	"bravo",
	"charlie",
	"delta",
	"echo",
	"foxtrot",
	"golf",
	"hotel",
	"india",
	"juliett",
	"kilo",
	"lima",
	"mike",
	"november",
	"oscar",
	"papa",
	"quebec",
	"romeo",
	"sierra",
	"tango",
	"uniform",
	"victor",
	"whiskey",
	"xray",
	"yankee",
	"zulu",
}

func init() {
	for i := range 1024 {
		lotsOfInts = append(lotsOfInts, i)
	}
}

func BenchmarkSearchIntsInsertionIndex(b *testing.B) {
	var index int
	var ok bool

	insertionIndex := insertionIndexSelect[int]()

	for b.Loop() {
		// Ensure not optimizing for any of the edge cases by searching for
		// every value from the list in the list.
		for i, v := range lotsOfInts {
			index, ok = insertionIndex(lotsOfInts, v)
			b.Run("index", func(b *testing.B) {
				if got, want := i, v; got != want {
					b.Errorf("GOT: %v; WANT: %v", got, want)
				}
			})
			b.Run("ok", func(b *testing.B) {
				if got, want := ok, true; got != want {
					b.Errorf("GOT: %v; WANT: %v", got, want)
				}
			})
		}
	}

	_ = index
}

func BenchmarkSearchIntsStdlib(b *testing.B) {
	var index int

	for b.Loop() {
		// Ensure not optimizing for any of the edge cases by searching for
		// every value from the list in the list.
		for i, v := range lotsOfInts {
			index = sort.SearchInts(lotsOfInts, v)
			b.Run("index", func(b *testing.B) {
				if got, want := i, v; got != want {
					b.Errorf("GOT: %v; WANT: %v", got, want)
				}
			})
		}
	}

	_ = index
}

func BenchmarkSearchStringsInsertionIndex(b *testing.B) {
	var index int
	var ok bool

	insertionIndex := insertionIndexSelect[string]()

	for b.Loop() {
		// Ensure not optimizing for any of the edge cases by searching for
		// every value from the list in the list.
		for _, v := range lotsOfStrings {
			index, ok = insertionIndex(lotsOfStrings, v)
			b.Run("ok", func(b *testing.B) {
				if got, want := ok, true; got != want {
					b.Errorf("GOT: %v; WANT: %v", got, want)
				}
			})
		}
	}

	_ = index
}

func BenchmarkSearchStringsStdlib(b *testing.B) {
	var index int

	for b.Loop() {
		// Ensure not optimizing for any of the edge cases by searching for
		// every value from the list in the list.
		for _, v := range lotsOfStrings {
			index = sort.SearchStrings(lotsOfStrings, v)
		}
	}

	_ = index
}

func TestGenericBinarySearch(t *testing.T) {
	t.Run("int", func(t *testing.T) {
		t.Run("insertionIndex", func(t *testing.T) {
			t.Run("empty list", func(t *testing.T) {
				const key = 1
				insertionIndex := insertionIndexSelect[int]()
				var slice []int

				i, ok := insertionIndex(slice, key)

				wantIndex := sort.SearchInts(slice, key)
				wantOk := wantIndex < len(slice) && slice[wantIndex] == key

				if got := i; got != wantIndex {
					t.Fatalf("index: GOT: %v; WANT: %v", got, wantIndex)
				}
				if got, want := ok, wantOk; got != want {
					t.Errorf("ok: GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("single item list", func(t *testing.T) {
				insertionIndex := insertionIndexSelect[int]()
				slice := []int{3}

				cases := []struct {
					name string
					key  int
				}{
					{
						name: "key before",
						key:  1,
					},
					{
						name: "key match",
						key:  3,
					},
					{
						name: "key after",
						key:  5,
					},
				}
				for _, tc := range cases {
					t.Run(tc.name, func(t *testing.T) {
						i, ok := insertionIndex(slice, tc.key)

						wantIndex := sort.SearchInts(slice, tc.key)
						wantOk := wantIndex < len(slice) && slice[wantIndex] == tc.key

						if got := i; got != wantIndex {
							t.Errorf("index: GOT: %v; WANT: %v", got, wantIndex)
						}
						if got, want := ok, wantOk; got != want {
							t.Errorf("ok: GOT: %v; WANT: %v", got, want)
						}
					})
				}
			})
			t.Run("multiple item list", func(t *testing.T) {
				insertionIndex := insertionIndexSelect[int]()
				slice := []int{1, 3, 5, 7, 9}

				cases := []struct {
					name string
					key  int
				}{
					{
						name: "key before first",
						key:  0,
					},
					{
						name: "key match first",
						key:  1,
					},
					{
						name: "key between first and second",
						key:  2,
					},
					{
						name: "key match second",
						key:  3,
					},
					{
						name: "key between second and third",
						key:  4,
					},
					{
						name: "key match third",
						key:  5,
					},
					{
						name: "key between third and forth",
						key:  6,
					},
					{
						name: "key match forth",
						key:  7,
					},
					{
						name: "key between forth and fifth",
						key:  8,
					},
					{
						name: "key match fifth",
						key:  9,
					},
					{
						name: "key after fifth",
						key:  10,
					},
				}
				for _, tc := range cases {
					t.Run(tc.name, func(t *testing.T) {
						i, ok := insertionIndex(slice, tc.key)

						wantIndex := sort.SearchInts(slice, tc.key)
						wantOk := wantIndex < len(slice) && slice[wantIndex] == tc.key

						if got := i; got != wantIndex {
							t.Errorf("index: GOT: %v; WANT: %v", got, wantIndex)
						}
						if got, want := ok, wantOk; got != want {
							t.Errorf("ok: GOT: %v; WANT: %v", got, want)
						}
					})
				}
			})
		})
	})

	t.Run("string", func(t *testing.T) {
		t.Run("insertionIndex", func(t *testing.T) {
			t.Run("empty list", func(t *testing.T) {
				const key = "bravo"
				insertionIndex := insertionIndexSelect[string]()
				var slice []string

				i, ok := insertionIndex(slice, key)

				wantIndex := sort.SearchStrings(slice, key)
				wantOk := wantIndex < len(slice) && slice[wantIndex] == key

				if got := i; got != wantIndex {
					t.Fatalf("index: GOT: %v; WANT: %v", got, wantIndex)
				}
				if got, want := ok, wantOk; got != want {
					t.Errorf("ok: GOT: %v; WANT: %v", got, want)
				}
			})
			t.Run("single item list", func(t *testing.T) {
				insertionIndex := insertionIndexSelect[string]()
				slice := []string{"bravo"}

				cases := []struct {
					name string
					key  string
				}{
					{
						name: "key before",
						key:  "alfa",
					},
					{
						name: "key match",
						key:  "bravo",
					},
					{
						name: "key after",
						key:  "charlie",
					},
				}
				for _, tc := range cases {
					t.Run(tc.name, func(t *testing.T) {
						i, ok := insertionIndex(slice, tc.key)

						wantIndex := sort.SearchStrings(slice, tc.key)
						wantOk := wantIndex < len(slice) && slice[wantIndex] == tc.key

						if got := i; got != wantIndex {
							t.Errorf("index: GOT: %v; WANT: %v", got, wantIndex)
						}
						if got, want := ok, wantOk; got != want {
							t.Errorf("ok: GOT: %v; WANT: %v", got, want)
						}
					})
				}
			})
			t.Run("multiple item list", func(t *testing.T) {
				insertionIndex := insertionIndexSelect[string]()
				slice := []string{"bravo", "delta", "foxtrot", "hotel", "juliett"}

				cases := []struct {
					name string
					key  string
				}{
					{
						name: "key before first",
						key:  "alfa",
					},
					{
						name: "key match first",
						key:  "bravo",
					},
					{
						name: "key between first and second",
						key:  "charlie",
					},
					{
						name: "key match second",
						key:  "delta",
					},
					{
						name: "key between second and third",
						key:  "echo",
					},
					{
						name: "key match third",
						key:  "foxtrot",
					},
					{
						name: "key between third and forth",
						key:  "golf",
					},
					{
						name: "key match forth",
						key:  "hotel",
					},
					{
						name: "key between forth and fifth",
						key:  "india",
					},
					{
						name: "key match fifth",
						key:  "juliett",
					},
					{
						name: "key after fifth",
						key:  "kilo",
					},
				}
				for _, tc := range cases {
					t.Run(tc.name, func(t *testing.T) {
						i, ok := insertionIndex(slice, tc.key)

						wantIndex := sort.SearchStrings(slice, tc.key)
						wantOk := wantIndex < len(slice) && slice[wantIndex] == tc.key

						if got := i; got != wantIndex {
							t.Errorf("index: GOT: %v; WANT: %v", got, wantIndex)
						}
						if got, want := ok, wantOk; got != want {
							t.Errorf("ok: GOT: %v; WANT: %v", got, want)
						}
					})
				}
			})
		})
	})

	t.Run("greater than or equal to", func(t *testing.T) {
		t.Run("empty list", func(t *testing.T) {
			i := searchGreaterThanOrEqualTo(1, nil)
			if got, want := i, 0; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
		})
		t.Run("single item list", func(t *testing.T) {
			cases := []struct {
				name  string
				key   int
				index int
			}{
				{
					name:  "key before",
					key:   1,
					index: 0,
				},
				{
					name:  "key match",
					key:   3,
					index: 0,
				},
				{
					name:  "key after",
					key:   5,
					index: 0,
				},
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					i := searchGreaterThanOrEqualTo(tc.key, []int{3})
					if got, want := i, tc.index; got != want {
						t.Fatalf("INDEX: GOT: %v; WANT: %v", got, want)
					}
				})
			}
		})
		t.Run("multiple item list", func(t *testing.T) {
			cases := []struct {
				name  string
				key   int
				index int
			}{
				{
					name:  "key before first",
					key:   0,
					index: 0,
				},
				{
					name:  "key match first",
					key:   1,
					index: 0,
				},
				{
					name:  "key between first and second",
					key:   2,
					index: 1,
				},
				{
					name:  "key match second",
					key:   3,
					index: 1,
				},
				{
					name:  "key between second and third",
					key:   4,
					index: 2,
				},
				{
					name:  "key match third",
					key:   5,
					index: 2,
				},
				{
					name:  "key after third",
					key:   6,
					index: 2,
				},
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					i := searchGreaterThanOrEqualTo(tc.key, []int{1, 3, 5})
					if got, want := i, tc.index; got != want {
						t.Fatalf("INDEX: GOT: %v; WANT: %v", got, want)
					}
				})
			}
		})
	})

	t.Run("less than or equal to", func(t *testing.T) {
		t.Run("empty list", func(t *testing.T) {
			i := searchLessThanOrEqualTo(1, nil)
			if got, want := i, 0; got != want {
				t.Fatalf("GOT: %v; WANT: %v", got, want)
			}
		})
		t.Run("single item list", func(t *testing.T) {
			cases := []struct {
				name  string
				key   int
				index int
			}{
				{
					name:  "key before",
					key:   1,
					index: 0,
				},
				{
					name:  "key match",
					key:   3,
					index: 0,
				},
				{
					name:  "key after",
					key:   5,
					index: 0,
				},
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					i := searchLessThanOrEqualTo(tc.key, []int{3})
					if got, want := i, tc.index; got != want {
						t.Fatalf("INDEX: GOT: %v; WANT: %v", got, want)
					}
				})
			}
		})
		t.Run("multiple item list", func(t *testing.T) {
			cases := []struct {
				name  string
				key   int
				index int
			}{
				{
					name:  "key before first",
					key:   0,
					index: 0,
				},
				{
					name:  "key match first",
					key:   1,
					index: 0,
				},
				{
					name:  "key between first and second",
					key:   2,
					index: 0,
				},
				{
					name:  "key match second",
					key:   3,
					index: 1,
				},
				{
					name:  "key between second and third",
					key:   4,
					index: 1,
				},
				{
					name:  "key match third",
					key:   5,
					index: 2,
				},
				{
					name:  "key after third",
					key:   6,
					index: 2,
				},
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					i := searchLessThanOrEqualTo(tc.key, []int{1, 3, 5})
					if got, want := i, tc.index; got != want {
						t.Fatalf("INDEX: GOT: %v; WANT: %v", got, want)
					}
				})
			}
		})
	})

	t.Run("internalIndexFromLeafIndex", func(t *testing.T) {
		t.Run("empty list", func(t *testing.T) {
			const key = 1
			insertionIndex := insertionIndexSelect[int]()
			var slice []int

			got := internalIndexFromLeafIndex(insertionIndex(slice, key))

			if want := 0; got != want {
				t.Fatalf("index: GOT: %v; WANT: %v", got, want)
			}
		})
		t.Run("single item list", func(t *testing.T) {
			insertionIndex := insertionIndexSelect[int]()
			slice := []int{3}

			cases := []struct {
				name string
				key  int
				want int
			}{
				{
					name: "key before",
					key:  1,
					want: 0,
				},
				{
					name: "key match",
					key:  3,
					want: 0,
				},
				{
					name: "key after",
					key:  5,
					want: 0,
				},
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					got := internalIndexFromLeafIndex(insertionIndex(slice, tc.key))

					if got != tc.want {
						t.Errorf("index: GOT: %v; WANT: %v", got, tc.want)
					}
				})
			}
		})
		t.Run("multiple item list", func(t *testing.T) {
			insertionIndex := insertionIndexSelect[int]()
			slice := []int{1, 3, 5, 7, 9}

			cases := []struct {
				name string
				key  int
				want int
			}{
				{
					name: "key before first",
					key:  0,
					want: 0,
				},
				{
					name: "key match first",
					key:  1,
					want: 0,
				},
				{
					name: "key between first and second",
					key:  2,
					want: 0,
				},
				{
					name: "key match second",
					key:  3,
					want: 1,
				},
				{
					name: "key between second and third",
					key:  4,
					want: 1,
				},
				{
					name: "key match third",
					key:  5,
					want: 2,
				},
				{
					name: "key between third and forth",
					key:  6,
					want: 2,
				},
				{
					name: "key match forth",
					key:  7,
					want: 3,
				},
				{
					name: "key between forth and fifth",
					key:  8,
					want: 3,
				},
				{
					name: "key match fifth",
					key:  9,
					want: 4,
				},
				{
					name: "key after fifth",
					key:  10,
					want: 4,
				},
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					got := internalIndexFromLeafIndex(insertionIndex(slice, tc.key))

					if got != tc.want {
						t.Errorf("index: GOT: %v; WANT: %v", got, tc.want)
					}
				})
			}
		})
	})
}
