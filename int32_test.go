package gobptree

import (
	"fmt"
	"testing"
)

func TestNewInt32TreeReturnsErrorWhenInvalidOrder(t *testing.T) {
	for _, v := range []int{0, -1, 1, 3, 11} {
		_, err := NewInt32Tree(v)
		if err == nil {
			ensureError(t, err, fmt.Sprintf("multiple of 2: %d", v))
		}
	}
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
		c := d.NewScannerAll()
		for c.Scan() {
			count++
		}
		ensureError(t, c.Close())

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

			c := d.NewScannerAll()
			for c.Scan() {
				_, v := c.Pair()
				values = append(values, v.(int32))
			}
			ensureError(t, c.Close())

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
			ensureError(t, c.Close())

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
			ensureError(t, c.Close())

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

		c := d.NewScannerAll()
		for c.Scan() {
			_, v := c.Pair()
			values = append(values, v.(int32))
		}
		ensureError(t, c.Close())

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
	d.Update(1, func(value any, ok bool) any {
		if got, want := ok, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := value, error(nil); got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		return "first"
	})
	d.Update(1, func(value any, ok bool) any {
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
	d.Update(int32(2), func(value any, ok bool) any {
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

func TestInt32Delete(t *testing.T) {
	const order = 32

	d, err := NewInt32Tree(order)
	if err != nil {
		t.Fatal(err)
	}

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

func benchmarkInt32(b *testing.B, order int, values []int) {
	var d *Int32Tree
	var err error

	b.Run("insert", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			d, err = NewInt32Tree(order)
			if err != nil {
				b.Fatal(err)
			}
			for _, v := range values {
				d.Insert(int32(v), int32(v))
			}
		}
	})

	b.Run("search", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, v := range values {
				if _, ok := d.Search(int32(v)); !ok {
					b.Fatalf("GOT: %v; WANT: %v", ok, true)
				}
			}
		}
	})

	b.Run("scan", func(b *testing.B) {
		var ignored int
		for i := 0; i < b.N; i++ {
			var count int
			scanner := d.NewScannerAll()
			for scanner.Scan() {
				count++
			}
			ensureError(b, scanner.Close())
			ignored = count
		}
		_ = ignored
	})

	b.Run("delete", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, v := range values {
				d.Delete(int32(v))
			}
		}
	})
}

func BenchmarkInt32Order16(b *testing.B) {
	const order = 16
	benchmarkInt32(b, order, randomizedValues)
}

func BenchmarkInt32Order32(b *testing.B) {
	const order = 32
	benchmarkInt32(b, order, randomizedValues)
}

func BenchmarkInt32Order64(b *testing.B) {
	const order = 64
	benchmarkInt32(b, order, randomizedValues)
}

func BenchmarkInt32Order128(b *testing.B) {
	const order = 128
	benchmarkInt32(b, order, randomizedValues)
}

func BenchmarkInt32Order256(b *testing.B) {
	const order = 256
	benchmarkInt32(b, order, randomizedValues)
}

func BenchmarkInt32Order512(b *testing.B) {
	const order = 512
	benchmarkInt32(b, order, randomizedValues)
}
