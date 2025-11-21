package gobptree

import (
	"fmt"
	"testing"
)

func TestNewInt64TreeReturnsErrorWhenInvalidOrder(t *testing.T) {
	for _, v := range []int{0, -1, 1, 3, 11} {
		_, err := NewInt64Tree(v)
		if err == nil {
			ensureError(t, err, fmt.Sprintf("multiple of 2: %d", v))
		}
	}
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
					d.Insert(i, i)
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
					d.Insert(i, i)
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
					d.Insert(i, i)
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
					d.Insert(i, i)
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
			var values []int64

			d, _ := NewInt64Tree(16)
			for i := int64(0); i < 15; i++ {
				d.Insert(i, i)
			}

			c := d.NewScannerAll()
			for c.Scan() {
				_, v := c.Pair()
				values = append(values, v.(int64))
			}
			ensureError(t, c.Close())

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
					d.Insert(i, i)
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
				d.Insert(i, i)
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
			d.Insert(i, i)
		}

		c := d.NewScannerAll()
		for c.Scan() {
			_, v := c.Pair()
			values = append(values, v.(int64))
		}
		ensureError(t, c.Close())

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
	d.Insert(3, int64(3))
	d.Update(2, func(value any, ok bool) any {
		if got, want := ok, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := value, error(nil); got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		return "fourth"
	})
	value, ok = d.Search(2)
	if got, want := ok, true; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
	if got, want := value, "fourth"; got != want {
		t.Errorf("GOT: %v; WANT: %v", got, want)
	}
}

func TestInt64Delete(t *testing.T) {
	const order = 32

	d, err := NewInt64Tree(order)
	if err != nil {
		t.Fatal(err)
	}

	for _, v := range randomizedValues {
		d.Insert(int64(v), int64(v))
	}

	for _, v := range randomizedValues {
		if _, ok := d.Search(int64(v)); !ok {
			t.Fatalf("GOT: %v; WANT: %v", ok, true)
		}
	}

	for _, v := range randomizedValues {
		d.Delete(int64(v))
	}

	t.Run("empty", func(t *testing.T) {
		d.Delete(int64(13))
	})
}

func benchmarkInt64(b *testing.B, order int, values []int) {
	var d *Int64Tree
	var err error

	b.Run("insert", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			d, err = NewInt64Tree(order)
			if err != nil {
				b.Fatal(err)
			}
			for _, v := range values {
				d.Insert(int64(v), int64(v))
			}
		}
	})

	b.Run("search", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, v := range values {
				if _, ok := d.Search(int64(v)); !ok {
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
				d.Delete(int64(v))
			}
		}
	})
}

func BenchmarkInt64Order16(b *testing.B) {
	const order = 16
	benchmarkInt64(b, order, randomizedValues)
}

func BenchmarkInt64Order32(b *testing.B) {
	const order = 32
	benchmarkInt64(b, order, randomizedValues)
}

func BenchmarkInt64Order64(b *testing.B) {
	const order = 64
	benchmarkInt64(b, order, randomizedValues)
}

func BenchmarkInt64Order128(b *testing.B) {
	const order = 128
	benchmarkInt64(b, order, randomizedValues)
}

func BenchmarkInt64Order256(b *testing.B) {
	const order = 256
	benchmarkInt64(b, order, randomizedValues)
}

func BenchmarkInt64Order512(b *testing.B) {
	const order = 512
	benchmarkInt64(b, order, randomizedValues)
}
