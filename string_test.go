package gobptree

import (
	"fmt"
	"strconv"
	"testing"
)

func TestNewStringTreeReturnsErrorWhenInvalidOrder(t *testing.T) {
	for _, v := range []int{0, -1, 1, 3, 11} {
		_, err := NewStringTree(v)
		if err == nil {
			ensureError(t, err, fmt.Sprintf("multiple of 2: %d", v))
		}
	}
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
		c := d.NewScannerAll()
		for c.Scan() {
			count++
		}
		ensureError(t, c.Close())

		if got, want := count, 0; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}

		ensureError(t, c.Close())
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

			ensureError(t, c.Close())
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

			ensureError(t, c.Close())
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

			ensureError(t, c.Close())
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

		ensureError(t, c.Close())
	})
}

func TestStringTreeUpdate(t *testing.T) {
	d, _ := NewStringTree(8)
	d.Update("1", func(value any, ok bool) any {
		if got, want := ok, false; got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		if got, want := value, error(nil); got != want {
			t.Errorf("GOT: %v; WANT: %v", got, want)
		}
		return "first"
	})
	d.Update("1", func(value any, ok bool) any {
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
	d.Update("2", func(value any, ok bool) any {
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
