package gobptree

import (
	"errors"
	"strings"
	"testing"
)

func TestCheckOrder(t *testing.T) {
	cases := []struct {
		i    int
		want error
	}{
		{-1, errors.New("cannot create tree")},
		{-2, errors.New("cannot create tree")},
		{0, errors.New("cannot create tree")},
		{1, errors.New("cannot create tree")},
		{2, nil},
		{3, errors.New("cannot create tree")},
		{4, nil},
		{5, errors.New("cannot create tree")},
		{6, errors.New("cannot create tree")},
		{7, errors.New("cannot create tree")},
		{8, nil},
		{9, errors.New("cannot create tree")},
		{10, errors.New("cannot create tree")},
		{11, errors.New("cannot create tree")},
		{12, errors.New("cannot create tree")},
		{13, errors.New("cannot create tree")},
		{14, errors.New("cannot create tree")},
		{15, errors.New("cannot create tree")},
		{16, nil},
	}

	for _, c := range cases {
		err := checkOrder(c.i)
		if c.want == nil {
			if err != nil {
				t.Errorf("CASE: %v; GOT: %v; WANT: %v", c.i, err, nil)
			}
		} else if err == nil || !strings.Contains(err.Error(), c.want.Error()) {
			t.Errorf("CASE: %v; GOT: %v; WANT: %v", c.i, err, c.want)
		}
	}
}
