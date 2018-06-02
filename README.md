# gobptree : Provides several non-blocking B+Tree data structures

## Overview [![GoDoc](https://godoc.org/github.com/karrick/gobptree?status.svg)](https://godoc.org/github.com/karrick/gobptree)

```Go
package main

import (
    "fmt"
    "os"

    "github.com/karrick/gobptree"
)

func main() {
    const oneMillion = 1000000

    var values, expected []int64

    t, err := gobptree.NewInt64Tree(8)
    if err != nil {
        fmt.Fprintf(os.Stderr, "ERROR: %s\n", err)
        os.Exit(1)
    }

    for i := 0; i < oneMillion; i++ {
        expected = append(expected, i)
        t.Insert(i, i)
    }

    c := t.NewScanner(0)

    for c.Scan() {
        _, v := c.Pair()
        values = append(values, v.(int64))
    }

    for i := 0; i < len(expected) && i < len(values); i++ {
        _, ok := t.Search(i)
        if !ok {
            fmt.Fprintf(os.Stderr, "GOT: %v; WANT: %v", ok, true)
            os.Exit(1)
        }
        if got, want := values[i], expected[i]; got != want {
            fmt.Fprintf(os.Stderr, "GOT: %v; WANT: %v", got, want)
        }
    }
}
```

## Install

```
go get github.com/karrick/gobptree
```

## License

MIT.
