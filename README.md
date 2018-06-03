# gobptree

Provides several _nearly_ non-blocking B+Tree data
structures. Deletions fail the not-blocking test because they must
lock the tree from the root node down to the leaf node where a
key-value datum is stored until the deletion completes. Insertions and
Search are non-blocking.

Int64Tree is optimized to use builtin int64 type values for the datum
keys. StringTree is optimized to use builtin string type values for
the datum keys. ComparableTree is designed to use any data structure
type as a datum key that implements the Comparable interface. Namely,
any data structure that has `Less(interface{}) bool`,
`Greater(interface{}) bool`, and `ZeroValue() Comparable`
methods. Examples of this flexible B+Tree implementation are provided
in the `godoc` documentation as well as the test files for the
ComparableTree type.

Insertions in all of the B+Tree data structures from this library are
highly parallelizable, because the nodes will be pre-emptively split
while traversing down the tree if necessary rather than having split
nodes bubble back up from the bottom. This is not only faster, but
allows the tree to release the lock on the parent node before visiting
the child nodes, allowing other operations to continue in parallel
that would otherwise block on the node lock.

As mentioned above, deletions from the tree, on the other hand,
require the lock to be held on each node in the tree until the
algorithm bubbles back up during the final stage of recursion. This is
because the tree will not know whether or not nodes must borrow from
their siblings or merge with their sibling until after the child node
has completed its deletion operation.

Like insertions, B+Tree searches only hold the lock on each node until
the appropriate child node is discovered, so they will not impede
insertions or other search operations.

The `Update` method will search for the specified key and invoke the
specified callback function with the key-value pair associated with
that key, and then finally update the stored value for the key with
the value provided by a callback's return value. If the specified key
was not found, `Update` still invokes the callback function and stores
its return value in the tree as a new key-value pair.

Additionally all B+Trees provided by this library support a
`NewScanner` function that returns a cursor that allows enumeration of
all nodes equal to or greater than the specified key. For instance, if
a tree has keys for all int64 values from 0 through 1000, calling
`NewScanner(10)` will return a scanner that lazily iterates through
all key-value pairs from 10 through 100. However, if the tree held all
odd values from 1 to 100, `NewScanner(10)` would return a cursor that
lazily enumerated all key-value pairs from 11 to 99. Note that if one
go-routine is walking through values from the tree while another
go-routine adds a new value, the new value will be included in the
enumeration provided the insertion completes before the enumeration
arrives at the specified leaf node where the key is found.

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
    const order = 64

    var values, expected []int64

    // Create a B+Tree of the specified order, using int64 values as
    // keys.
    t, err := gobptree.NewInt64Tree(order)
    if err != nil {
        fmt.Fprintf(os.Stderr, "ERROR: %s\n", err)
        os.Exit(1)
    }

    for i := 0; i < oneMillion; i++ {
        expected = append(expected, i)
        t.Insert(i, i)
    }

    // Scan through the B+Tree, collecting all values starting with
    // the value for key 0.
    c := t.NewScanner(0)
    for c.Scan() {
        // Get the key-value pair for this datum, but only need the value.
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
