# gobptree

Provides several _nearly_ non-blocking B+Tree data structures. Deletions fail
the not-blocking test because they must lock the tree from the root node down
to the leaf node where a key-value pair is stored until the deletion
completes. Insertions, Search, and Update functions are non-blocking.

  * ComparableTree
  * GenericTree
  * Int32Tree
  * Int64Tree
  * Uint32Tree
  * Uint64Tree
  * StringTree

`ComparableTree` is designed to use any data structure type as a datum key
that implements the Comparable interface. Namely, any data structure that has
methods for both `Less(interface{}) bool`, and `ZeroValue()
Comparable`. Examples of this flexible B+Tree implementation are provided in
the `godoc` documentation as well as the test files for the ComparableTree
type.

`GenericTree` implements a B+Tree using Go generics, allowing callers to
create a B+Tree that uses any type that satisfies the `cmp.Ordered` type
constraint as keys, and that stores any data type, or values that satisfy any
program specific type constraint. `GenericTree` is designed to replace the
need for all other B+Tree data structures from this library, and is now used
to implement those data structures.

The other B+Tree data structures in this library are left over from before Go
supported generics, and are all implemented using `GenericTree`.

---

Every B+Tree data structure in this library provides the following methods,
each of which is described below.

  * Delete(key)
  * Insert(key, value)
  * NewScanner(key)
  * NewScannerAll()
  * Rebalance(int)
  * Search(key)
  * Update(key, callback)

Insertions in all of the B+Tree data structures from this library are highly
parallelizable, because the nodes will be pre-emptively split while traversing
down from the root to the leaf if necessary rather than needing to split nodes
as its bubbles back up from the leaf nodes. This is not only faster, but
allows the tree to release the lock on the parent node before visiting the
child nodes, allowing other operations to continue in parallel that would
otherwise block on the node lock.

Like `Insert`, B+Tree `Search` only holds the lock on each node until the
appropriate child node is discovered, so they will not impede insertions or
other search operations.

Invoking `Delete` from the tree, in contrast to `Insert` and `Search`, does
require the lock to be held on each node in the tree until the algorithm
bubbles back up during the final stage of recursion. This is because the tree
will not know whether or not nodes must borrow from their siblings or merge
with their sibling until after the child node has completed its deletion
operation.

The `Rebalance` method will rebalance the B+Tree while ensuring that each node
has no more than the number of elements provided as an argument to the
method. For instance, to rebalance an order 64 tree so each node contains
exactly 32 children (except perhaps the final leaf node and its ancestors),
one would invoke `Rebalance(32)`. This could also fully pack a tree so each
node is as full as possible, `Rebalance(64)`. Both of these calls would speed
up all tree traversals by ensuring a balanced tree. However, they can also
leave room for additional growth throughout the tree's structure. This method
must be invoked with a count between 2 and the tree order, inclusive: [2,
order].

The `Update` method will search for the specified key and invoke the specified
callback function with the key-value pair associated with that key, and then
finally update the stored value for the key with the value provided by a
callback's return value. If the specified key was not found, `Update` still
invokes the callback function and stores its return value in the tree as a new
key-value pair. `Update` is lock-free, just like `Insert` and `Search`.
`Insert` is implemented using `Update`.

Additionally this library provides a `NewScanner` function that returns a
cursor that allows enumeration of all nodes equal to or greater than the
specified key in ascending order of the keys. This library also provides
`NewScannerAll` function that returns a cursor that enumerates every key in
ascending order. The cursor data structure returned by each of the B+Tree
structures' `NewScanner` or `NewScannerAll` methods each provide the following
interface. The cursor is designed to be used somewhat similarly to
`bufio.Scanner`, and an example is provided.

  * Close: releases the resources held by the cursor early
  * Pair: returns the key-value pair referenced by the cursor
  * Scan: returns true when additional key-value pairs remain

For example, if a tree has keys for all int64 values from 0 through 1000,
calling `NewScanner(10)` will return a scanner that lazily iterates through
all key-value pairs from 10 through 100. However, if the tree held all odd
values from 1 to 100, `NewScanner(10)` would return a cursor that lazily
enumerated all key-value pairs from 11 to 99. Note that if one goroutine is
walking through values from the tree while another goroutine adds a new value,
the new value will be included in the enumeration provided the insertion
completes before the enumeration arrives at the specified leaf node where the
key is found.

## Overview [![GoDoc](https://godoc.org/github.com/karrick/gobptree?status.svg)](https://godoc.org/github.com/karrick/gobptree)

```Go
package main

import (
    "fmt"
    "math/rand"
    "os"
    "time"

    "github.com/karrick/gobptree"
)

func main() {
    const oneMillion = 1000000
    const order = 64

    // Create a randomized list of int values from [0 to N).
    randomizedValues := rand.Perm(oneMillion)

    // Create a B+Tree of the specified order, using int64 values as keys.
    t, err := gobptree.NewGenerictree[int64, struct{}](order)
    if err != nil {
        fmt.Fprintf(os.Stderr, "ERROR: %s\n", err)
        os.Exit(1)
    }

    // Sort the randomized list of keys by inserting each of them into the
    // tree. For this example, we do not care about the value associated with
    // each key.
    for _, v := range randomizedValues {
        t.Insert(int64(v), struct{}{})
    }

    // Scan through the tree collecting all keys.
    var sortedValues []int64
    c := t.NewScannerAll()
    for c.Scan() {
        // Get the key-value pair for this datum, but only collect the key.
        k, _ := c.Pair()
        sortedValues = append(sortedValues, k)
    }
	c.Close()

    // Ensure enumerated order of the keys are in fact sorted, in other words, a
    // slice of int64 values from [0 to N).
    for i := int64(0); i < oneMillion; i++ {
        // Demonstrate searching for key, but disregard the returned value.
        _, ok := t.Search(i)
        if !ok {
            fmt.Fprintf(os.Stderr, "GOT: %v; WANT: %v", ok, true)
            os.Exit(1)
        }
        // Ensure sortedValues[i] matches i.
        if got, want := i, sortedValues[i]; got != want {
            fmt.Fprintf(os.Stderr, "GOT: %v; WANT: %v", got, want)
        }
    }

    // Demonstrate removing all keys from the tree in randomized order.
    for _, v := range randomizedValues {
        t.Delete(int64(v))
    }
}
```

## Install

```
go get github.com/karrick/gobptree
```

## License

MIT.
