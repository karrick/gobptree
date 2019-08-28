# gobptree

Provides several _nearly_ non-blocking B+Tree data
structures. Deletions fail the not-blocking test because they must
lock the tree from the root node down to the leaf node where a
key-value pair is stored until the deletion completes. Insertions and
Search functions are non-blocking.

  * Int32Tree
  * Int64Tree
  * Uint32Tree
  * Uint64Tree
  * StringTree
  * ComparableTree

ComparableTree is designed to use any data structure type as a datum
key that implements the Comparable interface. Namely, any data
structure that has `Less(interface{}) bool`, and `ZeroValue()
Comparable` methods. Examples of this flexible B+Tree implementation
are provided in the `godoc` documentation as well as the test files
for the ComparableTree type.

Other tree types are provided as optimized versions of their
respective data types.

---

Every B+Tree data structure in this library provides the following
methods, each of which is described below.

  * Delete(key)
  * Insert(key, value)
  * Search(key)
  * Update(key, callback)
  * NewScanner(key)

Insertions in all of the B+Tree data structures from this library are
highly parallelizable, because the nodes will be pre-emptively split
while traversing down from the root to the leaf if necessary rather
than having split nodes bubble back up from the bottom. This is not
only faster, but allows the tree to release the lock on the parent
node before visiting the child nodes, allowing other operations to
continue in parallel that would otherwise block on the node lock.

Like `Insert`, B+Tree `Search` only holds the lock on each node until
the appropriate child node is discovered, so they will not impede
insertions or other search operations.

In contrast to `Insert` and `Search`, however, invoking `Delete` from
the tree, require the lock to be held on each node in the tree until
the algorithm bubbles back up during the final stage of
recursion. This is because the tree will not know whether or not nodes
must borrow from their siblings or merge with their sibling until
after the child node has completed its deletion operation.

The `Update` method will search for the specified key and invoke the
specified callback function with the key-value pair associated with
that key, and then finally update the stored value for the key with
the value provided by a callback's return value. If the specified key
was not found, `Update` still invokes the callback function and stores
its return value in the tree as a new key-value pair.

Additionally this library provides a `NewScanner` function that
returns a cursor that allows enumeration of all nodes equal to or
greater than the specified key. The cursor data structure returned by
each of the B+Tree structures' `NewScanner` method each provide the
following interface. The cursor is designed to be used somewhat
similarly to `bufio.Scanner`, and an example is provided.

  * Close: releases the resources held by the cursor early
  * Pair: returns the key-value pair referenced by the cursor
  * Scan: returns true when additional key-value pairs remain

For example, if a tree has keys for all int64 values from 0 through
1000, calling `NewScanner(10)` will return a scanner that lazily
iterates through all key-value pairs from 10 through 100. However, if
the tree held all odd values from 1 to 100, `NewScanner(10)` would
return a cursor that lazily enumerated all key-value pairs from 11
to 99. Note that if one go-routine is walking through values from the
tree while another go-routine adds a new value, the new value will be
included in the enumeration provided the insertion completes before
the enumeration arrives at the specified leaf node where the key is
found.

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
    rand.Seed(time.Now().Unix())
    randomizedValues := rand.Perm(oneMillion)

    // Create a B+Tree of the specified order, using int64 values as keys.
    t, err := gobptree.NewInt64Tree(order)
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

    // Scan through the tree, collecting all keys starting with the specified
    // key, or the next key in the tree.
    var sortedValues []int64
    c := t.NewScanner(0)
    for c.Scan() {
        // Get the key-value pair for this datum, but only collect the key.
        k, _ := c.Pair()
        sortedValues = append(sortedValues, k)
    }

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
