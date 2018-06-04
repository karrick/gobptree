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

	fmt.Printf("Creating a list of randomized integers from 0 to %d\n", oneMillion)
	rand.Seed(time.Now().Unix())
	randomizedValues := rand.Perm(oneMillion)

	fmt.Printf("Creating a B+Tree of order %d, using int64 values as keys.\n", order)
	t, err := gobptree.NewInt64Tree(order)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Insert the randomized values into the tree, creating a sorted list.\n")
	// For this example, we do not care about the value associated with each
	// key.
	for _, v := range randomizedValues {
		t.Insert(int64(v), struct{}{})
	}

	fmt.Printf("Scan thru tree, collecting all keys in sorted order.\n")
	var sortedValues []int64
	c := t.NewScanner(-1)
	for c.Scan() {
		// Get the key-value pair for this datum, but only collect the key.
		k, _ := c.Pair()
		sortedValues = append(sortedValues, k)
	}

	fmt.Printf("Searching tree for each value from the sorted list.\n")
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

	fmt.Printf("Deleting all keys from the tree in randomized order.\n")
	for _, v := range randomizedValues {
		t.Delete(int64(v))
	}
}
