package main

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/karrick/gobptree"
	"github.com/karrick/golf"
)

func main() {
	count := golf.Int64("count", 1048576, "number of items")
	order := golf.Int64("order", 32, "order of tree")
	golf.Parse()

	if *count <= 0 {
		fmt.Fprintf(os.Stderr, "cannot run without size greater than 0: %d.", *count)
		os.Exit(2)
	}

	fmt.Printf("%s: Creating a B+Tree of order %d, using uint64 values as keys.\n", formatTime(), int(*order))
	t, err := gobptree.NewUint64Tree(int(*order))
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("%s: Creating a list of randomized integers from 0 to %d.\n", formatTime(), *count)
	rand.Seed(time.Now().Unix())
	randomizedValues := rand.Perm(int(*count))

	fmt.Printf("%s: Creating a sorted list by inserting the randomized values into the tree.\n", formatTime())
	// For this example, we do not care about the value associated with each
	// key.
	for _, v := range randomizedValues {
		t.Insert(uint64(v), struct{}{})
	}

	fmt.Printf("%s: Scanning through tree, collecting all keys in sorted order.\n", formatTime())
	var sortedValues []uint64
	c := t.NewScanner(0)
	for c.Scan() {
		// Get the key-value pair for this datum, but only collect the key.
		k, _ := c.Pair()
		sortedValues = append(sortedValues, k)
	}

	fmt.Printf("%s: Searching tree for each value from the sorted list.\n", formatTime())
	// Ensure enumerated order of the keys are in fact sorted, in other words, a
	// slice of uint64 values from [0 to N).
	for i := uint64(0); i < uint64(*count); i++ {
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

	fmt.Printf("%s: Deleting all keys from the tree in randomized order.\n", formatTime())
	for _, v := range randomizedValues {
		t.Delete(uint64(v))
	}

	fmt.Printf("%s: Complete.\n", formatTime())
}

func formatTime() string {
	return strconv.FormatFloat(float64(time.Now().UnixNano())/float64(time.Second), 'f', -1, 64)
}
