package main

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/karrick/gobptree"
	"github.com/karrick/golf"
)

func main() {
	optCount := golf.Int64("count", 1048576, "number of items")
	optOrder := golf.Int64("order", 32, "order of tree")
	optThreads := golf.Int64("threads", 1, "number of insertion threads")
	golf.Parse()

	if *optCount <= 0 {
		fmt.Fprintf(os.Stderr, "cannot run without size greater than 0: %d.", *optCount)
		os.Exit(2)
	}

	if *optThreads <= 0 {
		fmt.Fprintf(os.Stderr, "cannot run without thread count greater than 0: %d.", *optThreads)
		os.Exit(2)
	}

	fmt.Printf("%s: Creating a B+Tree of order %d, using uint32 values as keys.\n", formatTime(), int(*optOrder))
	t, err := gobptree.NewUint32Tree(int(*optOrder))
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("%s: Creating a list of randomized integers from 0 to %d.\n", formatTime(), *optCount)
	// rand.Seed(time.Now().Unix())
	// randomizedValues := rand.Perm(int(*optCount))
	randomizedValues := make([]int, *optCount)

	chunkSize := int(math.Ceil(float64(len(randomizedValues)) / float64(*optThreads))) // 125 / 10 -> 13 per thread
	fmt.Printf("chunk size: %d\n", chunkSize)

	fmt.Printf("%s: Creating a sorted list by inserting the randomized values into the tree.\n", formatTime())

	var wg sync.WaitGroup
	wg.Add(int(*optThreads))

	var fi int
	for i := 0; i < int(*optThreads); i++ {
		li := fi + chunkSize
		if l := len(randomizedValues); li > l {
			li = l
		}

		if false {
			go func(values []int) {
				fmt.Printf("%s: Creating thread with %d randomized values.\n", formatTime(), len(values))
				for _, v := range values {
					// For this example, we do not care about the value associated
					// with each key.
					t.Insert(uint32(v), struct{}{})
				}
				wg.Done()
			}(randomizedValues[fi:li])
		}

		fi += chunkSize
	}

	os.Exit(1)
	wg.Wait()

	fmt.Printf("%s: Scanning through tree, collecting all keys in sorted order.\n", formatTime())
	var sortedValues []uint32
	c := t.NewScanner(0)
	for c.Scan() {
		// Get the key-value pair for this datum, but only collect the key.
		k, _ := c.Pair()
		sortedValues = append(sortedValues, k)
	}

	fmt.Printf("%s: Searching tree for each value from the sorted list.\n", formatTime())
	// Ensure enumerated order of the keys are in fact sorted, in other words, a
	// slice of uint32 values from [0 to N).
	for i := uint32(0); i < uint32(*optCount); i++ {
		// Demonstrate searching for key, but disregard the returned value.
		_, ok := t.Search(i)
		if !ok {
			fmt.Fprintf(os.Stderr, "GOT: %v; WANT: %v\n", ok, true)
			os.Exit(1)
		}
		// Ensure sortedValues[i] matches i.
		if got, want := i, sortedValues[i]; got != want {
			fmt.Fprintf(os.Stderr, "GOT: %v; WANT: %v\n", got, want)
		}
	}

	fmt.Printf("%s: Deleting all keys from the tree in randomized order.\n", formatTime())
	for _, v := range randomizedValues {
		t.Delete(uint32(v))
	}

	fmt.Printf("%s: Complete.\n", formatTime())
}

func formatTime() string {
	return strconv.FormatFloat(float64(time.Now().UnixNano())/float64(time.Second), 'f', -1, 64)
}
