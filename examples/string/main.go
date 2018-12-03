package main

import (
	"bufio"
	"fmt"
	"os"

	"github.com/karrick/gobptree"
)

func main() {
	t, err := gobptree.NewStringTree(64)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %s\n", err)
		os.Exit(1)
	}

	input := bufio.NewScanner(os.Stdin)
	for input.Scan() {
		t.Insert(input.Text(), struct{}{})
	}

	if err := input.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %s\n", err)
		os.Exit(1)
	}

	// Enumerate in sorted order
	cursor := t.NewScanner("")
	for cursor.Scan() {
		k, _ := cursor.Pair()
		fmt.Println(k)
	}
}
