package gobptree

import "math/rand"

const benchmarkItemCount = 1 << 20

var randomizedValues []int

func init() {
	randomizedValues = rand.Perm(benchmarkItemCount)
}
