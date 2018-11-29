package gobptree

import (
	"math/rand"
	"time"
)

const benchmarkItemCount = 1 << 20

var randomizedValues []int

func init() {
	rand.Seed(time.Now().Unix())
	randomizedValues = rand.Perm(benchmarkItemCount)
}
