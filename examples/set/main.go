package main

import (
	"fmt"
	"math/rand"
	"sync"
)

const oneMillion = 1000000

func main() {
	concurrency := 64

	set := NewSyncSetInt64()

	var wg sync.WaitGroup
	wg.Add(concurrency)

	for i := range concurrency {
		go func() {
			var count int
			for j := range oneMillion {
				value := int64(j % 10)

				if rand.Int()%4 == 0 {
					set.Set(value)
					count++
				} else {
					_ = set.Exists(value)
					// count--
				}
			}
			fmt.Printf("%d count %d\n", i, count)
			wg.Done()
		}()
	}

	wg.Wait()
}
