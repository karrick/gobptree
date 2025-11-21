package gobptree

import (
	"cmp"
	"unsafe"
)

func insertionIndexSelect[K cmp.Ordered]() func([]K, K) (int, bool) {
	var keyZeroValue K
	if unsafe.Sizeof(uintptr(0)) < unsafe.Sizeof(keyZeroValue) {
		return insertionIndexUsingCompare
	}
	return insertionIndexUsingMachineOpcode
}

// insertionIndexUsingMachineOpcode returns the index of where key would be
// inserted into keys in sorted order. It returns the length of keys when key
// is greater than the largest value in keys.
//
// NOTE: This function is intended to be used when there is a machine opcode
// to compare two values of that data type.
//
// NOTE: This function is tested within this library to be equivalent to
// sort.SearchInts, however, offers an approximately 20% performance
// improvement of the standard library. When the benchmarks of sort.Search
// become faster than this function, then this function should be replaced by
// a call to sort.Search. However, because finding the insertion index is an
// integral part of every tree operation, invoked log (base order) N times for
// each tree operation, a 20% performance improvement is worth the additional
// code from this function.
func insertionIndexUsingMachineOpcode[K cmp.Ordered](keys []K, key K) (int, bool) {
	var lo int

	hi := len(keys)
	if hi == 0 {
		return 0, false // key not present, but would be added at index 0
	}
	hi-- // point to index of final element

loop:
	i := int(uint(lo+hi) >> 1) // avoid overflow

	if key < keys[i] {
		hi = i
		if lo < hi {
			goto loop
		}
		// POST: lo == hi == i, but we have already checked annd know that key
		// is less than this element, so key is not present.
		return lo, false
	}

	if keys[i] < key {
		lo = i + 1
		if lo < hi {
			goto loop
		}
		// POST: lo == hi
		if lo == len(keys) {
			return lo, false
		}
		if keys[lo] < key {
			return lo + 1, false
		}
		return lo, !(key < keys[lo])
	}

	return i, true // key == keys[i]
}

// insertionIndexUsingCompare returns the index of where key would be inserted
// into keys in sorted order. It returns the length of keys when key is
// greater than the largest value in keys.
//
// NOTE: This function is intended to be used when there is not a machine
// opcode to compare two values of that data type.
//
// NOTE: This function is tested within this library to be equivalent to
// sort.SearchInts, however, offers an approximately 20% performance
// improvement of the standard library. When the benchmarks of sort.Search
// become faster than this function, then this function should be replaced by
// a call to sort.Search. However, because finding the insertion index is an
// integral part of every tree operation, invoked log (base order) N times for
// each tree operation, a 20% performance improvement is worth the additional
// code from this function.
func insertionIndexUsingCompare[K cmp.Ordered](keys []K, key K) (int, bool) {
	var lo int

	hi := len(keys)
	if hi == 0 {
		return 0, false
	}
	hi-- // point to index of final element

loop:
	i := int(uint(lo+hi) >> 1) // avoid overflow

	compare := cmp.Compare(key, keys[i])

	if compare == -1 { // key < keys[i] {
		hi = i
		if lo < hi {
			goto loop
		}
		// POST: lo == hi == i, but we have already checked annd know that key
		// is less than this element, so key is not present.
		return lo, false
	}

	if compare == 1 { // keys[i] < key {
		lo = i + 1
		if lo < hi {
			goto loop
		}
		// POST: lo == hi
		if lo == len(keys) {
			return lo, false
		}
		compare = cmp.Compare(key, keys[lo])
		if compare == 1 { // keys[lo] < key {
			return lo + 1, false
		}
		return lo, compare != -1 // true when key < keys[lo]
	}

	return i, true // key == keys[i]
}

// searchGreaterThanOrEqualTo returns the index of the first value from values
// that is greater than or equal to key.
func searchGreaterThanOrEqualTo[K cmp.Ordered](key K, keys []K) int {
	var lo int

	hi := len(keys)
	if hi <= 1 {
		return 0
	}
	hi--

loop:
	i := int(uint(lo+hi) >> 1) // avoid overflow
	if key < keys[i] {
		if hi = i; lo < hi {
			goto loop
		}
		return lo
	}
	if keys[i] < key {
		if lo = i + 1; lo < hi {
			goto loop
		}
		return lo
	}
	return i // match
}

// searchLessThanOrEqualTo returns the index of the first value from values
// that is less than or equal to key.
func searchLessThanOrEqualTo[K cmp.Ordered](key K, keys []K) int {
	index := searchGreaterThanOrEqualTo(key, keys)
	// convert result to less than or equal to
	if index == len(keys) || key < keys[index] {
		if index > 0 {
			return index - 1
		}
	}
	return index
}

func internalIndexFromLeafIndex(index int, ok bool) int {
	if ok || index == 0 {
		return index
	}
	return index - 1
}
