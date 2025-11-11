package gobptree

import (
	"cmp"
	"fmt"
	"os"
	"sync"
)

// leafNode represents a leaf node for a GenericTree with keys of cmp.Ordered
// type. Its data is stored in a pair of strided arrays, where Runts[0]
// corresponds to the value in Values[0], and so forth for additional slice
// elements.
type leafNode[K cmp.Ordered, V any] struct {
	// Runts stores the key corresponding to the Values slice. Runts[n]
	// corresponds to Values[n].
	Runts []K

	// Values stores values of the tree. Runts[n] corresponds to Values[n].
	Values []V

	Next *leafNode[K, V] // Next points to next leaf to allow enumeration

	// mutex guards access to this node.
	mutex sync.RWMutex
}

func (n *leafNode[K, V]) lock()    { n.mutex.Lock() }
func (n *leafNode[K, V]) rlock()   { n.mutex.RLock() }
func (n *leafNode[K, V]) runlock() { n.mutex.RUnlock() }
func (n *leafNode[K, V]) unlock()  { n.mutex.Unlock() }

// runts is a debugging method.
func (n *leafNode[K, V]) runts() []K {
	n.rlock()
	defer n.runlock()
	return n.Runts
}

// absorbFromRight moves all of the sibling node's Runts and Values into left
// node, and sets the left's Next field to the value of the sibling's Next
// field.
//
// NOTE: sibling must be locked before calling.
func (left *leafNode[K, V]) absorbFromRight(sibling node[K, V]) {
	right := sibling.(*leafNode[K, V])

	if left.Next != right {
		// Superfluous check
		panic("cannot merge leaf with sibling other than next sibling")
	}

	left.Runts = append(left.Runts, right.Runts...)
	left.Values = append(left.Values, right.Values...)
	left.Next = right.Next

	// Perhaps not strictly needed, but de-allocate sibling fields and release
	// pointers.
	right.Runts = nil
	right.Values = nil
	right.Next = nil
}

// adoptFromLeft moves one element from the sibling node to the right node,
// after making room for it at the beginning of the right node's slices.
//
// NOTE: sibling must be locked before calling.
func (right *leafNode[K, V]) adoptFromLeft(sibling node[K, V]) {
	// TODO: Consider direct copy so do not need the zero values.
	var keyZeroValue K
	var valueZeroValue V

	left := sibling.(*leafNode[K, V])

	// Extend slices of the right node by appending the zero value of the key
	// and value data types.
	right.Runts = append(right.Runts, keyZeroValue)
	right.Values = append(right.Values, valueZeroValue)

	// Shift elements of the right node to the right from 0 to 1.
	copy(right.Runts[1:], right.Runts[0:])
	copy(right.Values[1:], right.Values[0:])

	// Copy the tail element of the left node to the head position of the
	// right node.
	index := len(left.Runts) - 1
	right.Runts[0] = left.Runts[index]
	right.Values[0] = left.Values[index]

	// Shrink the left node by one.
	left.Runts = left.Runts[:index]
	left.Values = left.Values[:index]
}

// adoptFromRight moves one element from the sibling node to the left node.
//
// NOTE: sibling must be locked before calling.
func (left *leafNode[K, V]) adoptFromRight(sibling node[K, V]) {
	right := sibling.(*leafNode[K, V])

	// Copy the head element of the right node to the tail position of the
	// left node.
	left.Runts = append(left.Runts, right.Runts[0])
	left.Values = append(left.Values, right.Values[0])

	// Shift elements of the right node to the left from 1 to 0.
	copy(right.Runts[0:], right.Runts[1:])
	copy(right.Values[0:], right.Values[1:])

	// Shrink the right node by one.
	index := len(right.Runts) - 1
	right.Runts = right.Runts[:index]
	right.Values = right.Values[:index]
}

func (n *leafNode[K, V]) count() int { return len(n.Runts) }

func (n *leafNode[K, V]) deleteKey(minSize int, key K) bool {
	const debug = true

	n.lock()
	defer n.unlock()

	index := searchGreaterThanOrEqualTo(key, n.Runts)

	if debug {
		fmt.Fprintf(os.Stderr, "BEFORE leafNode.deleteKey(%v): index: %d; keys: %v\n", key, index, n.Runts)
	}

	if index == len(n.Runts) || key != n.Runts[index] {
		// When key is not present in the leaf node, there is nothing to be
		// done. Return true because this has not shrunk this leaf node, and
		// presumably it is still at least its minimum size.
		return true
	}

	// When the key is present in the leaf node, remove it.

	// Shift all keys after the index to the left by one slot.
	copy(n.Runts[index:], n.Runts[index+1:])
	copy(n.Values[index:], n.Values[index+1:])

	// Shrink the slices by one.
	n.Runts = n.Runts[:len(n.Runts)-1]
	n.Values = n.Values[:len(n.Values)-1]

	// After removing the key from this node, return true when the node still
	// has the minimum number of keys; and return false otherwise.
	return len(n.Runts) >= minSize
}

func (n *leafNode[K, V]) isInternal() bool { return false }

// maybeSplit splits the node, giving half of its values to its new sibling,
// when the node is too full to accept any more values. When it does return a
// new right sibling, that node is locked.
//
// NOTE: This loop assumes the tree's order is a multiple of 2, which must be
// guarded for at tree instantiation time.
func (n *leafNode[K, V]) maybeSplit(order int) (node[K, V], node[K, V]) {
	if len(n.Runts) < order {
		return n, nil
	}

	newNodeRunts := order >> 1
	sibling := &leafNode[K, V]{
		Runts:  make([]K, newNodeRunts, order),
		Values: make([]V, newNodeRunts, order),
		Next:   n.Next,
	}

	// NOTE: Newly created sibling should be locked before attached to the
	// tree in order to prevent a data race where another goroutine finds this
	// new node.
	sibling.lock()

	// Right half of this node moves to sibling.
	for j := 0; j < newNodeRunts; j++ {
		sibling.Runts[j] = n.Runts[newNodeRunts+j]
		sibling.Values[j] = n.Values[newNodeRunts+j]
	}

	// Clear the Runts and pointers from the original node.
	n.Runts = n.Runts[:newNodeRunts]
	n.Values = n.Values[:newNodeRunts]
	n.Next = sibling

	return n, sibling
}

func (n *leafNode[K, V]) smallest() K {
	if len(n.Runts) == 0 {
		panic("leaf node has no Children")
	}
	return n.Runts[0]
}
