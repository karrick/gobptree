package gobptree

import (
	"cmp"
	"fmt"
	"io"
	"os"
	"strings"
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

const leafNodeLocking = true

func (n *leafNode[K, V]) lock() {
	if leafNodeLocking {
		n.mutex.Lock()
	}
}

func (n *leafNode[K, V]) rlock() {
	if leafNodeLocking {
		n.mutex.RLock()
	}
}

func (n *leafNode[K, V]) runlock() {
	if leafNodeLocking {
		n.mutex.RUnlock()
	}
}

func (n *leafNode[K, V]) unlock() {
	if leafNodeLocking {
		n.mutex.Unlock()
	}
}

// runts is a debugging method.
func (n *leafNode[K, V]) runts() []K {
	// n.rlock()
	// defer n.runlock()
	return n.Runts
}

// absorbFromRight moves all of the sibling node's Runts and Values into left
// node, and sets the left's Next field to the value of the sibling's Next
// field.
//
// NOTE: The sibling must be locked before calling.
func (left *leafNode[K, V]) absorbFromRight(sibling node[K, V]) {
	right := sibling.(*leafNode[K, V])

	if left.Next != right {
		// Only way to get here is upon introduction of a bug in the library.
		panic("BUG: cannot merge leaf with sibling other than next sibling")
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
// NOTE: The sibling must be locked before calling.
//
// NOTE: This method panics when sibling is empty.
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
//
// NOTE: This method panics when sibling is empty.
//
// NOTE: After return the right smallest has changed.
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

// deleteKey removes key and its value from the node, returning the number of
// elements after deletion, and the smallest value in the node.
//
// NOTE: Must hold exclusive lock to node before invocation.
func (n *leafNode[K, V]) deleteKey(minSize int, key K) (int, K) {
	const debug = false

	var smallestRunt K

	// Determine index where the key would be stored.
	index := searchGreaterThanOrEqualTo(key, n.Runts)

	if debug {
		fmt.Fprintf(os.Stderr, "leafNode.deleteKey(%v): BEFORE index: %d; minSize: %d; len: %d; keys: %v\n", key, index, minSize, len(n.Runts), n.Runts)
	}

	lenRunts := len(n.Runts)

	if index == lenRunts || key != n.Runts[index] {
		// When key is not present in the leaf node, there is nothing to be
		// done. Return true because this has not shrunk this leaf node, and
		// presumably it is still at least its minimum size.
		if lenRunts > 0 {
			smallestRunt = n.Runts[0]
		}
		return lenRunts, smallestRunt
	}

	// When the key is present in the leaf node, remove it.

	// Shift all keys after the index to the left by one slot.
	copy(n.Runts[index:], n.Runts[index+1:])
	copy(n.Values[index:], n.Values[index+1:])

	// Shrink the slices by one.
	n.Runts = n.Runts[:len(n.Runts)-1]
	n.Values = n.Values[:len(n.Values)-1]

	// After removing key and its value from this node, return true when the
	// node still has the minimum number of keys; and return false otherwise.
	lenRunts--
	if lenRunts > 0 {
		smallestRunt = n.Runts[0]
	}

	if debug {
		fmt.Fprintf(os.Stderr, "leafNode.deleteKey(%v): AFTER index: %d; minSize: %d; len: %d; keys: %v\n", key, index, minSize, len(n.Runts), n.Runts)
	}

	return lenRunts, smallestRunt
}

func (n *leafNode[K, V]) isInternal() bool { return false }

func (n *leafNode[K, V]) render(iow io.Writer, prefix string) {
	n.rlock()

	fmt.Fprintf(iow, "%s- LEAF:\n", prefix)
	// fmt.Fprintf(iow, "%s- LEAF: %v\n", prefix, n.Runts)

	childPrefix := prefix + "    "
	lenItems := len(n.Runts)

	for i := 0; i < lenItems; i++ {
		fmt.Fprintf(iow, "%s- %v = %v\n", childPrefix, n.Runts[i], n.Values[i])
	}

	// fmt.Fprintf(iow, "]\n")

	n.runlock()
}

func (n *leafNode[K, V]) smallest() K {
	if len(n.Runts) == 0 {
		panic("leaf node has no Children")
	}
	return n.Runts[0]
}

// split will return a new internal node after moving half of its values to
// the new node.
func (n *leafNode[K, V]) split(order int) node[K, V] {
	const debug = false

	newNodeRunts := order >> 1

	if debug {
		fmt.Fprintf(os.Stderr, "leafNode.split(%d): keys: %v\n", order, n.Runts)
	}

	sibling := &leafNode[K, V]{
		Runts:  make([]K, newNodeRunts, order),
		Values: make([]V, newNodeRunts, order),
		Next:   n.Next,
	}

	// Right half of this node moves to sibling.
	copy(sibling.Runts, n.Runts[newNodeRunts:])
	copy(sibling.Values, n.Values[newNodeRunts:])

	// Clear the Runts and pointers from the original node.
	n.Runts = n.Runts[:newNodeRunts]
	n.Values = n.Values[:newNodeRunts]
	n.Next = sibling

	return sibling
}

func (n *leafNode[K, V]) String() string {
	var builder strings.Builder
	builder.WriteString("LEAF: [")
	for i, runt := range n.Runts {
		if i > 0 {
			builder.WriteString(fmt.Sprintf(", %v => %v", runt, n.Values[i]))
		} else {
			builder.WriteString(fmt.Sprintf("%v => %v", runt, n.Values[i]))
		}
	}
	builder.WriteByte(']')
	return builder.String()
}

func (n *leafNode[K, V]) updateKey(key K, order int, knownPresent bool, callback func(V, bool) V) node[K, V] {
	const debug = false

	var keyZeroValue K
	var valueZeroValue V

	n.lock()
	defer n.unlock()

	index := searchGreaterThanOrEqualTo(key, n.Runts)

	if knownPresent || (index < len(n.Runts) && key == n.Runts[index]) {
		// Key already present in node.
		// DONE panic("TEST KEY ALREADY IN LEAF NODE")
		n.Values[index] = callback(n.Values[index], true)
		return nil
	}

	// POST: Key not present in node.

	if len(n.Runts) < order {
		// Can fit new key without a split.

		// DONE panic("TEST NO SPLIT REQUIRED")

		if debug {
			fmt.Fprintf(os.Stderr, "leafNode.updateKey(%v, %d, %t, callback): no split required; %s\n", key, order, knownPresent, n)
		}

		// Append zero values to make room in arrays
		n.Runts = append(n.Runts, keyZeroValue)
		n.Values = append(n.Values, valueZeroValue)

		// Shift elements to the right to make room for new data
		copy(n.Runts[index+1:], n.Runts[index:])
		copy(n.Values[index+1:], n.Values[index:])

		// Store the new key and value
		n.Runts[index] = key
		n.Values[index] = callback(valueZeroValue, false)
		return nil
	}

	if debug {
		fmt.Fprintf(os.Stderr, "leafNode.updateKey(%v, %d, %t, callback): BEFORE SPLIT node: %s\n", key, order, knownPresent, n)
	}

	// This node will need to be split in order to fit another element.
	newSibling := n.split(order).(*leafNode[K, V])

	if debug {
		fmt.Fprintf(os.Stderr, "leafNode.updateKey(%v, %d, %t, callback): AFTER SPLIT node: %s\n", key, order, knownPresent, n)
		fmt.Fprintf(os.Stderr, "leafNode.updateKey(%v, %d, %t, callback): AFTER SPLIT sibling: %s\n", key, order, knownPresent, newSibling)
	}

	// POST: Both this node and sibling are evenly divided, and key and its
	// value need to be added into one or the other of them.
	if key < newSibling.Runts[0] {
		// New key will go to this node.
		index = searchGreaterThanOrEqualTo(key, n.Runts)

		if key > n.Runts[index] {
			if debug {
				fmt.Fprintf(os.Stderr, "leafNode.updateKey(%v, %d, %t, callback): (key inserted after values on left side) index: %d; %s\n", key, order, knownPresent, index, n)
			}
			// DONE panic("TEST KEY INSERTED AFTER VALUES ON LEFT SIDE")
			n.Runts = append(n.Runts, key)
			n.Values = append(n.Values, callback(valueZeroValue, false))
			return newSibling
		}

		if debug {
			fmt.Fprintf(os.Stderr, "leafNode.updateKey(%v, %d, %t, callback): (key inserted either before or between values on left side) index: %d; %s\n", key, order, knownPresent, index, n)
		}

		// Append zero values to make room in arrays
		n.Runts = append(n.Runts, keyZeroValue)
		n.Values = append(n.Values, valueZeroValue)

		// Shift elements to the right to make room for new data
		copy(n.Runts[index+1:], n.Runts[index:])
		copy(n.Values[index+1:], n.Values[index:])

		// Store the new key and value
		n.Runts[index] = key

		newValue := callback(valueZeroValue, false)
		n.Values[index] = newValue

		if debug {
			fmt.Fprintf(os.Stderr, "leafNode.updateKey(%v, %d, %t, callback): (AFTER add to left side) value: %v; node: %s\n", key, order, knownPresent, newValue, n)
			fmt.Fprintf(os.Stderr, "leafNode.updateKey(%v, %d, %t, callback): (AFTER add to left side) value: %v; sibling: %s\n", key, order, knownPresent, newValue, newSibling)
		}

		return newSibling
	}

	// DONE panic("TEST KEY INSERTED AFTER VALUES ON RIGHT SIDE")

	// New key will go to newly created sibling.
	index = searchGreaterThanOrEqualTo(key, newSibling.Runts)

	newValue := callback(valueZeroValue, false)

	if key > newSibling.Runts[index] {
		if debug {
			fmt.Fprintf(os.Stderr, "leafNode.updateKey(%v, %d, %t, callback): (key inserted after values on left side) index: %d; value: %v; %s\n", key, order, knownPresent, index, newValue, newSibling)
		}
		newSibling.Runts = append(newSibling.Runts, key)
		newSibling.Values = append(newSibling.Values, newValue)
		return newSibling
	}

	// Append zero values to make room in arrays
	newSibling.Runts = append(newSibling.Runts, keyZeroValue)
	newSibling.Values = append(newSibling.Values, valueZeroValue)

	// Shift elements to the right to make room for new data
	copy(newSibling.Runts[index+1:], newSibling.Runts[index:])
	copy(newSibling.Values[index+1:], newSibling.Values[index:])

	// Store the new key and value
	newSibling.Runts[index] = key

	newSibling.Values[index] = newValue

	if debug {
		fmt.Fprintf(os.Stderr, "leafNode.updateKey(%v, %d, %t, callback): (AFTER add to right side) value: %v; node: %s\n", key, order, knownPresent, newValue, n)
		fmt.Fprintf(os.Stderr, "leafNode.updateKey(%v, %d, %t, callback): (AFTER add to right side) value: %v; sibling: %s\n", key, order, knownPresent, newValue, newSibling)
	}

	return newSibling
}
