package gobptree

import (
	"cmp"
	"fmt"
	"io"
	"strings"
	"sync"
)

// leafNode represents a leafNode node for a GenericTree with keys of
// cmp.Ordered type. Its data is stored in a pair of strided arrays, where
// Runts[0] corresponds to the value in Values[0], and so forth for additional
// slice elements.
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
	if leafNodeLocking {
		if false { // disable locking for this
			n.rlock()
			defer n.runlock()
		}
	}
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
func (right *leafNode[K, V]) adoptFromLeft(sibling node[K, V]) {
	// TODO: Consider direct copy so do not need the zero values.
	var keyZeroValue K
	var valueZeroValue V

	left := sibling.(*leafNode[K, V])

	leftIndex := len(left.Runts)
	if leftIndex == 0 {
		return
	}
	leftIndex-- // index of final item

	// Extend slices of the right node by appending the zero value of the key
	// and value data types.
	right.Runts = append(right.Runts, keyZeroValue)
	right.Values = append(right.Values, valueZeroValue)

	// Shift elements of the right node to the right from 0 to 1.
	copy(right.Runts[1:], right.Runts[0:])
	copy(right.Values[1:], right.Values[0:])

	// Copy the tail element of the left node to the head position of the
	// right node.
	right.Runts[0] = left.Runts[leftIndex]
	right.Values[0] = left.Values[leftIndex]

	// Shrink the left node by one.
	left.Runts = left.Runts[:leftIndex]
	left.Values = left.Values[:leftIndex]
}

// adoptFromRight moves one element from the sibling node to the left node.
//
// NOTE: sibling must be locked before calling.
//
// NOTE: After return the right smallest has changed.
func (left *leafNode[K, V]) adoptFromRight(sibling node[K, V]) {
	right := sibling.(*leafNode[K, V])

	rightIndex := len(right.Runts)
	if rightIndex == 0 {
		return
	}
	rightIndex-- // index of final item

	// Copy the head element of the right node to the tail position of the
	// left node.
	left.Runts = append(left.Runts, right.Runts[0])
	left.Values = append(left.Values, right.Values[0])

	// Shift elements of the right node to the left from 1 to 0.
	copy(right.Runts[0:], right.Runts[1:])
	copy(right.Values[0:], right.Values[1:])

	// Shrink the right node by one.
	right.Runts = right.Runts[:rightIndex]
	right.Values = right.Values[:rightIndex]
}

func (n *leafNode[K, V]) count() int { return len(n.Runts) }

// deleteKey removes key and its value from the node, returning the number of
// elements after deletion, and the smallest value in the node.
//
// NOTE: Must hold exclusive lock to node before invocation.
func (n *leafNode[K, V]) deleteKey(insertionIndex func(keys []K, key K) (int, bool), minSize int, key K) (int, K) {
	debug := newDebug(false, "leafNode.deleteKey(key=%v, minSize=%d)", key, minSize)

	lenItems := len(n.Runts)

	// Determine index where the key would be stored.
	index, ok := insertionIndex(n.Runts, key)

	if ok {
		debug("KEY IS PRESENT: BEFORE: index=%d keys=%v\n", index, n.Runts)

		// Shift all keys after the index to the left by one slot.
		copy(n.Runts[index:], n.Runts[index+1:])
		copy(n.Values[index:], n.Values[index+1:])

		// Shrink the slices by one.
		lenItems--
		n.Runts = n.Runts[:lenItems]
		n.Values = n.Values[:lenItems]

		debug("KEY IS PRESENT: AFTER: index=%d keys=%v\n", index, n.Runts)
	} else {
		debug("KEY NOT PRESENT: index=%d keys=%v\n", index, n.Runts)
	}

	if lenItems == 0 {
		var smallestRunt K
		return lenItems, smallestRunt
	}
	return lenItems, n.Runts[0]
}

func (n *leafNode[K, V]) isInternal() bool { return false }

func (n *leafNode[K, V]) render(iow io.Writer, prefix string) {
	n.rlock()

	_, _ = fmt.Fprintf(iow, "%s- LEAF:\n", prefix)

	childPrefix := prefix + "    "
	lenItems := len(n.Runts)

	for i := 0; i < lenItems; i++ {
		_, _ = fmt.Fprintf(iow, "%s- %v = %v\n", childPrefix, n.Runts[i], n.Values[i])
	}

	n.runlock()
}

func (n *leafNode[K, V]) smallest() K {
	if len(n.Runts) == 0 {
		panic("leaf node has no Children")
	}
	return n.Runts[0]
}

// split will return a new leaf node after moving half of its values to the
// newly created leaf node.
func (n *leafNode[K, V]) split(order int) *leafNode[K, V] {
	debug := newDebug(false, "leafNode.split(%d)", order)

	debug("keys=%v\n", n.Runts)

	newNodeRunts := order >> 1

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
			builder.WriteString(fmt.Sprintf(", %v=>%v", runt, n.Values[i]))
		} else {
			builder.WriteString(fmt.Sprintf("%v=>%v", runt, n.Values[i]))
		}
	}
	builder.WriteByte(']')
	return builder.String()
}

// updateKey will locate the key-value pair, creating a place for the pair if
// the key does not yet exist in the tree. It invokes callback with the key's
// value and true if the key was in the tree, or the zero value for the key's
// type and false if the key was not in the tree. This stores the return value
// of callback as the value for key in the tree before returning.
//
// This method returns the new node when this node split in order to
// accommodate the new key.
func (n *leafNode[K, V]) updateKey(insertionIndex func(keys []K, key K) (int, bool), key K, order int, knownPresent bool, callback func(V, bool) V) node[K, V] {
	debug := newDebug(false, "leafNode.updateKey(key=%v, order=%d)", key, order)

	var keyZeroValue K
	var valueZeroValue V

	n.lock()
	defer n.unlock()

	// OPTIMIZATION: The knownPresent argument is set to true by an ancestor
	// node when it discovers the key is an exact member of its Runts
	// slice. In such cases, the key is already present and stored at a node
	// found by following index 0 to a leaf node. Because the key is already
	// present, this is an update rather than an insertion, and no node
	// splitting will be required. When the key is known to be present, then
	// the index is already set correctly, because the key is stored at index
	// 0.
	if knownPresent {
		// DONE panic("TEST LEAF NODE: KEY KNOWN PRESENT")
		debug("KNOWN_PRESENT is true: node=%v\n", n)
		n.Values[0] = callback(n.Values[0], true)
		return nil
	}

	// When the key is not already known to be present because it was found in
	// the runts of an ancestor node, must search for it in the slice of
	// runts.
	index, ok := insertionIndex(n.Runts, key)

	// When the key is found in the runts of the leaf node, then this is an
	// update rather than an insertion; no node splitting will be required.
	if ok {
		// DONE panic("TEST LEAF NODE; KEY FOUND IN RUNTS")
		debug("ALREADY PRESENT: index=%d; node=%v\n", index, n)
		n.Values[index] = callback(n.Values[index], true)
		return nil
	}

	// POST: Key is not present in node.
	newValue := callback(valueZeroValue, false)

	// When this node can accommodate at least one more element, no node
	// splitting will be required.
	if len(n.Runts) < order {
		// DONE panic("TEST LEAF NODE; NO SPLIT REQUIRED")
		debug("BEFORE: NO SPLIT REQUIRED: index=%d node=%v\n", index, n)

		// Append zero values to make room in backing arrays.
		n.Runts = append(n.Runts, keyZeroValue)
		n.Values = append(n.Values, valueZeroValue)

		// Shift elements to the right by one element to make room for new
		// data.
		copy(n.Runts[index+1:], n.Runts[index:])
		copy(n.Values[index+1:], n.Values[index:])

		// Store the new key and value, as returned by the callback function.
		n.Runts[index] = key
		n.Values[index] = callback(valueZeroValue, false)
		debug("AFTER: NO SPLIT REQUIRED: index=%d node=%v\n", index, n)
		return nil
	}

	// POST: Node split is required.

	debug("BEFORE n.split: node=%v\n", n)
	newSibling := n.split(order)

	// POST: Both this node and sibling are evenly divided, and key and its
	// value need to be added into one or the other of them.

	// When key is before smallest key in the new sibling, this key will go
	// into the left node.
	if key < newSibling.Runts[0] {
		index, _ = insertionIndex(n.Runts, key) // ignorning ok because we know not present

		if index == len(n.Runts) {
			// DONE panic("TEST LEAF NODE SPLIT REQUIRED; KEY ON LEFT SIDE; KEY AFTER INDEX")
			n.Runts = append(n.Runts, key)
			n.Values = append(n.Values, newValue)
			debug("AFTER n.split: KEY INSERTED AFTER VALUES ON LEFT SIDE: index=%d node=%v\n", index, n)
			debug("AFTER n.split: KEY INSERTED AFTER VALUES ON LEFT SIDE: index=%d newSibling=%v\n", index, newSibling)
			return newSibling
		}

		// DONE panic("TEST LEAF NODE SPLIT REQUIRED; KEY ON LEFT SIDE; KEY BEFORE INDEX")

		// Append zero values to make room in arrays
		n.Runts = append(n.Runts, keyZeroValue)
		n.Values = append(n.Values, valueZeroValue)

		// Shift elements to the right to make room for new data
		copy(n.Runts[index+1:], n.Runts[index:])
		copy(n.Values[index+1:], n.Values[index:])

		// Store the new key and value
		n.Runts[index] = key
		n.Values[index] = newValue

		debug("AFTER n.split: KEY INSERTED EITHER BEFORE OR BETWEEN VALUES ON LEFT SIDE: index=%d node=%v\n", index, n)
		debug("AFTER n.split: KEY INSERTED EITHER BEFORE OR BETWEEN VALUES ON LEFT SIDE: index=%d newSibling=%v\n", index, newSibling)
		return newSibling
	}

	// New key will go to newly created sibling.

	index, _ = insertionIndex(newSibling.Runts, key) // ignoring ok because we know not present

	if index == len(newSibling.Runts) {
		// DONE panic("TEST LEAF NODE SPLIT REQUIRED; KEY ON RIGHT SIDE; KEY AFTER INDEX")
		newSibling.Runts = append(newSibling.Runts, key)
		newSibling.Values = append(newSibling.Values, newValue)
		debug("AFTER n.split: KEY INSERTED AFTER VALUES ON RIGHT SIDE: index=%d node=%v\n", index, n)
		debug("AFTER n.split: KEY INSERTED AFTER VALUES ON RIGHT SIDE: index=%d newNewSibling=%v\n", index, newSibling)
		return newSibling
	}

	// DONE panic("TEST LEAF NODE SPLIT REQUIRED; KEY ON RIGHT SIDE; KEY BEFORE INDEX")

	// Append zero values to make room in arrays
	newSibling.Runts = append(newSibling.Runts, keyZeroValue)
	newSibling.Values = append(newSibling.Values, valueZeroValue)

	// Shift elements to the right to make room for new data
	copy(newSibling.Runts[index+1:], newSibling.Runts[index:])
	copy(newSibling.Values[index+1:], newSibling.Values[index:])

	// Store the new key and value
	newSibling.Runts[index] = key
	newSibling.Values[index] = newValue

	debug("AFTER n.split: KEY INSERTED BETWEEN VALUES ON RIGHT SIDE: node=%v\n", n)
	debug("AFTER n.split: KEY INSERTED BETWEEN VALUES ON RIGHT SIDE: newSibling=%v\n", newSibling)
	return newSibling
}
