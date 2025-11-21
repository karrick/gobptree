package gobptree

import (
	"cmp"
	"fmt"
	"io"
	"sync"
)

const attemptAdoption = true

// internalNode represents an internal node for a GenericTree with keys of a
// cmp.Ordered type. Its data is stored in a pair of strided arrays, where
// Runts[0] corresponds to the smallest key in Children[0], and so forth for
// additional slice elements.
type internalNode[K cmp.Ordered, V any] struct {
	// Runts stores the smallest key value for the corresponding Children
	// slice. Runts[n] corresponds to Children[n].
	Runts []K

	// Children stores pointers to additional nodes of the tree. Runts[n]
	// corresponds to Chidlren[n].
	Children []node[K, V]

	// mutex guards access to this node.
	mutex sync.RWMutex
}

const internalNodeLocking = false

func (n *internalNode[K, V]) lock() {
	if internalNodeLocking {
		n.mutex.Lock()
	}
}

func (n *internalNode[K, V]) rlock() {
	if internalNodeLocking {
		n.mutex.RLock()
	}
}

func (n *internalNode[K, V]) runlock() {
	if internalNodeLocking {
		n.mutex.RUnlock()
	}
}

func (n *internalNode[K, V]) unlock() {
	if internalNodeLocking {
		n.mutex.Unlock()
	}
}

// runts is a debugging method.
func (n *internalNode[K, V]) runts() []K {
	n.rlock()
	defer n.runlock()
	return n.Runts
}

// absorbFromRight moves all of the sibling node's Runts and Children into
// left node.
//
// NOTE: The sibling must be locked before calling.
func (left *internalNode[K, V]) absorbFromRight(sibling node[K, V]) {
	right := sibling.(*internalNode[K, V])

	left.Runts = append(left.Runts, right.Runts...)
	left.Children = append(left.Children, right.Children...)

	// Perhaps not strictly needed, but de-allocate sibling fields and release
	// pointers.
	right.Runts = nil
	right.Children = nil
}

// adoptFromLeft moves one element from the sibling node to the right node,
// after making room for it at the beginning of the right node's slices.
//
// NOTE: The sibling must be locked before calling.
func (right *internalNode[K, V]) adoptFromLeft(sibling node[K, V]) {
	// TODO: Consider direct copy so do not need the zero values.
	var keyZeroValue K

	left := sibling.(*internalNode[K, V])

	leftIndex := len(left.Runts)
	if leftIndex == 0 {
		return
	}
	leftIndex-- // index of final item

	// Extend slices of the right node by appending the zero value of the key
	// and pointer data types.
	right.Runts = append(right.Runts, keyZeroValue)
	right.Children = append(right.Children, nil)

	// Shift elements of the right node to the right from 0 to 1.
	copy(right.Runts[1:], right.Runts[0:])
	copy(right.Children[1:], right.Children[0:])

	// Copy the tail element of the left node to the head position of the
	// right node.
	right.Runts[0] = left.Runts[leftIndex]
	right.Children[0] = left.Children[leftIndex]

	// Shrink the left node by one.
	left.Runts = left.Runts[:leftIndex]
	left.Children = left.Children[:leftIndex]
}

// adoptFromRight moves one element from the sibling node to the left node.
//
// NOTE: The sibling must be locked before calling.
//
// NOTE: After return the right smallest has changed.
func (left *internalNode[K, V]) adoptFromRight(sibling node[K, V]) {
	right := sibling.(*internalNode[K, V])

	rightIndex := len(right.Runts)
	if rightIndex == 0 {
		return
	}
	rightIndex-- // index of final item

	// Copy the head element of the right node to the tail position of the
	// left node.
	left.Runts = append(left.Runts, right.Runts[0])
	left.Children = append(left.Children, right.Children[0])

	// Shift elements of the right node to the left from 1 to 0.
	copy(right.Runts[0:], right.Runts[1:])
	copy(right.Children[0:], right.Children[1:])

	// Shrink the right node by one.
	right.Runts = right.Runts[:rightIndex]
	right.Children = right.Children[:rightIndex]
}

func (n *internalNode[K, V]) count() int { return len(n.Runts) }

// deleteKey removes key and its value from the node, returning the number of
// elements after deletion.
//
// NOTE: Must hold exclusive lock to node before invocation.
func (n *internalNode[K, V]) deleteKey(insertionIndex func(keys []K, key K) (int, bool), minSize int, key K) (int, K) {
	var smallestRunt K

	debug := newDebug(false, "internalNode.deleteKey(key=%v, minSize=%d)", key, minSize)

	// Determine index of the child node where the key would be stored.
	index := internalIndexFromLeafIndex(insertionIndex(n.Runts, key))

	debug("BEFORE: index=%d runts=%v\n", index, n.Runts)

	// Acquire exclusive lock to the child node.
	child := n.Children[index]
	child.lock()
	defer child.unlock()

	// Delete the key from the child.
	childSize, childSmallest := child.deleteKey(insertionIndex, minSize, key)

	// Updates the runt stored for the corresponding child branch.
	n.Runts[index] = childSmallest

	if childSize >= minSize {
		debug("AFTER: CHILD STILL HAS MINSIZE: child-runts=%v\n", child.runts())
		return len(n.Runts), n.Runts[0]
	}

	// POST: child is too small; need to combine node with one of its
	// immediate neighbors.

	var left, right node[K, V]
	var leftCount, rightCount int

	// Because the child node is too small, we need to have it adopt one
	// element from either its left or its right siblings. We will try the
	// right sibling first to encourage left-leaning trees.

	rightIndex := index + 1
	if rightIndex < len(n.Runts) {
		// When child has a right sibling, check whether the right sibling has
		// more elements than the minimum:
		right = n.Children[rightIndex]

		// Acquire exclusive access to the right sibling.
		right.lock()
		defer right.unlock()

		rightCount = right.count()
		if rightCount > minSize {
			// When right sibling has more then the minimum number of
			// elements, the child node can adopt a single element from its
			// right sibling.
			debug("BEFORE: CHILD ADOPTS FROM RIGHT: child-runts=%v\n", child.runts())
			debug("BEFORE: CHILD ADOPTS FROM RIGHT: right-runts=%v\n", right.runts())

			child.adoptFromRight(right)

			// After the child node has adopted an element from its right
			// sibling, this node, which is the parent to both, has a new runt
			// value for the right sibling.
			n.Runts[rightIndex] = right.smallest()

			// After the child has adopted a single element from its sibling,
			// it has exactly the minimum number of elements.
			debug("AFTER: CHILD ADOPTS FROM RIGHT: runts=%v\n", n.Runts)
			debug("AFTER: CHILD ADOPTS FROM RIGHT: child-runts=%v\n", child.runts())
			debug("AFTER: CHILD ADOPTS FROM RIGHT: right-runts=%v\n", right.runts())
			return len(n.Runts), n.Runts[0]
		}
	}

	leftIndex := index - 1
	if leftIndex >= 0 {
		// When child has a left sibling, check whether the left sibling has
		// more elements than the minimum:
		left = n.Children[leftIndex]

		// Acquire exclusive access to the left sibling.
		left.lock()
		defer left.unlock()

		leftCount = left.count()
		if leftCount > minSize {
			// When left sibling has more then the minimum number of elements,
			// the child node can adopt a single element from its left
			// sibling.
			debug("BEFORE: CHILD ADOPTS FROM LEFT: left-runts=%v\n", left.runts())
			debug("BEFORE: CHILD ADOPTS FROM LEFT: child-runts=%v\n", child.runts())

			child.adoptFromLeft(left)

			// After the child node has adopted an element from its left
			// sibling, this node, which is the parent to both, has a new runt
			// value for the left sibling.
			n.Runts[index] = child.smallest()

			// After the child has adopted a single element from its sibling,
			// it has exactly the minimum number of elements.
			debug("AFTER: CHILD ADOPTS FROM LEFT: runts=%v\n", n.Runts)
			debug("AFTER: CHILD ADOPTS FROM LEFT: left-runts=%v\n", left.runts())
			debug("AFTER: CHILD ADOPTS FROM LEFT: child-runts=%v\n", child.runts())
			return len(n.Runts), n.Runts[0]
		}

		// The child could not adopt an element from either its right or left
		// sibling. Because the child does have a left sibling, have its left
		// sibling absorb all of the child's elements, and eliminate the
		// child.
		debug("BEFORE: LEFT ABSORBS CHILD: left-runts=%v\n", left.runts())
		debug("BEFORE: LEFT ABSORBS CHILD: child-runts=%v\n", child.runts())

		left.absorbFromRight(child)

		// Shift the runt values and children pointers one element to the left
		// to eliminate the child node.
		copy(n.Runts[index:], n.Runts[index+1:])
		copy(n.Children[index:], n.Children[index+1:])

		// Shrink both slices by one element.
		n.Runts = n.Runts[:len(n.Runts)-1]
		n.Children = n.Children[:len(n.Children)-1]

		debug("AFTER: LEFT ABSORBS CHILD: runts=%v\n", n.Runts)
		debug("AFTER: LEFT ABSORBS CHILD: left-runts=%v\n", left.runts())
		debug("AFTER: LEFT ABSORBS CHILD: child-runts=%v\n", child.runts())

		// This internal node has one fewer children after the child was
		// absorbed by its left sibling.
		if len(n.Runts) > 0 {
			return len(n.Runts), n.Runts[0]
		}
		return 0, smallestRunt
	}

	if rightCount > 0 {
		// The child could not adopt an element from its right sibling, and it
		// has no left sibling. Therefore, have the child absorb all of the
		// elements from its right sibling.
		debug("BEFORE: CHILD ABSORBS RIGHT: child-runts=%v\n", child.runts())
		debug("BEFORE: CHILD ABSORBS RIGHT: right-runts=%v\n", right.runts())

		child.absorbFromRight(right)

		// Shift the runt values and children pointers one element to the left
		// to eliminate the right node.
		copy(n.Runts[index+1:], n.Runts[index+2:])
		copy(n.Children[index+1:], n.Children[index+2:])

		// Shrink both slices by one element.
		n.Runts = n.Runts[:len(n.Runts)-1]
		n.Children = n.Children[:len(n.Children)-1]

		debug("AFTER: CHILD ABSORBS RIGHT: runts=%v\n", n.Runts)
		debug("AFTER: CHILD ABSORBS RIGHT: child-runts=%v\n", child.runts())
		debug("AFTER: CHILD ABSORBS RIGHT: right-runts=%v\n", right.runts())

		// This internal node has one fewer Children after the child absorbed
		// its right sibling.
		if len(n.Runts) > 0 {
			return len(n.Runts), n.Runts[0]
		}
		return 0, smallestRunt
	}

	//
	// FIXME: This is a contrived state of the tree.
	//
	// panic("both left and right siblings have no Children")
	debug("AFTER: NO SIBLINGS: runts=%v\n", n.Runts)
	debug("AFTER: NO SIBLINGS: child-runts=%v\n", child.runts())
	if len(n.Runts) > 0 {
		return len(n.Runts), n.Runts[0]
	}
	return 0, smallestRunt
}

func (n *internalNode[K, V]) isInternal() bool { return true }

func (n *internalNode[K, V]) render(iow io.Writer, prefix string) {
	n.rlock()

	_, _ = fmt.Fprintf(iow, "%sINTERNAL:\n", prefix)

	childPrefix := prefix + "    "
	lenItems := len(n.Runts)

	for i := 0; i < lenItems; i++ {
		_, _ = fmt.Fprintf(iow, "%s  - RUNT: %v\n", prefix, n.Runts[i])
		n.Children[i].render(iow, childPrefix)
	}

	n.runlock()
}

func (n *internalNode[K, V]) smallest() K {
	if len(n.Runts) == 0 {
		// Cannot get here unless bug introduced in library.
		panic("BUG: internal node has no Children")
	}
	return n.Runts[0]
}

// split will return a new internal node after moving half of its values to
// the newly created internal node.
func (n *internalNode[K, V]) split(order int) *internalNode[K, V] {
	debug := newDebug(false, "internalNode.split(%v)", order)

	debug("runts=%v\n", n.Runts)

	newNodeRunts := order >> 1

	sibling := &internalNode[K, V]{
		Runts:    make([]K, newNodeRunts, order),
		Children: make([]node[K, V], newNodeRunts, order),
	}

	// Right half of this node moves to sibling.
	copy(sibling.Runts, n.Runts[newNodeRunts:])
	copy(sibling.Children, n.Children[newNodeRunts:])

	// Clear the runts and children pointers from the original node.
	n.Runts = n.Runts[:newNodeRunts]
	n.Children = n.Children[:newNodeRunts]

	return sibling
}

func (n *internalNode[K, V]) String() string {
	return fmt.Sprintf("INTERNAL: %v", n.Runts)
}

// TODO: make internal node orchestrate adoption and handle details currently
// being handled in deleteKey.

// updateKey will locate the key-value pair, creating a place for the pair if
// the key does not yet exist in the tree. It invokes callback with the key's
// value and true if the key was in the tree, or the zero value for the key's
// type and false if the key was not in the tree. This stores the return value
// of callback as the value for key in the tree before returning.
//
// This method returns the new node when this node split in order to
// accommodate the new key.
func (n *internalNode[K, V]) updateKey(insertionIndex func(keys []K, key K) (int, bool), key K, order int, knownPresent bool, callback func(V, bool) (V, error)) (node[K, V], error) {
	var err error
	var keyZeroValue K

	debug := newDebug(false, "internalNode.updateKey(key=%v, order=%d)", key, order)

	n.lock()
	defer n.unlock()

	// OPTIMIZATION: The knownPresent argument is set to true by an ancestor
	// node when it discovers the key is an exact member of its Runts
	// slice. In such cases, the key is already present and stored at a node
	// found by following index 0 to a leaf node. Because the key is already
	// present, this is an update rather than an insertion, and no node
	// splitting will be required..
	if knownPresent {
		// DONE panic("TEST INTERNAL NODE: KEY KNOWN PRESENT")
		debug("KNOWN_PRESENT is true: node=%v\n", n)
		_, err = n.Children[0].updateKey(insertionIndex, key, order, knownPresent, callback)
		return nil, err
	}

	// When the key is not already known to be present, search for key in the
	// runt slice to determine at which child node this key would be stored.
	index, ok := insertionIndex(n.Runts, key)
	index = internalIndexFromLeafIndex(index, ok)

	child := n.Children[index]

	// Check whether key is already present because it short-cuts question
	// about needing to split any nodes.
	if ok {
		// OPTIMIZATION: When key is member of the runts, then it is also a
		// member of the tree. Therefore, this is an update rather than an
		// insertion, and no node splitting will be required.

		// DONE panic("TEST INTERNAL NODE KEY FOUND IN RUNTS OF INTERNAL NODE")

		debug("ALREADY PRESENT: index=%d node=%v\n", index, n)
		_, err = child.updateKey(insertionIndex, key, order, true, callback)
		return nil, err
	}

	// POST: even when the key is not a member of the runts slice, it may
	// still be in the tree. However, we cannot rule out whether a node split
	// will be required at this node or below.

	if child.count() < order {
		// The child can accept another element without splitting.

		// DONE panic("TEST CHILD WILL NOT SPLIT")
		_, err = child.updateKey(insertionIndex, key, order, false, callback)
		if err != nil {
			return nil, err
		}

		n.Runts[index] = child.smallest()
		debug("NOT ALREADY PRESENT: CHILD CAN FIT MORE: index=%d node=%v\n", index, n)
		debug("NOT ALREADY PRESENT: CHILD CAN FIT MORE: child=%v\n", child)
		return nil, nil
	}

	if attemptAdoption {
		// If child has a left sibling that has extra room, first have the
		// left sibling adopt an element from the child, then insert the new
		// element in the child.
		leftIndex := index - 1
		if leftIndex >= 0 {
			// This has a left sibling.
			leftSibling := n.Children[leftIndex]
			if leftSibling.count() < order {
				// When left sibling can accept another node, have it adopt
				// from child, so child has room for this key.
				leftSibling.adoptFromRight(child)
				_, err = child.updateKey(insertionIndex, key, order, false, callback)
				if err != nil {
					// Must restore previous structure when child would be too
					// small.
					child.adoptFromLeft(leftSibling)
					return nil, err
				}
				n.Runts[index] = child.smallest()
				// DONE panic("TEST left adopt from child")
				return nil, nil
			}
		}

		// If child has a right sibling that has extra room, first have the
		// right sibling adopt an element from the child, then insert the new
		// element in the child.
		rightIndex := index + 1
		if rightIndex < len(n.Runts) {
			// This has a right sibling.
			rightSibling := n.Children[rightIndex]
			if rightSibling.count() < order {
				// When right sibling can accept another node, have it adopt
				// from child, so child has room for this key.
				rightSibling.adoptFromLeft(child)
				_, err = child.updateKey(insertionIndex, key, order, false, callback)
				if err != nil {
					// Must restore previous structure when child would be too
					// small.
					child.adoptFromRight(rightSibling)
					return nil, err
				}
				n.Runts[index] = child.smallest()
				// panic("TEST right adopt from child")
				return nil, nil
			}
		}
	}

	debug("NOT ALREADY PRESENT: CHILD CANNOT FIT MORE: node=%v\n", n)
	debug("NOT ALREADY PRESENT: CHILD CANNOT FIT MORE: index=%d child=%v\n", index, child)

	// Insert key into child, and because afterward it may have a new smallest
	// value, update the runt corresponding to the child node after the key
	// was inserted.
	childSibling, err := child.updateKey(insertionIndex, key, order, false, callback)
	if err != nil {
		return nil, err
	}

	n.Runts[index] = child.smallest()

	debug("AFTER child.updateKey: node=%v\n", n)
	debug("AFTER child.updateKey: child=%v\n", child)
	debug("AFTER child.updateKey: childSibling=%v\n", childSibling)

	if childSibling == nil {
		// When the child did not split, simply return.
		// DONE panic("TEST NO NEW CHILD SIBLING")
		return nil, nil
	}

	childSiblingSmallest := childSibling.smallest()
	var attachNode *internalNode[K, V]
	var nodeSibling *internalNode[K, V]

	if len(n.Runts) == order {
		// This node is already full and cannot accommodate the new child
		// sibling node.
		debug("BEFORE n.split: node=%s\n", n)
		nodeSibling = n.split(order)
		debug("AFTER n.split: node=%s\n", n)
		debug("AFTER n.split: nodeSibling=%s\n", nodeSibling)

		nodeSiblingSmallest := nodeSibling.smallest()
		if childSiblingSmallest < nodeSiblingSmallest {
			debug("AFTER n.split: child sibling gets attached to node: %s\n", n)
			attachNode = n
		} else {
			debug("AFTER n.split: child sibling gets attached to node sibling: %s\n", nodeSibling)
			attachNode = nodeSibling
		}

		// but first must attach node and node sibling
	} else {
		debug("NO NODE SPLIT: child sibling gets attached to node: %s\n", n)
		attachNode = n
	}

	// Where will child sibling be added inside attach node?
	index, _ = insertionIndex(attachNode.Runts, childSiblingSmallest)

	// Append zero values to make room in arrays
	attachNode.Runts = append(attachNode.Runts, keyZeroValue)
	attachNode.Children = append(attachNode.Children, nil)
	debug("GROW: index=%d attachNode=%s\n", index, attachNode)

	// Shift elements to the right to make room for new data
	copy(attachNode.Runts[index+1:], attachNode.Runts[index:])
	copy(attachNode.Children[index+1:], attachNode.Children[index:])
	debug("SHIFT: index=%d attachNode=%s\n", index, attachNode)

	// Store the new key and value
	attachNode.Runts[index] = childSiblingSmallest
	attachNode.Children[index] = childSibling
	debug("DONE: index=%d attachNode=%s\n", index, attachNode)

	if nodeSibling != nil {
		// NOTE: This required because caller expects interface and this
		// variable is declared as a pointer to a non-interface.
		debug("RETURN: node sibling: %s\n", nodeSibling)
		return nodeSibling, nil
	}
	return nil, nil
}
