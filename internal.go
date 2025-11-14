package gobptree

import (
	"cmp"
	"fmt"
	"io"
	"os"
	"sync"
)

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

const internalNodeLocking = true

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
//
// NOTE: This method panics when sibling is empty.
func (right *internalNode[K, V]) adoptFromLeft(sibling node[K, V]) {
	// TODO: Consider direct copy so do not need the zero values.
	var keyZeroValue K

	left := sibling.(*internalNode[K, V])

	// Extend slices of the right node by appending the zero value of the key
	// and pointer data types.
	right.Runts = append(right.Runts, keyZeroValue)
	right.Children = append(right.Children, nil)

	// Shift elements of the right node to the right from 0 to 1.
	copy(right.Runts[1:], right.Runts[0:])
	copy(right.Children[1:], right.Children[0:])

	// Copy the tail element of the left node to the head position of the
	// right node.
	index := len(left.Runts) - 1
	right.Runts[0] = left.Runts[index]
	right.Children[0] = left.Children[index]

	// Shrink the left node by one.
	left.Runts = left.Runts[:index]
	left.Children = left.Children[:index]
}

// adoptFromRight moves one element from the sibling node to the left node.
//
// NOTE: The sibling must be locked before calling.
//
// NOTE: This method panics when sibling is empty.
//
// NOTE: After return the right smallest has changed.
func (left *internalNode[K, V]) adoptFromRight(sibling node[K, V]) {
	right := sibling.(*internalNode[K, V])

	// Copy the head element of the right node to the tail position of the
	// left node.
	left.Runts = append(left.Runts, right.Runts[0])
	left.Children = append(left.Children, right.Children[0])

	// Shift elements of the right node to the left from 1 to 0.
	copy(right.Runts[0:], right.Runts[1:])
	copy(right.Children[0:], right.Children[1:])

	// Shrink the right node by one.
	index := len(right.Runts) - 1
	right.Runts = right.Runts[:index]
	right.Children = right.Children[:index]
}

func (n *internalNode[K, V]) count() int { return len(n.Runts) }

// deleteKey removes key and its value from the node, returning the number of
// elements after deletion.
//
// NOTE: Must hold exclusive lock to node before invocation.
func (n *internalNode[K, V]) deleteKey(minSize int, key K) (int, K) {
	const debug = false

	var smallestRunt K

	// Determine index of the child node where the key would be stored.
	index := searchLessThanOrEqualTo(key, n.Runts)

	if debug {
		fmt.Fprintf(os.Stderr, "internalNode.deleteKey(%v): BEFORE index: %d; minSize: %d; len: %d; keys: %v\n", key, index, minSize, len(n.Runts), n.Runts)
	}

	// Acquire exclusive lock to the child node.
	child := n.Children[index]
	child.lock()
	defer child.unlock()

	// Delete the key from the child.
	childSize, childSmallest := child.deleteKey(minSize, key)

	// Updates the runt stored for the corresponding child branch.
	n.Runts[index] = childSmallest

	if childSize >= minSize {
		// Nothing more to be done: quick return.
		lenRunts := len(n.Runts)
		if debug {
			fmt.Fprintf(os.Stderr, "internalNode.deleteKey(%v): AFTER (quick return); index: %d; minSize: %d; len: %d; keys: %v\n", key, index, minSize, lenRunts, n.Runts)
		}
		return lenRunts, n.Runts[0]
	}

	// POST: child is too small; need to combine node with one of its
	// immediate neighbors.

	var leftSibling, rightSibling node[K, V]
	var leftCount, rightCount int

	// Because the child node is too small, we need to have it adopt one
	// element from either its left or its right siblings. We will try the
	// right sibling first to encourage left-leaning trees.

	rightIndex := index + 1
	if rightIndex < len(n.Runts) {
		// When child has a right sibling, check whether the right sibling has
		// more elements than the minimum:
		rightSibling = n.Children[rightIndex]

		// Acquire exclusive access to the right sibling.
		rightSibling.lock()
		defer rightSibling.unlock()

		rightCount = rightSibling.count()
		if rightCount > minSize {
			// When right sibling has more then the minimum number of
			// elements, the child node can adopt a single element from its
			// right sibling.
			child.adoptFromRight(rightSibling)

			// After the child node has adopted an element from its right
			// sibling, this node, which is the parent to both, has a new runt
			// value for the right sibling.
			n.Runts[rightIndex] = rightSibling.smallest()

			// After the child has adopted a single element from its sibling,
			// it has exactly the minimum number of elements.
			lenRunts := len(n.Runts)
			if debug {
				fmt.Fprintf(os.Stderr, "internalNode.deleteKey(%v): AFTER (child adopted from right); index: %d; minSize: %d; len: %d; keys: %v\n", key, index, minSize, lenRunts, n.Runts)
			}
			return lenRunts, n.Runts[0]
		}
	}

	leftIndex := index - 1
	if leftIndex >= 0 {
		// When child has a left sibling, check whether the left sibling has
		// more elements than the minimum:
		leftSibling = n.Children[leftIndex]

		// Acquire exclusive access to the left sibling.
		leftSibling.lock()
		defer leftSibling.unlock()

		leftCount = leftSibling.count()
		if leftCount > minSize {
			if debug {
				fmt.Fprintf(os.Stderr, "internalNode.deleteKey(%v): BEFORE (child adopted from left); index: %d; minSize: %d; left len: %d; keys: %v\n", key, index, minSize, len(leftSibling.runts()), leftSibling.runts())
			}

			// When left sibling has more then the minimum number of elements,
			// the child node can adopt a single element from its left
			// sibling.
			child.adoptFromLeft(leftSibling)

			// After the child node has adopted an element from its left
			// sibling, this node, which is the parent to both, has a new runt
			// value for the left sibling.
			n.Runts[index] = child.smallest()

			// After the child has adopted a single element from its sibling,
			// it has exactly the minimum number of elements.
			lenRunts := len(n.Runts)
			if debug {
				fmt.Fprintf(os.Stderr, "internalNode.deleteKey(%v): AFTER (child adopted from left); index: %d; minSize: %d; left len: %d; keys: %v\n", key, index, minSize, len(leftSibling.runts()), leftSibling.runts())
				fmt.Fprintf(os.Stderr, "internalNode.deleteKey(%v): AFTER (child adopted from left); index: %d; minSize: %d; len: %d; keys: %v\n", key, index, minSize, lenRunts, n.Runts)
			}
			return lenRunts, n.Runts[0]
		}

		// The child could not adopt an element from either its right or left
		// sibling. Because the child does have a left sibling, have it absorb
		// all of the child's elements, and eliminate the child.
		leftSibling.absorbFromRight(child)

		// Shift the runt values and children pointers one element to the left
		// to eliminate the child node.
		copy(n.Runts[index:], n.Runts[index+1:])
		copy(n.Children[index:], n.Children[index+1:])

		// Shrink both slices by one element.
		n.Runts = n.Runts[:len(n.Runts)-1]
		n.Children = n.Children[:len(n.Children)-1]

		// This internal node has one fewer Children after the child was
		// absorbed by its left sibling.
		lenRunts := len(n.Runts)
		if debug {
			fmt.Fprintf(os.Stderr, "internalNode.deleteKey(%v): AFTER (child absorbed by left); index: %d; minSize: %d; len: %d; keys: %v\n", key, index, minSize, lenRunts, n.Runts)
		}
		if lenRunts > 0 {
			smallestRunt = n.Runts[0]
		}
		return lenRunts, smallestRunt
	}

	if rightCount > 0 {
		// The child could not adopt an element from its right sibling, and it
		// has no left sibling. Therefore, have the child absorb all of the
		// elements from its right sibling.
		child.absorbFromRight(rightSibling)

		// Shift the runt values and children pointers one element to the left
		// to eliminate the right node.
		copy(n.Runts[index+1:], n.Runts[index+2:])
		copy(n.Children[index+1:], n.Children[index+2:])

		// Shrink both slices by one element.
		n.Runts = n.Runts[:len(n.Runts)-1]
		n.Children = n.Children[:len(n.Children)-1]

		// This internal node has one fewer Children after the child absorbed
		// its right sibling.
		lenRunts := len(n.Runts)
		if debug {
			fmt.Fprintf(os.Stderr, "internalNode.deleteKey(%v): AFTER (right absorbed by child); index: %d; minSize: %d; len: %d; keys: %v\n", key, index, minSize, lenRunts, n.Runts)
		}
		return lenRunts, n.Runts[0]
	}

	// panic("both left and right siblings have no Children")
	lenRunts := len(n.Runts)
	if debug {
		fmt.Fprintf(os.Stderr, "internalNode.deleteKey(%v): AFTER (no siblings); index: %d; minSize: %d; len: %d; keys: %v\n", key, index, minSize, lenRunts, n.Runts)
	}
	if lenRunts > 0 {
		smallestRunt = n.Runts[0]
	}
	return lenRunts, smallestRunt
}

func (n *internalNode[K, V]) isInternal() bool { return true }

func (n *internalNode[K, V]) render(iow io.Writer, prefix string) {
	n.rlock()

	fmt.Fprintf(iow, "%sINTERNAL: %v\n", prefix, n.Runts)

	childPrefix := prefix + "    "
	lenItems := len(n.Runts)

	for i := 0; i < lenItems; i++ {
		fmt.Fprintf(iow, "%s  - RUNT: %v\n", prefix, n.Runts[i])
		n.Children[i].render(iow, childPrefix)
	}

	n.runlock()
}

func (n *internalNode[K, V]) smallest() K {
	if len(n.Runts) == 0 {
		panic("internal node has no Children")
	}
	return n.Runts[0]
}

// split will return a new internal node after moving half of its values to
// the new node.
func (n *internalNode[K, V]) split(order int) node[K, V] {
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

	// var builder strings.Builder
	// builder.WriteString("INTERNAL: ")
	// for _, runt := range n.Runts {
	// 	builder.WriteString(fmt.Sprintf("(%v)", runt))
	// }
	// return builder.String()
}

// TODO: make internal node orchestrate adoption and handle details currently
// being handled in deleteKey.

// returns new node when this node has split.
func (n *internalNode[K, V]) updateKey(key K, order int, knownPresent bool, callback func(V, bool) V) node[K, V] {
	const attemptAdoption = false
	const debug = true

	var keyZeroValue K

	n.lock()
	defer n.unlock()

	// Get index of child where key would be. Check whether this internal
	// node's runts slice has key at that index. If so, key is already present
	// in tree, and will not need to expand.
	index := searchLessThanOrEqualTo(key, n.Runts)

	if debug {
		fmt.Fprintf(os.Stderr, "internalNode.updateKey(%v, %d, %t, callback): index: %d; node: %s\n", key, order, knownPresent, index, n)
	}

	// Check whether key is already present because it short-cuts question
	// about needing to split any nodes.
	if knownPresent || key == n.Runts[index] {
		// When key is member of the runts, then it is also a member of the
		// tree. Therefore, this is an update rather than an insertion, and no
		// node splitting will be required. This branch is an optimization.

		// DONE panic("TEST KEY ALREADY RUNT")
		_ = n.Children[index].updateKey(key, order, true, callback)
		return nil
	}

	// POST: The key may be in the tree even when it is not a member of the
	// runts slice. However, we cannot rule out whether a node split will be
	// required at this node or below.

	child := n.Children[index]
	if debug {
		fmt.Fprintf(os.Stderr, "internalNode.updateKey(%v, %d, false, callback): index: %d; child: %s\n", key, order, index, child)
	}

	if child.count() < order {
		// The child can accept another element without splitting.

		// DONE panic("TEST CHILD WILL NOT SPLIT")
		_ = child.updateKey(key, order, false, callback)
		return nil
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
				_ = child.updateKey(key, order, false, callback)
				panic("TEST left adopt from child")
				return nil
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
				_ = child.updateKey(key, order, false, callback)
				panic("TEST right adopt from child")
				return nil
			}
		}
	}

	if debug {
		fmt.Fprintf(os.Stderr, "internalNode.updateKey(%v, %d, false, callback): BEFORE child.updateKey: child: %s\n", key, order, child)
	}

	// POST: Any siblings this child may have are already full; however it is
	// still unknown whether or not this is a new or updated node.
	childSibling := child.updateKey(key, order, false, callback)

	if childSibling == nil {
		// When the child did not split, simply return.
		panic("TEST NO NEW CHILD SIBLING")
		return nil
	}

	if debug {
		fmt.Fprintf(os.Stderr, "internalNode.updateKey(%v, %d, false, callback): AFTER child.updateKey: child: %s\n", key, order, child)
		fmt.Fprintf(os.Stderr, "internalNode.updateKey(%v, %d, false, callback): AFTER child.updateKey: child sibling: %s\n", key, order, childSibling)
	}

	// POST: The child split, and therefore this node must also split because
	// it is too small to accommodate another child.
	nodeSibling := n.split(order).(*internalNode[K, V])

	if debug {
		fmt.Fprintf(os.Stderr, "internalNode.updateKey(%v, %d, false, callback): AFTER n.split: node: %s\n", key, order, n)
		fmt.Fprintf(os.Stderr, "internalNode.updateKey(%v, %d, false, callback): AFTER n.split: sibling: %s\n", key, order, nodeSibling)
	}

	// POST: Both this node and sibling are evenly divided, and key and its
	// value need to be added into one or the other of them.

	if key < nodeSibling.Runts[0] {
		// The new key will go to this node.

		const before = false
		if before {
			index = searchGreaterThanOrEqualTo(key, n.Runts)
		} else {
			index = searchLessThanOrEqualTo(key, n.Runts)
		}

		if debug {
			fmt.Fprintf(os.Stderr, "internalNode.updateKey(%v, %d, false, callback): (add key to left) BEFORE index: %d; node: %s\n", key, order, index, n)
		}

		// Append zero values to make room in arrays
		n.Runts = append(n.Runts, keyZeroValue)
		n.Children = append(n.Children, nil)
		if debug {
			fmt.Fprintf(os.Stderr, "internalNode.updateKey(%v, %d, false, callback): (add key to left) GROW index: %d; node: %s\n", key, order, index, n)
		}

		// Shift elements to the right to make room for new data
		copy(n.Runts[index+1:], n.Runts[index:])
		copy(n.Children[index+1:], n.Children[index:])
		if debug {
			fmt.Fprintf(os.Stderr, "internalNode.updateKey(%v, %d, false, callback): (add key to left) SHIFT index: %d; node: %s\n", key, order, index, n)
		}

		n.Children[index+1] = childSibling

		if before {
			n.Runts[index] = child.smallest() // the child may have a new smallest value
			n.Runts[index+1] = childSibling.smallest() // the child's new sibling has a yet unknown smallest value
		} else {
			for i, child := range n.Children {
				n.Runts[i] = child.smallest()
			}
		}

		if debug {
			fmt.Fprintf(os.Stderr, "internalNode.updateKey(%v, %d, false, callback): (add key to left) DONE node: %s\n", key, order, n)
			fmt.Fprintf(os.Stderr, "internalNode.updateKey(%v, %d, false, callback): (add key to left) DONE sibling: %s\n", key, order, nodeSibling)
		}

		// panic("TEST LEFT")
		return nodeSibling
	}

	// The new key will go to newly created sibling.
	index = searchGreaterThanOrEqualTo(key, nodeSibling.Runts)

	if debug {
		fmt.Fprintf(os.Stderr, "internalNode.updateKey(%v, %d, false, callback): (add key to left) BEFORE index: %d; sibling: %s\n", key, order, index, nodeSibling)
	}

	// Append zero values to make room in arrays
	nodeSibling.Runts = append(nodeSibling.Runts, keyZeroValue)
	nodeSibling.Children = append(nodeSibling.Children, nil)

	if debug {
		fmt.Fprintf(os.Stderr, "internalNode.updateKey(%v, %d, false, callback): (add key to left) GROW index: %d; sibling: %s\n", key, order, index, nodeSibling)
	}

	// Shift elements to the right to make room for new data
	copy(nodeSibling.Runts[index+1:], nodeSibling.Runts[index:])
	copy(nodeSibling.Children[index+1:], nodeSibling.Children[index:])

	if debug {
		fmt.Fprintf(os.Stderr, "internalNode.updateKey(%v, %d, false, callback): (add key to left) SHIFT index: %d; sibling: %s\n", key, order, index, nodeSibling)
	}

	// Store the new key and value
	nodeSibling.Runts[index] = childSibling.smallest()
	nodeSibling.Children[index] = childSibling

	if debug {
		fmt.Fprintf(os.Stderr, "internalNode.updateKey(%v, %d, false, callback): (add key to right) DONE node: %s\n", key, order, n)
		fmt.Fprintf(os.Stderr, "internalNode.updateKey(%v, %d, false, callback): (add key to right) DONE sibling: %s\n", key, order, nodeSibling)
	}

	// panic("TEST RIGHT")
	return nodeSibling
}
