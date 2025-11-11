package gobptree

import (
	"cmp"
	"fmt"
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

func (n *internalNode[K, V]) lock()    { n.mutex.Lock() }
func (n *internalNode[K, V]) rlock()   { n.mutex.RLock() }
func (n *internalNode[K, V]) runlock() { n.mutex.RUnlock() }
func (n *internalNode[K, V]) unlock()  { n.mutex.Unlock() }

// runts is a debugging method.
func (n *internalNode[K, V]) runts() []K {
	n.rlock()
	defer n.runlock()
	return n.Runts
}

// absorbFromRight moves all of the sibling node's Runts and Children into
// left node.
//
// NOTE: sibling must be locked before calling.
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
// NOTE: sibling must be locked before calling.
func (right *internalNode[K, V]) adoptFromLeft(sibling node[K, V]) {
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
// NOTE: sibling must be locked before calling.
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

	// NOTE: The right smallest has changed.
}

func (n *internalNode[K, V]) count() int { return len(n.Runts) }

// deleteKey removes key and its value from the node, returning true when the
// node has at least minSize elements after the deletion, and returning false
// when the node has fewer elements than minSize.
func (n *internalNode[K, V]) deleteKey(minSize int, key K) bool {
	n.lock()
	defer n.unlock()

	// Determine index of the child node where key would be stored.
	index := searchLessThanOrEqualTo(key, n.Runts)

	if false {
		fmt.Fprintf(os.Stderr, "internalNode.deleteKey(%v): BEFORE index: %d; keys: %v\n", key, index, n.Runts)
	}

	// Acquire exclusive lock to the child node.
	child := n.Children[index]
	child.lock()
	defer child.unlock()

	// Delete the key from the child.
	if child.deleteKey(minSize, key) {
		// Recall that deleteKey returns true when after the delete the child
		// node still has at least minSize elements.
		if false {
			fmt.Fprintf(os.Stderr, "internalNode.deleteKey(%v): AFTER true index: %d; keys: %v\n", key, index, n.Runts)
		}
		return true
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
			if false {
				fmt.Fprintf(os.Stderr, "internalNode.deleteKey(%v): (child adopted from right) runts: %v; child runts: %v; right runts: %v\n", key, n.Runts, child.runts(), rightSibling.runts())
			}
			return true
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
			// When left sibling has more then the minimum number of elements,
			// the child node can adopt a single element from its left
			// sibling.
			child.adoptFromLeft(leftSibling)

			// After the child has adopted a single element from its sibling,
			// it has exactly the minimum number of elements.
			if false {
				fmt.Fprintf(os.Stderr, "internalNode.deleteKey(%v): (child adopted from left) keys: %v\n", key, n.Runts)
			}
			return true
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
		return len(n.Runts) >= minSize
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
		if false {
			fmt.Fprintf(os.Stderr, "internalNode.deleteKey(%v): (child absorbed from right) keys: %v\n", key, n.Runts)
		}
		return len(n.Runts) >= minSize
	}

	// panic("both left and right siblings have no Children")
	if false {
		fmt.Fprintf(os.Stderr, "internalNode.deleteKey(%v): (no siblings) keys: %v\n", key, n.Runts)
	}
	return false
}

func (n *internalNode[K, V]) isInternal() bool { return true }

// maybeSplit splits the node, giving half of its values to its new sibling,
// when the node is too full to accept any more values. When it does return a
// new right sibling, that node is locked.
//
// NOTE: This loop assumes the tree's order is a multiple of 2, which must be
// guarded for at tree instantiation time.
func (n *internalNode[K, V]) maybeSplit(order int) (node[K, V], node[K, V]) {
	if len(n.Runts) < order {
		return n, nil
	}

	newNodeRunts := order >> 1
	sibling := &internalNode[K, V]{
		Runts:    make([]K, newNodeRunts, order),
		Children: make([]node[K, V], newNodeRunts, order),
	}

	// NOTE: Newly created sibling should be locked before attached to the
	// tree in order to prevent a data race where another goroutine finds this
	// new node.
	sibling.lock()

	// Right half of this node moves to sibling.
	for j := 0; j < newNodeRunts; j++ {
		sibling.Runts[j] = n.Runts[newNodeRunts+j]
		sibling.Children[j] = n.Children[newNodeRunts+j]
	}

	// Clear the runts and children pointers from the original node.
	n.Runts = n.Runts[:newNodeRunts]
	n.Children = n.Children[:newNodeRunts]

	return n, sibling
}

func (n *internalNode[K, V]) smallest() K {
	if len(n.Runts) == 0 {
		panic("internal node has no Children")
	}
	return n.Runts[0]
}
