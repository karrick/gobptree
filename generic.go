package gobptree

// NOTE: Because many insertion loops successively insert larger numbers, when
// splitting nodes, rather than splitting a node evenly, consider splitting it
// in a way that puts an extra node on the left side, so the next node to be
// added will end up on the right side, and they both remain balanced.

import (
	"cmp"
	"errors"
	"fmt"
	"sync"
)

// searchGreaterThanOrEqualTo returns the index of the first value from Values
// that is greater than or equal to key.  search for index of runt that is
// greater than or equal to key.
func searchGreaterThanOrEqualTo[K cmp.Ordered](key K, Values []K) int {
	var lo int

	hi := len(Values)
	if hi <= 1 {
		return 0
	}
	hi--

loop:
	m := (lo + hi) >> 1
	v := Values[m]
	if key < v {
		if hi = m; lo < hi {
			goto loop
		}
		return lo
	}
	if key > v {
		if lo = m + 1; lo < hi {
			goto loop
		}
		return lo
	}
	return m
}

// searchLessThanOrEqualTo returns the index of the first value from Values
// that is less than or equal to key.
func searchLessThanOrEqualTo[K cmp.Ordered](key K, Values []K) int {
	index := searchGreaterThanOrEqualTo(key, Values)
	// convert result to less than or equal to
	if index == len(Values) || key < Values[index] {
		if index > 0 {
			return index - 1
		}
	}
	return index
}

// node represents either an internal or a leaf node for a GenericTree with
// keys of any cmp.Ordered type.
type node[K cmp.Ordered] interface {
	absorbRight(node[K])
	adoptFromLeft(node[K])
	adoptFromRight(node[K])
	count() int
	deleteKey(int, K) bool
	isInternal() bool
	lock()
	maybeSplit(order int) (node[K], node[K])
	smallest() K
	unlock()
}

// internalNode represents an internal node for a GenericTree with keys of any
// cmp.Ordered type. Its data is stored in a pair of strided arrays, where
// Runts[0] corresponds to the smallest key in Children[0], and so forth for
// additional slice elements.
type internalNode[K cmp.Ordered] struct {
	// Runts stores the smallest key value for the corresponding Children
	// slice. Runts[n] corresponds to Children[n].
	Runts []K

	// Children stores pointers to additional nodes of the tree. Runts[n]
	// corresponds to Chidlren[n].
	Children []node[K]

	// mutex guards access to this node.
	mutex sync.Mutex
}

// absorbRight moves all of the sibling node's Runts and Children into left
// node.
//
// NOTE: sibling must be locked before calling.
func (left *internalNode[K]) absorbRight(sibling node[K]) {
	right := sibling.(*internalNode[K])

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
func (right *internalNode[K]) adoptFromLeft(sibling node[K]) {
	var keyZeroValue K

	left := sibling.(*internalNode[K])

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
func (left *internalNode[K]) adoptFromRight(sibling node[K]) {
	right := sibling.(*internalNode[K])

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

func (n *internalNode[K]) count() int { return len(n.Runts) }

// deleteKey removes key and its value from the node, returning true when the
// node has at least minSize elements after the deletion, and returning false
// when the node has fewer elements than minSize.
func (n *internalNode[K]) deleteKey(minSize int, key K) bool {
	// Determine index of the child node where key would be stored.
	index := searchLessThanOrEqualTo(key, n.Runts)

	// Acquire exclusive lock to the child node.
	child := n.Children[index]
	child.lock()
	defer child.unlock()

	// Delete the key from the child.
	if child.deleteKey(minSize, key) {
		// Recall that deleteKey returns true when after the delete the child
		// node still has at least minSize elements.
		return true
	}

	// POST: child is too small; need to combine node with one of its
	// immediate neighbors.

	var leftSibling, rightSibling node[K]
	var leftCount, rightCount int

	if index < len(n.Runts)-1 {
		// try right sibling first to encourage left leaning trees
		rightSibling = n.Children[index+1]
		rightSibling.lock()
		defer rightSibling.unlock()
		rightCount = rightSibling.count()
		if rightCount > minSize {
			child.adoptFromRight(rightSibling)
			n.Runts[index+1] = rightSibling.smallest()
			return true
		}
	}
	// POST: If right, it is exactly minimum size.

	if index > 0 {
		// try left sibling
		leftSibling = n.Children[index-1]
		leftSibling.lock()
		defer leftSibling.unlock()
		leftCount = leftSibling.count()
		if leftCount > minSize {
			child.adoptFromLeft(leftSibling)
			return true
		}
	}
	// POST: If left, it is exactly minimum size.

	// POST: Could not adopt a single node from either side, because either
	// child is left or right edge and has no siblings to its left or right, or
	// the siblings it does have each only has the minimum number of Children.

	if leftCount > 0 {
		leftSibling.absorbRight(child)
		copy(n.Runts[index:], n.Runts[index+1:])
		n.Runts = n.Runts[:len(n.Runts)-1]
		copy(n.Children[index:], n.Children[index+1:])
		n.Children = n.Children[:len(n.Children)-1]
		// This node has one fewer Children.
		return len(n.Runts) >= minSize
	}

	// When right has no Children, then should not be in a position where left
	// also has no Children.
	if rightCount == 0 {
		panic("both left and right siblings have no Children")
	}

	child.absorbRight(rightSibling)
	copy(n.Runts[index+1:], n.Runts[index+2:])
	n.Runts = n.Runts[:len(n.Runts)-1]
	copy(n.Children[index+1:], n.Children[index+2:])
	n.Children = n.Children[:len(n.Children)-1]
	// This node has one fewer Children.
	return len(n.Runts) >= minSize
}

func (n *internalNode[K]) isInternal() bool { return true }

func (n *internalNode[K]) lock() { /* i.mutex.Lock() */ }

// maybeSplit splits the node, giving half of its Values to its new sibling,
// when the node is too full to accept any more Values. When it does return a
// new right sibling, that node is locked.
//
// NOTE: This loop assumes the tree's order is a multiple of 2, which must be
// guarded for at tree instantiation time.
func (n *internalNode[K]) maybeSplit(order int) (node[K], node[K]) {
	if len(n.Runts) < order {
		return n, nil
	}

	newNodeRunts := order >> 1
	sibling := &internalNode[K]{
		Runts:    make([]K, newNodeRunts, order),
		Children: make([]node[K], newNodeRunts, order),
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

func (n *internalNode[K]) smallest() K {
	if len(n.Runts) == 0 {
		panic("internal node has no Children")
	}
	return n.Runts[0]
}

func (n *internalNode[K]) unlock() { /* i.mutex.Unlock() */ }

// internalNode represents a leaf node for a GenericTree with keys of any
// cmp.Ordered type. Its data is stored in a pair of strided arrays, where
// Runts[0] corresponds to the value in Values[0], and so forth for additional
// slice elements.
type leafNode[K cmp.Ordered] struct {
	// Runts stores the key corresponding to the Values slice. Runts[n]
	// corresponds to Values[n].
	Runts []K

	// Values stores values of the tree. Runts[n] corresponds to Values[n].
	Values []any

	Next *leafNode[K] // Next points to next leaf to allow enumeration

	// mutex guards access to this node.
	mutex sync.Mutex
}

// absorbRight moves all of the sibling node's Runts and Values into left
// node, and sets the left's Next field to the value of the sibling's Next
// field.
//
// NOTE: sibling must be locked before calling.
func (left *leafNode[K]) absorbRight(sibling node[K]) {
	right := sibling.(*leafNode[K])

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
func (right *leafNode[K]) adoptFromLeft(sibling node[K]) {
	var keyZeroValue K

	left := sibling.(*leafNode[K])

	// Extend slices of the right node by appending the zero value of the key
	// and pointer data types.
	right.Runts = append(right.Runts, keyZeroValue)
	right.Values = append(right.Values, nil)

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
func (left *leafNode[K]) adoptFromRight(sibling node[K]) {
	right := sibling.(*leafNode[K])

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

func (n *leafNode[K]) count() int { return len(n.Runts) }

func (n *leafNode[K]) deleteKey(minSize int, key K) bool {
	index := searchGreaterThanOrEqualTo(key, n.Runts)
	if index == len(n.Runts) || key != n.Runts[index] {
		return true
	}
	copy(n.Runts[index:], n.Runts[index+1:])
	copy(n.Values[index:], n.Values[index+1:])
	n.Runts = n.Runts[:len(n.Runts)-1]
	n.Values = n.Values[:len(n.Values)-1]
	return len(n.Runts) >= minSize
}

func (n *leafNode[K]) isInternal() bool { return false }

func (n *leafNode[K]) lock() { /* l.mutex.Lock() */ }

// maybeSplit splits the node, giving half of its Values to its new sibling,
// when the node is too full to accept any more Values. When it does return a
// new right sibling, that node is locked.
//
// NOTE: This loop assumes the tree's order is a multiple of 2, which must be
// guarded for at tree instantiation time.
func (n *leafNode[K]) maybeSplit(order int) (node[K], node[K]) {
	if len(n.Runts) < order {
		return n, nil
	}

	newNodeRunts := order >> 1
	sibling := &leafNode[K]{
		Runts:  make([]K, newNodeRunts, order),
		Values: make([]any, newNodeRunts, order),
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

func (n *leafNode[K]) smallest() K {
	if len(n.Runts) == 0 {
		panic("leaf node has no Children")
	}
	return n.Runts[0]
}

func (n *leafNode[K]) unlock() { /* l.mutex.Unlock() */ }

// GenericTree is a B+Tree of elements using Int64 keys.
type GenericTree[K cmp.Ordered] struct {
	root      node[K]
	order     int
	rootMutex sync.Mutex
}

// NewGenericTree returns a newly initialized GenericTree of the specified
// order.
func NewGenericTree[K cmp.Ordered](order int) (*GenericTree[K], error) {
	if err := checkOrder(order); err != nil {
		return nil, err
	}
	return &GenericTree[K]{
		root: &leafNode[K]{
			Runts:  make([]K, 0, order),
			Values: make([]any, 0, order),
		},
		order: order,
	}, nil
}

// Delete removes the key-value pair from the tree.
func (t *GenericTree[K]) Delete(key K) {
	t.rootMutex.Lock()
	defer t.rootMutex.Unlock()

	t.root.lock()
	defer t.root.unlock()

	if t.root.deleteKey(t.order, key) || t.root.count() > 1 {
		// Root is only too small when fewer than 2 Children
		return
	}

	// POST: Root has either 0 or 1 elements.

	// If root is a leaf node, then it is already as small as it can
	// be. Otherwise, when root is an internal node, discard

	// Root might be an internal or a leaf node. If leaf node, the root is
	// already as small as can be.
	if root, ok := t.root.(*internalNode[K]); ok {
		// Root has outlived its usefulness when it has only a single child.
		t.root = root.Children[0]
	}
}

// Insert inserts the key-value pair into the tree, replacing the existing value
// with the new value if the key is already in the tree.
func (t *GenericTree[K]) Insert(key K, value any) {
	t.rootMutex.Lock()
	defer t.rootMutex.Unlock()

	var keyZeroValue K

	n := t.root
	n.lock()

	// Split the root node when required. Regardless of whether the root is an
	// internal or a leaf node, the root shall become an internal node.
	if left, right := n.maybeSplit(t.order); right != nil {
		leftSmallest := left.smallest()
		if key < leftSmallest {
			leftSmallest = key
		}
		rightSmallest := right.smallest()
		t.root = &internalNode[K]{
			Runts:    []K{leftSmallest, rightSmallest},
			Children: []node[K]{left, right},
		}
		// Decide whether we need to descend left or right.
		if key >= rightSmallest {
			n.unlock() // unlock the left, since same node
			n = right
		} else {
			right.unlock()
		}
	}

	for n.isInternal() {
		parent := n.(*internalNode[K])
		index := searchLessThanOrEqualTo(key, parent.Runts)

		child := parent.Children[index]
		child.lock()

		if index == 0 {
			if smallest := child.smallest(); key < smallest {
				// preemptively update smallest value
				parent.Runts[0] = key
			}
		}

		// Split the internal node when required.
		if _, right := child.maybeSplit(t.order); right != nil {
			// Insert sibling to the right of current node.
			parent.Runts = append(parent.Runts, keyZeroValue)
			parent.Children = append(parent.Children, nil)
			copy(parent.Runts[index+2:], parent.Runts[index+1:])
			copy(parent.Children[index+2:], parent.Children[index+1:])
			parent.Children[index+1] = right
			rightSmallest := right.smallest()
			parent.Runts[index+1] = rightSmallest
			// Decide whether we need to descend left or right.
			if key >= rightSmallest {
				child.unlock() // release lock on child
				child = right  // descend to newly created sibling
			} else {
				right.unlock()
			}
		}

		// POST: tail end recursion to intended child
		parent.unlock() // release lock on this node before go to child locked above
		n = child
	}

	ln := n.(*leafNode[K])

	// When the new value will become the first element in a leaf, which is only
	// possible for an empty tree, or when new key comes after final leaf runt,
	// a simple append will suffice.
	if len(ln.Runts) == 0 || key > ln.Runts[len(ln.Runts)-1] {
		ln.Runts = append(ln.Runts, key)
		ln.Values = append(ln.Values, value)
		ln.unlock()
		return
	}

	index := searchGreaterThanOrEqualTo(key, ln.Runts)

	if key == ln.Runts[index] {
		// When the key matches the runt, merely need to update the value.
		ln.Values[index] = value
		ln.unlock()
		return
	}

	// Make room for and insert the new key-value pair into leaf.

	// Append zero Values to make room in arrays
	ln.Runts = append(ln.Runts, keyZeroValue)
	ln.Values = append(ln.Values, nil)
	// Shift elements to the right to make room for new data
	copy(ln.Runts[index+1:], ln.Runts[index:])
	copy(ln.Values[index+1:], ln.Values[index:])
	// Store the new data
	ln.Runts[index] = key
	ln.Values[index] = value
	ln.unlock()
}

// Search returns the value associated with key from the tree.
func (t *GenericTree[K]) Search(key K) (any, bool) {
	t.rootMutex.Lock()
	defer t.rootMutex.Unlock()

	var value any
	var ok bool
	n := t.root
	n.lock()
	for n.isInternal() {
		parent := n.(*internalNode[K])
		child := parent.Children[searchLessThanOrEqualTo(key, parent.Runts)]
		child.lock()
		parent.unlock()
		n = child
	}
	l := n.(*leafNode[K])

	if len(l.Runts) > 0 {
		i := searchGreaterThanOrEqualTo(key, l.Runts)
		if key == l.Runts[i] {
			value = l.Values[i]
			ok = true
		}
	}

	l.unlock()
	return value, ok
}

// Update searches for key and invokes callback with key's associated value,
// waits for callback to return a new value, and stores callback's return value
// as the new value for key. When key is not found, callback will be invoked
// with nil and false to signify the key was not found. After this method
// returns, the key will exist in the tree with the new value returned by the
// callback function.
func (t *GenericTree[K]) Update(key K, callback func(any, bool) any) {
	t.rootMutex.Lock()
	defer t.rootMutex.Unlock()

	var keyZeroValue K

	n := t.root
	n.lock()

	// Split the root node when required. Regardless of whether the root is an
	// internal or a leaf node, the root shall become an internal node.
	if left, right := n.maybeSplit(t.order); right != nil {
		leftSmallest := left.smallest()
		if key < leftSmallest {
			leftSmallest = key
		}
		rightSmallest := right.smallest()
		t.root = &internalNode[K]{
			Runts:    []K{leftSmallest, rightSmallest},
			Children: []node[K]{left, right}, // 511
		}
		// Decide whether we need to descend left or right.
		if key >= rightSmallest {
			n.unlock() // unlock the left, since same node
			n = right
		} else {
			right.unlock()
		}
	}

	for n.isInternal() {
		parent := n.(*internalNode[K])
		index := searchLessThanOrEqualTo(key, parent.Runts)

		child := parent.Children[index] // 525
		child.lock()

		if index == 0 {
			if smallest := child.smallest(); key < smallest {
				// preemptively update smallest value
				parent.Runts[0] = key
			}
		}

		// Split the internal node when required.
		if _, right := child.maybeSplit(t.order); right != nil {
			// Insert sibling to the right of current node.
			parent.Runts = append(parent.Runts, keyZeroValue)
			parent.Children = append(parent.Children, nil)
			copy(parent.Runts[index+2:], parent.Runts[index+1:])
			copy(parent.Children[index+2:], parent.Children[index+1:])
			parent.Children[index+1] = right
			rightSmallest := right.smallest()
			parent.Runts[index+1] = rightSmallest
			// Decide whether we need to descend left or right.
			if key >= rightSmallest {
				child.unlock() // release lock on child
				child = right  // descend to newly created sibling
			} else {
				right.unlock()
			}
		}

		// POST: tail end recursion to intended child
		parent.unlock() // release lock on this node before go to child locked above
		n = child
	}

	ln := n.(*leafNode[K])

	// When the new value will become the first element in a leaf, which is only
	// possible for an empty tree, or when new key comes after final leaf runt,
	// a simple append will suffice.
	if len(ln.Runts) == 0 || key > ln.Runts[len(ln.Runts)-1] {
		value := callback(nil, false)
		ln.Runts = append(ln.Runts, key)
		ln.Values = append(ln.Values, value)
		ln.unlock()
		return
	}

	index := searchGreaterThanOrEqualTo(key, ln.Runts)

	if key == ln.Runts[index] {
		// When the key matches the runt, merely need to update the value.
		ln.Values[index] = callback(ln.Values[index], true)
		ln.unlock()
		return
	}

	// Make room for and insert the new key-value pair into leaf.

	// Append zero Values to make room in arrays
	ln.Runts = append(ln.Runts, keyZeroValue)
	ln.Values = append(ln.Values, nil)
	// Shift elements to the right to make room for new data
	copy(ln.Runts[index+1:], ln.Runts[index:])
	copy(ln.Values[index+1:], ln.Values[index:])
	// Store the new data
	ln.Runts[index] = key
	ln.Values[index] = callback(nil, false)
	ln.unlock()
}

// NewScanner returns a cursor that iteratively returns key-value pairs from
// the tree in ascending order starting at the specified key, or, if key is
// not found, the next key; and ending after all successive pairs have been
// returned. To enumerate all values in a tree, use NewScannerAll, which is
// faster than invoking this method with the minimum key value.
//
// NOTE: This function exits still holding the lock on one of the tree's leaf
// nodes, which may block other operations on the tree that require
// modification of the locked node. The leaf node is only unlocked after
// closing the Cursor.
func (t *GenericTree[K]) NewScanner(key K) *GenericCursor[K] {
	n := t.root
	n.lock()

	for {
		switch tv := n.(type) {
		case *internalNode[K]:
			child := tv.Children[searchLessThanOrEqualTo(key, tv.Runts)]
			child.lock()
			tv.unlock()
			n = child
		case *leafNode[K]:
			return newGenericCursor(tv, searchGreaterThanOrEqualTo(key, tv.Runts))
		default:
			panic(fmt.Errorf("GOT: %#v; WANT: node", n))
		}
	}
}

// NewScannerAll returns a cursor that iteratively returns all key-value pairs
// from the tree in ascending order. To start scanning at a particular key
// value, use NewScanner. This method is faster than invoking NewScanner with
// the minimum key value.
//
// NOTE: This function exits still holding the lock on one of the tree's leaf
// nodes, which may block other operations on the tree that require
// modification of the locked node. The leaf node is only unlocked after
// closing the Cursor.
func (t *GenericTree[K]) NewScannerAll() *GenericCursor[K] {
	n := t.root
	n.lock()

	for {
		switch tv := n.(type) {
		case *internalNode[K]:
			child := tv.Children[0] // go to the left most child
			child.lock()
			tv.unlock()
			n = child
		case *leafNode[K]:
			return newGenericCursor(tv, 0) // start at left most value
		default:
			panic(fmt.Errorf("GOT: %#v; WANT: node", n))
		}
	}
}

// GenericCursor is used to enumerate key-value pairs from the tree in
// ascending order.
type GenericCursor[K cmp.Ordered] struct {
	leaf  *leafNode[K]
	index int
}

func newGenericCursor[K cmp.Ordered](leaf *leafNode[K], index int) *GenericCursor[K] {
	// Initialize cursor with index one smaller than requested, so initial
	// scan lines up the cursor to reference the desired key-value pair. The
	// fact that this needs to use the index before the starting index is the
	// only reason why this method exists, as the logic is invoked from
	// several places.
	return &GenericCursor[K]{leaf: leaf, index: index - 1}
}

// Close releases the lock on the leaf node under the cursor. This method is
// provided to signal no further intention of scanning the remainder key-value
// pairs in the tree. It is necessary to invoke this method in order to
// release the lock the cursor holds on one of the leaf nodes in the tree.
func (c *GenericCursor[K]) Close() error {
	if c.leaf == nil {
		return errors.New("cannot Close a closed Scanner")
	}
	c.leaf.unlock()
	c.leaf = nil
	return nil
}

// Pair returns the key-value pair referenced by the cursor. This method will
// panic when invoked before invoking the Scan method at least once.
func (c *GenericCursor[K]) Pair() (K, any) {
	return c.leaf.Runts[c.index], c.leaf.Values[c.index]
}

// Scan advances the cursor to reference the next key-value pair in the tree
// in ascending order, and returns true when there is at least one more
// key-value pair to be observed with the Pair method. If the final key-value
// pair has already been observed, this unlocks the final leaf in the tree and
// returns false. This method must be invoked at least once before invoking
// the Pair method.
func (c *GenericCursor[K]) Scan() bool {
	if c.index++; c.index == len(c.leaf.Runts) {
		n := c.leaf.Next
		if n == nil {
			return false
		}
		n.lock()
		c.leaf.unlock()
		c.leaf = n
		c.index = 0
	}
	return true
}
