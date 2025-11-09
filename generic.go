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

// searchGreaterThanOrEqualTo returns the index of the first value from values
// that is greater than or equal to key.  search for index of runt that is
// greater than or equal to key.
func searchGreaterThanOrEqualTo[K cmp.Ordered](key K, values []K) int {
	var lo int

	hi := len(values)
	if hi <= 1 {
		return 0
	}
	hi--

loop:
	m := (lo + hi) >> 1
	v := values[m]
	if key < v {
		if hi = m; lo < hi {
			goto loop
		}
		return lo
	}
	if v < key {
		if lo = m + 1; lo < hi {
			goto loop
		}
		return lo
	}
	return m
}

// searchLessThanOrEqualTo returns the index of the first value from values
// that is less than or equal to key.
func searchLessThanOrEqualTo[K cmp.Ordered](key K, values []K) int {
	index := searchGreaterThanOrEqualTo(key, values)
	// convert result to less than or equal to
	if index == len(values) || key < values[index] {
		if index > 0 {
			return index - 1
		}
	}
	return index
}

// node represents either an internal or a leaf node for a GenericTree with
// keys of a cmp.Ordered type, and values of any type.
type node[K cmp.Ordered, V any] interface {
	absorbRight(node[K, V])
	adoptFromLeft(node[K, V])
	adoptFromRight(node[K, V])
	count() int
	deleteKey(int, K) bool
	isInternal() bool
	lock()
	maybeSplit(order int) (node[K, V], node[K, V])
	smallest() K
	unlock()
}

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
	mutex sync.Mutex
}

// absorbRight moves all of the sibling node's Runts and Children into left
// node.
//
// NOTE: sibling must be locked before calling.
func (left *internalNode[K, V]) absorbRight(sibling node[K, V]) {
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
}

func (n *internalNode[K, V]) count() int { return len(n.Runts) }

// deleteKey removes key and its value from the node, returning true when the
// node has at least minSize elements after the deletion, and returning false
// when the node has fewer elements than minSize.
func (n *internalNode[K, V]) deleteKey(minSize int, key K) bool {
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

	var leftSibling, rightSibling node[K, V]
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
	// child is left or right edge and has no siblings to its left or right,
	// or the siblings it does have each only has the minimum number of
	// Children.

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

func (n *internalNode[K, V]) isInternal() bool { return true }

func (n *internalNode[K, V]) lock() { /* i.mutex.Lock() */ }

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

func (n *internalNode[K, V]) unlock() { /* i.mutex.Unlock() */ }

// internalNode represents a leaf node for a GenericTree with keys of
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
	mutex sync.Mutex
}

// absorbRight moves all of the sibling node's Runts and Values into left
// node, and sets the left's Next field to the value of the sibling's Next
// field.
//
// NOTE: sibling must be locked before calling.
func (left *leafNode[K, V]) absorbRight(sibling node[K, V]) {
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
	// and pointer data types.
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

func (n *leafNode[K, V]) isInternal() bool { return false }

func (n *leafNode[K, V]) lock() { /* l.mutex.Lock() */ }

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

func (n *leafNode[K, V]) unlock() { /* l.mutex.Unlock() */ }

// GenericTree is a B+Tree of elements using key whose type satisfy the
// cmp.Ordered constraint.
type GenericTree[K cmp.Ordered, V any] struct {
	root      node[K, V]
	order     int
	rootMutex sync.Mutex
}

// NewGenericTree returns a newly initialized GenericTree of the specified
// order.
func NewGenericTree[K cmp.Ordered, V any](order int) (*GenericTree[K, V], error) {
	if err := checkOrder(order); err != nil {
		return nil, err
	}
	return &GenericTree[K, V]{
		root: &leafNode[K, V]{
			Runts:  make([]K, 0, order),
			Values: make([]V, 0, order),
		},
		order: order,
	}, nil
}

// Delete removes the key-value pair from the tree.
func (t *GenericTree[K, V]) Delete(key K) {
	t.rootMutex.Lock()
	defer t.rootMutex.Unlock()

	t.root.lock()
	defer t.root.unlock()

	// NOTE: Before invoking count method, we know we can return without
	// combining nodes when deleteKey returns true. If deleteKey returns
	// false, then root node no longer has the minimum number of items.
	//
	// ???: Should this set the minimum size to half of the tree order?
	minSize := t.order
	if t.root.deleteKey(minSize, key) || t.root.count() > 1 {
		return // root node is large enough
	}

	// POST: Root has either 0 or 1 elements.

	// If root is a leaf node, then it is already as small as it can
	// be. Otherwise, when root is an internal node, discard and replace by
	// its leaf node.

	if root, ok := t.root.(*internalNode[K, V]); ok {
		// Root has outlived its usefulness when it has only a single child.
		t.root = root.Children[0]
	}
}

// Insert inserts the key-value pair into the tree, replacing the existing
// value with the new value if the key is already in the tree.
func (t *GenericTree[K, V]) Insert(key K, value V) {
	// NOTE: This has the Same logic as Update, and rather than duplicate that
	// logic, merely invoke Update method with a callback that ignores its
	// arguments and returns the value to be stored.
	t.Update(key, func(_ V, _ bool) V { return value })
}

// Search returns the value associated with key from the tree.
func (t *GenericTree[K, V]) Search(key K) (V, bool) {
	var value V
	var ok bool

	t.rootMutex.Lock()
	defer t.rootMutex.Unlock()

	n := t.root
	n.lock()

	for n.isInternal() {
		parent := n.(*internalNode[K, V])
		child := parent.Children[searchLessThanOrEqualTo(key, parent.Runts)]
		child.lock()
		parent.unlock()
		n = child
	}

	leaf := n.(*leafNode[K, V])

	if len(leaf.Runts) > 0 {
		i := searchGreaterThanOrEqualTo(key, leaf.Runts)
		if key == leaf.Runts[i] {
			value = leaf.Values[i]
			ok = true
		}
	}

	leaf.unlock()
	return value, ok
}

// Update searches for key and invokes callback with key's associated value,
// waits for callback to return a new value, and stores callback's return
// value as the new value for key. When key is not found, callback will be
// invoked with nil and false to signify the key was not found. After this
// method returns, the key will exist in the tree with the new value returned
// by the callback function.
func (t *GenericTree[K, V]) Update(key K, callback func(V, bool) V) {
	var keyZeroValue K
	var valueZeroValue V

	t.rootMutex.Lock()
	defer t.rootMutex.Unlock()

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
		t.root = &internalNode[K, V]{
			Runts:    []K{leftSmallest, rightSmallest},
			Children: []node[K, V]{left, right},
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
		internal := n.(*internalNode[K, V])
		index := searchLessThanOrEqualTo(key, internal.Runts)

		child := internal.Children[index]
		child.lock()

		if index == 0 {
			if smallest := child.smallest(); key < smallest {
				// preemptively update smallest value
				internal.Runts[0] = key
			}
		}

		// Split the child node when required.
		if _, right := child.maybeSplit(t.order); right != nil {
			// Insert sibling to the right of current node.
			internal.Runts = append(internal.Runts, keyZeroValue)
			internal.Children = append(internal.Children, nil)
			// Shift runts and children to the right by one element.
			copy(internal.Runts[index+2:], internal.Runts[index+1:])
			copy(internal.Children[index+2:], internal.Children[index+1:])
			// Insert new right element into internal node.
			internal.Children[index+1] = right
			rightSmallest := right.smallest()
			internal.Runts[index+1] = rightSmallest
			// Decide whether we need to descend left or right.
			if key < rightSmallest {
				// will not add this pair to the right node
				right.unlock()
			} else {
				// will add this pair to the right node
				child.unlock() // release lock on child
				child = right  // descend to newly created sibling
			}
		}

		// POST: tail end recursion to intended child
		internal.unlock() // release lock on this node before go to child locked above
		n = child
	}

	leaf := n.(*leafNode[K, V])

	// When the new value will become the first element in a leaf, which is
	// only possible for an empty tree, or when new key comes after final leaf
	// runt, a simple append will suffice.
	if len(leaf.Runts) == 0 || key > leaf.Runts[len(leaf.Runts)-1] {
		value := callback(valueZeroValue, false)
		leaf.Runts = append(leaf.Runts, key)
		leaf.Values = append(leaf.Values, value)
		leaf.unlock()
		return
	}

	index := searchGreaterThanOrEqualTo(key, leaf.Runts)

	if key == leaf.Runts[index] {
		// When the key matches the runt, merely need to update the value.
		leaf.Values[index] = callback(leaf.Values[index], true)
		leaf.unlock()
		return
	}

	// Make room for and insert the new key-value pair into leaf.

	// Append zero values to make room in arrays
	leaf.Runts = append(leaf.Runts, keyZeroValue)
	leaf.Values = append(leaf.Values, valueZeroValue)
	// Shift elements to the right to make room for new data
	copy(leaf.Runts[index+1:], leaf.Runts[index:])
	copy(leaf.Values[index+1:], leaf.Values[index:])
	// Store the new data
	leaf.Runts[index] = key
	leaf.Values[index] = callback(valueZeroValue, false)
	leaf.unlock()
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
func (t *GenericTree[K, V]) NewScanner(key K) *GenericCursor[K, V] {
	n := t.root
	n.lock()

	for {
		switch tv := n.(type) {
		case *internalNode[K, V]:
			child := tv.Children[searchLessThanOrEqualTo(key, tv.Runts)]
			child.lock()
			tv.unlock()
			n = child
		case *leafNode[K, V]:
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
func (t *GenericTree[K, V]) NewScannerAll() *GenericCursor[K, V] {
	n := t.root
	n.lock()

	for {
		switch tv := n.(type) {
		case *internalNode[K, V]:
			child := tv.Children[0] // go to the left most child
			child.lock()
			tv.unlock()
			n = child
		case *leafNode[K, V]:
			return newGenericCursor(tv, 0) // start at left most value
		default:
			panic(fmt.Errorf("GOT: %#v; WANT: node", n))
		}
	}
}

// GenericCursor is used to enumerate key-value pairs from the tree in
// ascending order.
type GenericCursor[K cmp.Ordered, V any] struct {
	leaf  *leafNode[K, V]
	index int
}

func newGenericCursor[K cmp.Ordered, V any](leaf *leafNode[K, V], index int) *GenericCursor[K, V] {
	// Initialize cursor with index one smaller than requested, so initial
	// scan lines up the cursor to reference the desired key-value pair. The
	// fact that this needs to use the index before the starting index is the
	// only reason why this method exists, as the logic is invoked from
	// several places.
	return &GenericCursor[K, V]{leaf: leaf, index: index - 1}
}

// Close releases the lock on the leaf node under the cursor. This method is
// provided to signal no further intention of scanning the remainder key-value
// pairs in the tree. It is necessary to invoke this method in order to
// release the lock the cursor holds on one of the leaf nodes in the tree.
func (c *GenericCursor[K, V]) Close() error {
	if c.leaf == nil {
		return errors.New("cannot Close a closed Scanner")
	}
	c.leaf.unlock()
	c.leaf = nil
	return nil
}

// Pair returns the key-value pair referenced by the cursor. This method will
// panic when invoked before invoking the Scan method at least once.
func (c *GenericCursor[K, V]) Pair() (K, V) {
	return c.leaf.Runts[c.index], c.leaf.Values[c.index]
}

// Scan advances the cursor to reference the next key-value pair in the tree
// in ascending order, and returns true when there is at least one more
// key-value pair to be observed with the Pair method. If the final key-value
// pair has already been observed, this unlocks the final leaf in the tree and
// returns false. This method must be invoked at least once before invoking
// the Pair method.
func (c *GenericCursor[K, V]) Scan() bool {
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
