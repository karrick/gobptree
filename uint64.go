package gobptree

import (
	"fmt"
	"sync"
)

// uint64SearchGreaterThanOrEqualTo returns the index of the first value from
// values that is greater than or equal to key.  search for index of runt that
// is greater than or equal to key.
func uint64SearchGreaterThanOrEqualTo(key uint64, values []uint64) int {
	// search for index of runt that is greater than or equal to key
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
	if key > v {
		if lo = m + 1; lo < hi {
			goto loop
		}
		return lo
	}
	return m
}

// uint64SearchLessThanOrEqualTo returns the index of the first value from
// values that is less than or equal to key.
func uint64SearchLessThanOrEqualTo(key uint64, values []uint64) int {
	index := uint64SearchGreaterThanOrEqualTo(key, values)
	// convert result to less than or equal to
	if index == len(values) || key < values[index] {
		if index > 0 {
			return index - 1
		}
	}
	return index
}

// uint64Node represents either an internal or a leaf node for a
// Uint64Tree using Uint64 keys.
type uint64Node interface {
	absorbRight(uint64Node)
	adoptFromLeft(uint64Node)
	adoptFromRight(uint64Node)
	count() int
	deleteKey(int, uint64) bool
	isInternal() bool
	lock()
	maybeSplit(order int) (uint64Node, uint64Node)
	smallest() uint64
	unlock()
}

// uint64InternalNode represents an internal node for a Uint64Tree with
// Uint64 keys.
type uint64InternalNode struct {
	runts    []uint64
	children []uint64Node
	mutex    sync.Mutex
}

func (left *uint64InternalNode) absorbRight(sibling uint64Node) {
	right := sibling.(*uint64InternalNode)
	left.runts = append(left.runts, right.runts...)
	left.children = append(left.children, right.children...)

	// Perhaps following are not strictly needed, but de-allocate slices.
	right.runts = nil
	right.children = nil
}

func (right *uint64InternalNode) adoptFromLeft(sibling uint64Node) {
	left := sibling.(*uint64InternalNode)

	right.runts = append(right.runts, 0)
	right.children = append(right.children, nil)
	copy(right.runts[1:], right.runts[0:])
	copy(right.children[1:], right.children[0:])

	index := len(left.runts) - 1
	right.runts[0] = left.runts[index]
	right.children[0] = left.children[index]

	left.runts = left.runts[:index]
	left.children = left.children[:index]
}

func (left *uint64InternalNode) adoptFromRight(sibling uint64Node) {
	right := sibling.(*uint64InternalNode)

	left.runts = append(left.runts, right.runts[0])
	left.children = append(left.children, right.children[0])

	copy(right.runts[0:], right.runts[1:])
	copy(right.children[0:], right.children[1:])

	index := len(right.runts) - 1
	right.runts = right.runts[:index]
	right.children = right.children[:index]
}

func (i *uint64InternalNode) count() int { return len(i.runts) }

func (i *uint64InternalNode) deleteKey(minSize int, key uint64) bool {
	index := uint64SearchLessThanOrEqualTo(key, i.runts)
	child := i.children[index]
	child.lock()
	defer child.unlock()

	if !child.deleteKey(minSize, key) {
		return false
	}
	// POST: child is too small

	var leftSibling, rightSibling uint64Node
	var leftCount, rightCount int

	if index < len(i.runts)-1 {
		// try right sibling first to encourage left leaning trees
		rightSibling = i.children[index+1]
		rightSibling.lock()
		defer rightSibling.unlock()
		if rightCount = rightSibling.count(); rightCount > minSize {
			child.adoptFromRight(rightSibling)
			i.runts[index+1] = rightSibling.smallest()
			return false
		}
	}
	// POST: If right, it is exactly minimum size.

	if index > 0 {
		// try left sibling
		leftSibling = i.children[index-1]
		leftSibling.lock()
		defer leftSibling.unlock()
		if leftCount = leftSibling.count(); leftCount > minSize {
			child.adoptFromLeft(leftSibling)
			return false
		}
	}
	// POST: If left, it is exactly minimum size.

	// POST: Could not adopt a single node from either side, because either
	// child is left or right edge and has no siblings to its left or right, or
	// the siblings it does have each only has the minimum number of children.

	if leftCount > 0 {
		leftSibling.absorbRight(child)
		copy(i.runts[index:], i.runts[index+1:])
		i.runts = i.runts[:len(i.runts)-1]
		copy(i.children[index:], i.children[index+1:])
		i.children = i.children[:len(i.children)-1]
		// This node has one fewer children.
		return len(i.runts) < minSize
	}

	// When right has no children, then should not be in a position where left
	// also has no children.
	if rightCount == 0 {
		panic("both left and right siblings have no children")
	}

	child.absorbRight(rightSibling)
	copy(i.runts[index+1:], i.runts[index+2:])
	i.runts = i.runts[:len(i.runts)-1]
	copy(i.children[index+1:], i.children[index+2:])
	i.children = i.children[:len(i.children)-1]
	// This node has one fewer children.
	return len(i.runts) < minSize
}

func (i *uint64InternalNode) isInternal() bool { return true }

func (i *uint64InternalNode) lock() { i.mutex.Lock() }

// maybeSplit splits the node, giving half of its values to its new sibling,
// when the node is too full to accept any more values.
//
// NOTE: This loop assumes the tree's order is a multiple of 2, which must be
// guarded for at tree instantiation time.
func (i *uint64InternalNode) maybeSplit(order int) (uint64Node, uint64Node) {
	if len(i.runts) < order {
		return i, nil
	}
	newNodeRunts := order >> 1
	sibling := &uint64InternalNode{
		runts:    make([]uint64, newNodeRunts, order),
		children: make([]uint64Node, newNodeRunts, order),
	}
	// Right half of this node moves to sibling.
	for j := 0; j < newNodeRunts; j++ {
		sibling.runts[j] = i.runts[newNodeRunts+j]
		sibling.children[j] = i.children[newNodeRunts+j]
	}
	// Clear the runts and pointers from the original node.
	i.runts = i.runts[:newNodeRunts]
	i.children = i.children[:newNodeRunts]
	return i, sibling
}

func (i *uint64InternalNode) smallest() uint64 {
	if len(i.runts) == 0 {
		panic("internal node has no children")
	}
	return i.runts[0]
}

func (i *uint64InternalNode) unlock() { i.mutex.Unlock() }

// uint64LeafNode represents a leaf node for a Uint64Tree using
// Uint64 keys.
type uint64LeafNode struct {
	runts  []uint64
	values []interface{}
	next   *uint64LeafNode // points to next leaf to allow enumeration
	mutex  sync.Mutex
}

func (left *uint64LeafNode) absorbRight(sibling uint64Node) {
	right := sibling.(*uint64LeafNode)
	if left.next != right {
		// Superfluous check
		panic("cannot merge leaf with sibling other than next sibling")
	}
	left.runts = append(left.runts, right.runts...)
	left.values = append(left.values, right.values...)
	left.next = right.next

	// Perhaps following are not strictly needed, but de-allocate slices and
	// release pointers.
	right.runts = nil
	right.values = nil
	right.next = nil
}

func (right *uint64LeafNode) adoptFromLeft(sibling uint64Node) {
	left := sibling.(*uint64LeafNode)

	right.runts = append(right.runts, 0)
	right.values = append(right.values, nil)
	copy(right.runts[1:], right.runts[0:])
	copy(right.values[1:], right.values[0:])

	index := len(left.runts) - 1
	right.runts[0] = left.runts[index]
	right.values[0] = left.values[index]

	left.runts = left.runts[:index]
	left.values = left.values[:index]
}

func (left *uint64LeafNode) adoptFromRight(sibling uint64Node) {
	right := sibling.(*uint64LeafNode)
	left.runts = append(left.runts, right.runts[0])
	left.values = append(left.values, right.values[0])
	copy(right.runts[0:], right.runts[1:])
	copy(right.values[0:], right.values[1:])
	index := len(right.runts) - 1
	right.runts = right.runts[:index]
	right.values = right.values[:index]
}

func (l *uint64LeafNode) count() int { return len(l.runts) }

func (l *uint64LeafNode) deleteKey(minSize int, key uint64) bool {
	index := uint64SearchGreaterThanOrEqualTo(key, l.runts)
	if index == len(l.runts) || key != l.runts[index] {
		return false
	}
	copy(l.runts[index:], l.runts[index+1:])
	copy(l.values[index:], l.values[index+1:])
	l.runts = l.runts[:len(l.runts)-1]
	l.values = l.values[:len(l.values)-1]
	return len(l.runts) < minSize
}

func (l *uint64LeafNode) isInternal() bool { return false }

func (l *uint64LeafNode) lock() { l.mutex.Lock() }

// maybeSplit splits the node, giving half of its values to its new sibling,
// when the node is too full to accept any more values.
//
// NOTE: This loop assumes the tree's order is a multiple of 2, which must be
// guarded for at tree instantiation time.
func (l *uint64LeafNode) maybeSplit(order int) (uint64Node, uint64Node) {
	if len(l.runts) < order {
		return l, nil
	}
	newNodeRunts := order >> 1
	sibling := &uint64LeafNode{
		runts:  make([]uint64, newNodeRunts, order),
		values: make([]interface{}, newNodeRunts, order),
		next:   l.next,
	}
	// Right half of this node moves to sibling.
	for j := 0; j < newNodeRunts; j++ {
		sibling.runts[j] = l.runts[newNodeRunts+j]
		sibling.values[j] = l.values[newNodeRunts+j]
	}
	// Clear the runts and pointers from the original node.
	l.runts = l.runts[:newNodeRunts]
	l.values = l.values[:newNodeRunts]
	l.next = sibling
	return l, sibling
}

func (l *uint64LeafNode) smallest() uint64 {
	if len(l.runts) == 0 {
		panic("leaf node has no children")
	}
	return l.runts[0]
}

func (l *uint64LeafNode) unlock() { l.mutex.Unlock() }

// Uint64Tree is a B+Tree of elements using Uint64 keys.
type Uint64Tree struct {
	root  uint64Node
	order int
}

// NewUint64Tree returns a newly initialized Uint64Tree of the specified
// order.
func NewUint64Tree(order int) (*Uint64Tree, error) {
	if order <= 0 || order%2 == 1 {
		return nil, fmt.Errorf("cannot create tree when order is not a multiple of 2: %d", order)
	}
	return &Uint64Tree{
		root: &uint64LeafNode{
			runts:  make([]uint64, 0, order),
			values: make([]interface{}, 0, order),
		},
		order: order,
	}, nil
}

// Delete removes the key-value pair from the tree.
func (t *Uint64Tree) Delete(key uint64) {
	t.root.lock()
	defer t.root.unlock()

	if !t.root.deleteKey(t.order, key) || t.root.count() > 1 {
		// Root is only too small when fewer than 2 children
		return
	}
	// Root might be an internal or a leaf node. If leaf node, the root is
	// already as small as can be.
	if root, ok := t.root.(*uint64InternalNode); ok {
		// Root has outlived its usefulness when it has only a single child.
		t.root = root.children[0]
	}
}

// Insert inserts the key-value pair into the tree, replacing the existing value
// with the new value if the key is already in the tree.
func (t *Uint64Tree) Insert(key uint64, value interface{}) {
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
		t.root = &uint64InternalNode{
			runts:    []uint64{leftSmallest, rightSmallest},
			children: []uint64Node{left, right},
		}
		// Decide whether we need to descend left or right.
		if key >= rightSmallest {
			right.lock()
			n.unlock() // unlock the left, since same node
			n = right
		}
	}

	for n.isInternal() {
		parent := n.(*uint64InternalNode)
		index := uint64SearchLessThanOrEqualTo(key, parent.runts)

		child := parent.children[index]
		child.lock()

		if index == 0 {
			if smallest := child.smallest(); key < smallest {
				// preemptively update smallest value
				parent.runts[0] = key
			}
		}

		// Split the internal node when required.
		if _, right := child.maybeSplit(t.order); right != nil {
			// Insert sibling to the right of current node.
			parent.runts = append(parent.runts, 0)
			parent.children = append(parent.children, nil)
			copy(parent.runts[index+2:], parent.runts[index+1:])
			copy(parent.children[index+2:], parent.children[index+1:])
			parent.children[index+1] = right
			rightSmallest := right.smallest()
			parent.runts[index+1] = rightSmallest
			// Decide whether we need to descend left or right.
			if key >= rightSmallest {
				right.lock()   // grab lock on its new sibling
				child.unlock() // release lock on child
				child = right  // descend to newly created sibling
			}
		}

		// POST: tail end recursion to intended child
		parent.unlock() // release lock on this node before go to child locked above
		n = child
	}

	ln := n.(*uint64LeafNode)

	// When the new value will become the first element in a leaf, which is only
	// possible for an empty tree, or when new key comes after final leaf runt,
	// a simple append will suffice.
	if len(ln.runts) == 0 || key > ln.runts[len(ln.runts)-1] {
		ln.runts = append(ln.runts, key)
		ln.values = append(ln.values, value)
		ln.unlock()
		return
	}

	index := uint64SearchGreaterThanOrEqualTo(key, ln.runts)

	if key == ln.runts[index] {
		// When the key matches the runt, merely need to update the value.
		ln.values[index] = value
		ln.unlock()
		return
	}

	// Make room for and insert the new key-value pair into leaf.

	// Append zero values to make room in arrays
	ln.runts = append(ln.runts, 0)
	ln.values = append(ln.values, nil)
	// Shift elements to the right to make room for new data
	copy(ln.runts[index+1:], ln.runts[index:])
	copy(ln.values[index+1:], ln.values[index:])
	// Store the new data
	ln.runts[index] = key
	ln.values[index] = value
	ln.unlock()
}

// Search returns the value associated with key from the tree.
func (t *Uint64Tree) Search(key uint64) (interface{}, bool) {
	var value interface{}
	var ok bool
	n := t.root
	n.lock()
	for n.isInternal() {
		parent := n.(*uint64InternalNode)
		child := parent.children[uint64SearchLessThanOrEqualTo(key, parent.runts)]
		child.lock()
		parent.unlock()
		n = child
	}
	l := n.(*uint64LeafNode)

	if len(l.runts) > 0 {
		i := uint64SearchGreaterThanOrEqualTo(key, l.runts)
		if key == l.runts[i] {
			value = l.values[i]
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
func (t *Uint64Tree) Update(key uint64, callback func(interface{}, bool) interface{}) {
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
		t.root = &uint64InternalNode{
			runts:    []uint64{leftSmallest, rightSmallest},
			children: []uint64Node{left, right},
		}
		// Decide whether we need to descend left or right.
		if key >= rightSmallest {
			right.lock()
			n.unlock() // unlock the left, since same node
			n = right
		}
	}

	for n.isInternal() {
		parent := n.(*uint64InternalNode)
		index := uint64SearchLessThanOrEqualTo(key, parent.runts)

		child := parent.children[index]
		child.lock()

		if index == 0 {
			if smallest := child.smallest(); key < smallest {
				// preemptively update smallest value
				parent.runts[0] = key
			}
		}

		// Split the internal node when required.
		if _, right := child.maybeSplit(t.order); right != nil {
			// Insert sibling to the right of current node.
			parent.runts = append(parent.runts, 0)
			parent.children = append(parent.children, nil)
			copy(parent.runts[index+2:], parent.runts[index+1:])
			copy(parent.children[index+2:], parent.children[index+1:])
			parent.children[index+1] = right
			rightSmallest := right.smallest()
			parent.runts[index+1] = rightSmallest
			// Decide whether we need to descend left or right.
			if key >= rightSmallest {
				right.lock()   // grab lock on its new sibling
				child.unlock() // release lock on child
				child = right  // descend to newly created sibling
			}
		}

		// POST: tail end recursion to intended child
		parent.unlock() // release lock on this node before go to child locked above
		n = child
	}

	ln := n.(*uint64LeafNode)

	// When the new value will become the first element in a leaf, which is only
	// possible for an empty tree, or when new key comes after final leaf runt,
	// a simple append will suffice.
	if len(ln.runts) == 0 || key > ln.runts[len(ln.runts)-1] {
		value := callback(nil, false)
		ln.runts = append(ln.runts, key)
		ln.values = append(ln.values, value)
		ln.unlock()
		return
	}

	index := uint64SearchGreaterThanOrEqualTo(key, ln.runts)

	if key == ln.runts[index] {
		// When the key matches the runt, merely need to update the value.
		ln.values[index] = callback(ln.values[index], true)
		ln.unlock()
		return
	}

	// Make room for and insert the new key-value pair into leaf.

	// Append zero values to make room in arrays
	ln.runts = append(ln.runts, 0)
	ln.values = append(ln.values, nil)
	// Shift elements to the right to make room for new data
	copy(ln.runts[index+1:], ln.runts[index:])
	copy(ln.values[index+1:], ln.values[index:])
	// Store the new data
	ln.runts[index] = key
	ln.values[index] = callback(nil, false)
	ln.unlock()
}

// NewScanner returns a cursor that iteratively returns key-value pairs from the
// tree in ascending order starting at key, or if key is not found the next key,
// and ending after all successive pairs have been returned. To enumerate all
// values in a Uint64Tree, invoke with key set to 0.
//
// NOTE: This function exists still holding the lock on one of the tree's leaf
// nodes, which may block other operations on the tree that require modification
// of the locked node. The leaf node is only unlocked either by closing the
// Cursor, or after all key-value pairs have been visited using Scan.
func (t *Uint64Tree) NewScanner(key uint64) *Uint64Cursor {
	n := t.root
	n.lock()
	for n.isInternal() {
		parent := n.(*uint64InternalNode)
		child := parent.children[uint64SearchLessThanOrEqualTo(key, parent.runts)]
		child.lock()
		parent.unlock()
		n = child
	}
	ln := n.(*uint64LeafNode)
	return newUint64Cursor(ln, uint64SearchGreaterThanOrEqualTo(key, ln.runts))
}

// Uint64Cursor is used to enumerate key-value pairs from the tree in
// ascending order.
type Uint64Cursor struct {
	l *uint64LeafNode
	i int
}

func newUint64Cursor(l *uint64LeafNode, i int) *Uint64Cursor {
	// Initialize cursor with index one smaller than requested, so initial scan
	// lines up the cursor to reference the desired key-value pair.
	return &Uint64Cursor{l: l, i: i - 1}
}

// Close releases the lock on the leaf node under the cursor. This method is
// provided to signal no further intention of scanning the remainder key-value
// pairs in the tree. It is not necessary to call Close if Scan is called
// repeatedly until Scan returns false.
func (c *Uint64Cursor) Close() error {
	if c.l != nil {
		c.l.unlock()
		c.l = nil
	}
	return nil
}

// Pair returns the key-value pair referenced by the cursor.
func (c *Uint64Cursor) Pair() (uint64, interface{}) {
	return c.l.runts[c.i], c.l.values[c.i]
}

// Scan advances the cursor to reference the next key-value pair in the tree in
// ascending order, and returns true when there is at least one more key-value
// pair to be observed with the Pair method. If the final key-value pair has
// already been observed, this unlocks the final leaf in the tree and returns
// false.
func (c *Uint64Cursor) Scan() bool {
	if c.i++; c.i == len(c.l.runts) {
		if c.l.next == nil {
			c.l.unlock()
			c.l = nil
			return false
		}
		n := c.l.next
		n.lock()
		c.l.unlock()
		c.l = n
		c.i = 0
	}
	return true
}
