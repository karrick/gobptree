package gobptree

import (
	"fmt"
	"sync"
)

// stringSearchGreaterThanOrEqualTo returns the index of the first value
// from values that is greater than or equal to key.
func stringSearchGreaterThanOrEqualTo(key string, values []string) int {
	// search for index of runt that is greater than or equal to key
	var low int
	var high = len(values) - 1
	for low < high {
		index := (low + high) >> 1
		value := values[index]
		if key < value {
			high = index
		} else if key > value {
			low = index + 1
		} else {
			return index
		}
	}
	return low
}

// stringSearchLessThanOrEqualTo returns the index of the first value from
// values that is less than or equal to key.
func stringSearchLessThanOrEqualTo(key string, values []string) int {
	index := stringSearchGreaterThanOrEqualTo(key, values)
	// convert result to less than or equal to
	if index == len(values) || key < values[index] {
		if index > 0 {
			return index - 1
		}
	}
	return index
}

// stringNode represents either an internal or a leaf node for a
// StringTree using String keys.
type stringNode interface {
	absorbRight(stringNode)
	adoptFromLeft(stringNode)
	adoptFromRight(stringNode)
	count() int
	deleteKey(int, string) bool
	isInternal() bool
	lock()
	maybeSplit(order int) (stringNode, stringNode)
	smallest() string
	unlock()
}

// stringInternalNode represents an internal node for a StringTree with
// String keys.
type stringInternalNode struct {
	runts    []string
	children []stringNode
	mutex    sync.Mutex
}

func (left *stringInternalNode) absorbRight(sibling stringNode) {
	right := sibling.(*stringInternalNode)
	left.runts = append(left.runts, right.runts...)
	left.children = append(left.children, right.children...)

	// Perhaps following are not strictly needed, but de-allocate slices.
	right.runts = nil
	right.children = nil
}

func (right *stringInternalNode) adoptFromLeft(sibling stringNode) {
	left := sibling.(*stringInternalNode)

	right.runts = append(right.runts, "")
	right.children = append(right.children, nil)
	copy(right.runts[1:], right.runts[0:])
	copy(right.children[1:], right.children[0:])

	index := len(left.runts) - 1
	right.runts[0] = left.runts[index]
	right.children[0] = left.children[index]

	left.runts = left.runts[:index]
	left.children = left.children[:index]
}

func (left *stringInternalNode) adoptFromRight(sibling stringNode) {
	right := sibling.(*stringInternalNode)

	left.runts = append(left.runts, right.runts[0])
	left.children = append(left.children, right.children[0])

	copy(right.runts[0:], right.runts[1:])
	copy(right.children[0:], right.children[1:])

	index := len(right.runts) - 1
	right.runts = right.runts[:index]
	right.children = right.children[:index]
}

func (i *stringInternalNode) count() int { return len(i.runts) }

func (i *stringInternalNode) deleteKey(minSize int, key string) bool {
	index := stringSearchLessThanOrEqualTo(key, i.runts)
	child := i.children[index]
	child.lock()
	defer child.unlock()

	if !child.deleteKey(minSize, key) {
		return false
	}
	// POST: child is too small

	var leftSibling, rightSibling stringNode
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

func (i *stringInternalNode) isInternal() bool { return true }

func (i *stringInternalNode) lock() { i.mutex.Lock() }

// maybeSplit splits the node, giving half of its values to its new sibling,
// when the node is too full to accept any more values.
//
// NOTE: This loop assumes the tree's order is a multiple of 2, which must be
// guarded for at tree instantiation time.
func (i *stringInternalNode) maybeSplit(order int) (stringNode, stringNode) {
	if len(i.runts) < order {
		return i, nil
	}
	newNodeRunts := order >> 1
	sibling := &stringInternalNode{
		runts:    make([]string, newNodeRunts, order),
		children: make([]stringNode, newNodeRunts, order),
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

func (i *stringInternalNode) smallest() string {
	if len(i.runts) == 0 {
		panic("internal node has no children")
	}
	return i.runts[0]
}

func (i *stringInternalNode) unlock() { i.mutex.Unlock() }

// stringLeafNode represents a leaf node for a StringTree using
// String keys.
type stringLeafNode struct {
	runts  []string
	values []interface{}
	next   *stringLeafNode // points to next leaf to allow enumeration
	mutex  sync.Mutex
}

func (left *stringLeafNode) absorbRight(sibling stringNode) {
	right := sibling.(*stringLeafNode)
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

func (right *stringLeafNode) adoptFromLeft(sibling stringNode) {
	left := sibling.(*stringLeafNode)

	right.runts = append(right.runts, "")
	right.values = append(right.values, nil)
	copy(right.runts[1:], right.runts[0:])
	copy(right.values[1:], right.values[0:])

	index := len(left.runts) - 1
	right.runts[0] = left.runts[index]
	right.values[0] = left.values[index]

	left.runts = left.runts[:index]
	left.values = left.values[:index]
}

func (left *stringLeafNode) adoptFromRight(sibling stringNode) {
	right := sibling.(*stringLeafNode)
	left.runts = append(left.runts, right.runts[0])
	left.values = append(left.values, right.values[0])
	copy(right.runts[0:], right.runts[1:])
	copy(right.values[0:], right.values[1:])
	index := len(right.runts) - 1
	right.runts = right.runts[:index]
	right.values = right.values[:index]
}

func (l *stringLeafNode) count() int { return len(l.runts) }

func (l *stringLeafNode) deleteKey(minSize int, key string) bool {
	index := stringSearchGreaterThanOrEqualTo(key, l.runts)
	if index == len(l.runts) || key != l.runts[index] {
		return false
	}
	copy(l.runts[index:], l.runts[index+1:])
	copy(l.values[index:], l.values[index+1:])
	l.runts = l.runts[:len(l.runts)-1]
	l.values = l.values[:len(l.values)-1]
	return len(l.runts) < minSize
}

func (l *stringLeafNode) isInternal() bool { return false }

func (l *stringLeafNode) lock() { l.mutex.Lock() }

// maybeSplit splits the node, giving half of its values to its new sibling,
// when the node is too full to accept any more values.
//
// NOTE: This loop assumes the tree's order is a multiple of 2, which must be
// guarded for at tree instantiation time.
func (l *stringLeafNode) maybeSplit(order int) (stringNode, stringNode) {
	if len(l.runts) < order {
		return l, nil
	}
	newNodeRunts := order >> 1
	sibling := &stringLeafNode{
		runts:  make([]string, newNodeRunts, order),
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

func (l *stringLeafNode) smallest() string {
	if len(l.runts) == 0 {
		panic("leaf node has no children")
	}
	return l.runts[0]
}

func (l *stringLeafNode) unlock() { l.mutex.Unlock() }

// StringTree is a B+Tree of elements using String keys.
type StringTree struct {
	root  stringNode
	order int
}

// NewStringTree returns a newly initialized StringTree of the specified
// order.
func NewStringTree(order int) (*StringTree, error) {
	if order <= 0 || order%2 == 1 {
		return nil, fmt.Errorf("cannot create tree when order is not a multiple of 2: %d", order)
	}
	return &StringTree{
		root: &stringLeafNode{
			runts:  make([]string, 0, order),
			values: make([]interface{}, 0, order),
		},
		order: order,
	}, nil
}

// Delete removes the key-value pair from the tree.
func (t *StringTree) Delete(key string) {
	t.root.lock()
	defer t.root.unlock()

	if !t.root.deleteKey(t.order, key) || t.root.count() > 1 {
		// Root is only too small when fewer than 2 children
		return
	}
	// Root might be an internal or a leaf node. If leaf node, the root is
	// already as small as can be.
	if root, ok := t.root.(*stringInternalNode); ok {
		// Root has outlived its usefulness when it has only a single child.
		t.root = root.children[0]
	}
}

// Insert inserts the key-value pair into the tree, replacing the existing value
// with the new value if the key is already in the tree.
func (t *StringTree) Insert(key string, value interface{}) {
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
		t.root = &stringInternalNode{
			runts:    []string{leftSmallest, rightSmallest},
			children: []stringNode{left, right},
		}
		// Decide whether we need to descend left or right.
		if key >= rightSmallest {
			right.lock()
			n.unlock() // unlock the left, since same node
			n = right
		}
	}

	for n.isInternal() {
		parent := n.(*stringInternalNode)
		index := stringSearchLessThanOrEqualTo(key, parent.runts)

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
			parent.runts = append(parent.runts, "")
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

	ln := n.(*stringLeafNode)

	// When the new value will become the first element in a leaf, which is only
	// possible for an empty tree, or when new key comes after final leaf runt,
	// a simple append will suffice.
	if len(ln.runts) == 0 || key > ln.runts[len(ln.runts)-1] {
		ln.runts = append(ln.runts, key)
		ln.values = append(ln.values, value)
		ln.unlock()
		return
	}

	index := stringSearchGreaterThanOrEqualTo(key, ln.runts)

	if key == ln.runts[index] {
		// When the key matches the runt, merely need to update the value.
		ln.values[index] = value
		ln.unlock()
		return
	}

	// Make room for and insert the new key-value pair into leaf.

	// Append zero values to make room in arrays
	ln.runts = append(ln.runts, "")
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
func (t *StringTree) Search(key string) (interface{}, bool) {
	var value interface{}
	var ok bool
	n := t.root
	n.lock()
	for n.isInternal() {
		parent := n.(*stringInternalNode)
		child := parent.children[stringSearchLessThanOrEqualTo(key, parent.runts)]
		child.lock()
		parent.unlock()
		n = child
	}
	l := n.(*stringLeafNode)

	if len(l.runts) > 0 {
		i := stringSearchGreaterThanOrEqualTo(key, l.runts)
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
func (t *StringTree) Update(key string, callback func(interface{}, bool) interface{}) {
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
		t.root = &stringInternalNode{
			runts:    []string{leftSmallest, rightSmallest},
			children: []stringNode{left, right},
		}
		// Decide whether we need to descend left or right.
		if key >= rightSmallest {
			right.lock()
			n.unlock() // unlock the left, since same node
			n = right
		}
	}

	for n.isInternal() {
		parent := n.(*stringInternalNode)
		index := stringSearchLessThanOrEqualTo(key, parent.runts)

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
			parent.runts = append(parent.runts, "")
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

	ln := n.(*stringLeafNode)

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

	index := stringSearchGreaterThanOrEqualTo(key, ln.runts)

	if key == ln.runts[index] {
		// When the key matches the runt, merely need to update the value.
		ln.values[index] = callback(ln.values[index], true)
		ln.unlock()
		return
	}

	// Make room for and insert the new key-value pair into leaf.

	// Append zero values to make room in arrays
	ln.runts = append(ln.runts, "")
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
// values in a StringTree, invoke with key set to the empty string.
//
// NOTE: This function exists still holding the lock on one of the tree's leaf
// nodes, which may block other operations on the tree that require modification
// of the locked node. The leaf node is only unlocked either by closing the
// Cursor, or after all key-value pairs have been visited using Scan.
func (t *StringTree) NewScanner(key string) *StringCursor {
	n := t.root
	n.lock()
	for n.isInternal() {
		parent := n.(*stringInternalNode)
		child := parent.children[stringSearchLessThanOrEqualTo(key, parent.runts)]
		child.lock()
		parent.unlock()
		n = child
	}
	ln := n.(*stringLeafNode)
	return newStringCursor(ln, stringSearchGreaterThanOrEqualTo(key, ln.runts))
}

// StringCursor is used to enumerate key-value pairs from the tree in
// ascending order.
type StringCursor struct {
	l *stringLeafNode
	i int
}

func newStringCursor(l *stringLeafNode, i int) *StringCursor {
	// Initialize cursor with index one smaller than requested, so initial scan
	// lines up the cursor to reference the desired key-value pair.
	return &StringCursor{l: l, i: i - 1}
}

// Close releases the lock on the leaf node under the cursor. This method is
// provided to signal no further intention of scanning the remainder key-value
// pairs in the tree. It is not necessary to call Close if Scan is called
// repeatedly until Scan returns false.
func (c *StringCursor) Close() error {
	if c.l != nil {
		c.l.unlock()
		c.l = nil
	}
	return nil
}

// Pair returns the key-value pair referenced by the cursor.
func (c *StringCursor) Pair() (string, interface{}) {
	return c.l.runts[c.i], c.l.values[c.i]
}

// Scan advances the cursor to reference the next key-value pair in the tree in
// ascending order, and returns true when there is at least one more key-value
// pair to be observed with the Pair method. If the final key-value pair has
// already been observed, this unlocks the final leaf in the tree and returns
// false.
func (c *StringCursor) Scan() bool {
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
