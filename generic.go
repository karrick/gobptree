package gobptree

import (
	"cmp"
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

// genericNode represents either an internal or a leaf node for a
// Int64Tree using Int64 keys.
type genericNode[K cmp.Ordered] interface {
	absorbRight(genericNode[K])
	adoptFromLeft(genericNode[K])
	adoptFromRight(genericNode[K])
	count() int
	deleteKey(int, K) bool
	isInternal() bool
	lock()
	maybeSplit(order int) (genericNode[K], genericNode[K])
	smallest() K
	unlock()
}

// genericInternalNode represents an internal node for a GenericTree with keys
// of any cmp.Ordered type.
type genericInternalNode[K cmp.Ordered] struct {
	Runts    []K
	Children []genericNode[K]
	mutex    sync.Mutex
}

func (left *genericInternalNode[K]) absorbRight(sibling genericNode[K]) {
	right := sibling.(*genericInternalNode[K])
	left.Runts = append(left.Runts, right.Runts...)
	left.Children = append(left.Children, right.Children...)

	// Perhaps following are not strictly needed, but de-allocate slices.
	right.Runts = nil
	right.Children = nil
}

func (right *genericInternalNode[K]) adoptFromLeft(sibling genericNode[K]) {
	var zeroValue K

	left := sibling.(*genericInternalNode[K])

	right.Runts = append(right.Runts, zeroValue)
	right.Children = append(right.Children, nil)
	copy(right.Runts[1:], right.Runts[0:])
	copy(right.Children[1:], right.Children[0:])

	index := len(left.Runts) - 1
	right.Runts[0] = left.Runts[index]
	right.Children[0] = left.Children[index]

	left.Runts = left.Runts[:index]
	left.Children = left.Children[:index]
}

func (left *genericInternalNode[K]) adoptFromRight(sibling genericNode[K]) {
	right := sibling.(*genericInternalNode[K])

	left.Runts = append(left.Runts, right.Runts[0])
	left.Children = append(left.Children, right.Children[0])

	copy(right.Runts[0:], right.Runts[1:])
	copy(right.Children[0:], right.Children[1:])

	index := len(right.Runts) - 1
	right.Runts = right.Runts[:index]
	right.Children = right.Children[:index]
}

func (i *genericInternalNode[K]) count() int { return len(i.Runts) }

// deleteKey removes key and its value from the node, returning true when the
// node has fewer items than minSize.
func (i *genericInternalNode[K]) deleteKey(minSize int, key K) bool {
	index := searchLessThanOrEqualTo(key, i.Runts)
	child := i.Children[index]
	child.lock()
	defer child.unlock()

	if !child.deleteKey(minSize, key) {
		return false
	}

	// POST: child is too small; need to combine node with one of its
	// immediate neighbors.

	var leftSibling, rightSibling genericNode[K]
	var leftCount, rightCount int

	if index < len(i.Runts)-1 {
		// try right sibling first to encourage left leaning trees
		rightSibling = i.Children[index+1]
		rightSibling.lock()
		defer rightSibling.unlock()
		if rightCount = rightSibling.count(); rightCount > minSize {
			child.adoptFromRight(rightSibling)
			i.Runts[index+1] = rightSibling.smallest()
			return false
		}
	}
	// POST: If right, it is exactly minimum size.

	if index > 0 {
		// try left sibling
		leftSibling = i.Children[index-1]
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
	// the siblings it does have each only has the minimum number of Children.

	if leftCount > 0 {
		leftSibling.absorbRight(child)
		copy(i.Runts[index:], i.Runts[index+1:])
		i.Runts = i.Runts[:len(i.Runts)-1]
		copy(i.Children[index:], i.Children[index+1:])
		i.Children = i.Children[:len(i.Children)-1]
		// This node has one fewer Children.
		return len(i.Runts) < minSize
	}

	// When right has no Children, then should not be in a position where left
	// also has no Children.
	if rightCount == 0 {
		panic("both left and right siblings have no Children")
	}

	child.absorbRight(rightSibling)
	copy(i.Runts[index+1:], i.Runts[index+2:])
	i.Runts = i.Runts[:len(i.Runts)-1]
	copy(i.Children[index+1:], i.Children[index+2:])
	i.Children = i.Children[:len(i.Children)-1]
	// This node has one fewer Children.
	return len(i.Runts) < minSize
}

func (i *genericInternalNode[K]) isInternal() bool { return true }

func (i *genericInternalNode[K]) lock() { /* i.mutex.Lock() */ }

// maybeSplit splits the node, giving half of its Values to its new sibling,
// when the node is too full to accept any more Values. When it does return a
// new right sibling, that node is locked.
//
// NOTE: This loop assumes the tree's order is a multiple of 2, which must be
// guarded for at tree instantiation time.
func (i *genericInternalNode[K]) maybeSplit(order int) (genericNode[K], genericNode[K]) {
	if len(i.Runts) < order {
		return i, nil
	}

	newNodeRunts := order >> 1
	sibling := &genericInternalNode[K]{
		Runts:    make([]K, newNodeRunts, order),
		Children: make([]genericNode[K], newNodeRunts, order),
	}

	// NOTE: Newly created sibling should be locked before attached to the
	// tree in order to prevent a data race where another goroutine finds this
	// new node.
	sibling.lock()

	// Right half of this node moves to sibling.
	for j := 0; j < newNodeRunts; j++ {
		sibling.Runts[j] = i.Runts[newNodeRunts+j]
		sibling.Children[j] = i.Children[newNodeRunts+j]
	}

	// Clear the runts and children pointers from the original node.
	i.Runts = i.Runts[:newNodeRunts]
	i.Children = i.Children[:newNodeRunts]

	return i, sibling
}

func (i *genericInternalNode[K]) smallest() K {
	if len(i.Runts) == 0 {
		panic("internal node has no Children")
	}
	return i.Runts[0]
}

func (i *genericInternalNode[K]) unlock() { /* i.mutex.Unlock() */ }

// genericLeafNode represents a leaf node for a Int64Tree using
// Int64 keys.
type genericLeafNode[K cmp.Ordered] struct {
	Runts  []K
	Values []any
	Next   *genericLeafNode[K] // points to next leaf to allow enumeration
	mutex  sync.Mutex
}

func (left *genericLeafNode[K]) absorbRight(sibling genericNode[K]) {
	right := sibling.(*genericLeafNode[K])
	if left.Next != right {
		// Superfluous check
		panic("cannot merge leaf with sibling other than next sibling")
	}
	left.Runts = append(left.Runts, right.Runts...)
	left.Values = append(left.Values, right.Values...)
	left.Next = right.Next

	// Perhaps following are not strictly needed, but de-allocate slices and
	// release pointers.
	right.Runts = nil
	right.Values = nil
	right.Next = nil
}

func (right *genericLeafNode[K]) adoptFromLeft(sibling genericNode[K]) {
	var zeroValue K

	left := sibling.(*genericLeafNode[K])

	right.Runts = append(right.Runts, zeroValue)
	right.Values = append(right.Values, nil)
	copy(right.Runts[1:], right.Runts[0:])
	copy(right.Values[1:], right.Values[0:])

	index := len(left.Runts) - 1
	right.Runts[0] = left.Runts[index]
	right.Values[0] = left.Values[index]

	left.Runts = left.Runts[:index]
	left.Values = left.Values[:index]
}

func (left *genericLeafNode[K]) adoptFromRight(sibling genericNode[K]) {
	right := sibling.(*genericLeafNode[K])
	left.Runts = append(left.Runts, right.Runts[0])
	left.Values = append(left.Values, right.Values[0])
	copy(right.Runts[0:], right.Runts[1:])
	copy(right.Values[0:], right.Values[1:])
	index := len(right.Runts) - 1
	right.Runts = right.Runts[:index]
	right.Values = right.Values[:index]
}

func (l *genericLeafNode[K]) count() int { return len(l.Runts) }

func (l *genericLeafNode[K]) deleteKey(minSize int, key K) bool {
	index := searchGreaterThanOrEqualTo(key, l.Runts)
	if index == len(l.Runts) || key != l.Runts[index] {
		return false
	}
	copy(l.Runts[index:], l.Runts[index+1:])
	copy(l.Values[index:], l.Values[index+1:])
	l.Runts = l.Runts[:len(l.Runts)-1]
	l.Values = l.Values[:len(l.Values)-1]
	return len(l.Runts) < minSize
}

func (l *genericLeafNode[K]) isInternal() bool { return false }

func (l *genericLeafNode[K]) lock() { /* l.mutex.Lock() */ }

// maybeSplit splits the node, giving half of its Values to its new sibling,
// when the node is too full to accept any more Values. When it does return a
// new right sibling, that node is locked.
//
// NOTE: This loop assumes the tree's order is a multiple of 2, which must be
// guarded for at tree instantiation time.
func (l *genericLeafNode[K]) maybeSplit(order int) (genericNode[K], genericNode[K]) {
	if len(l.Runts) < order {
		return l, nil
	}

	newNodeRunts := order >> 1
	sibling := &genericLeafNode[K]{
		Runts:  make([]K, newNodeRunts, order),
		Values: make([]any, newNodeRunts, order),
		Next:   l.Next,
	}

	// NOTE: Newly created sibling should be locked before attached to the
	// tree in order to prevent a data race where another goroutine finds this
	// new node.
	sibling.lock()

	// Right half of this node moves to sibling.
	for j := 0; j < newNodeRunts; j++ {
		sibling.Runts[j] = l.Runts[newNodeRunts+j]
		sibling.Values[j] = l.Values[newNodeRunts+j]
	}

	// Clear the Runts and pointers from the original node.
	l.Runts = l.Runts[:newNodeRunts]
	l.Values = l.Values[:newNodeRunts]
	l.Next = sibling

	return l, sibling
}

func (l *genericLeafNode[K]) smallest() K {
	if len(l.Runts) == 0 {
		panic("leaf node has no Children")
	}
	return l.Runts[0]
}

func (l *genericLeafNode[K]) unlock() { /* l.mutex.Unlock() */ }

// GenericTree is a B+Tree of elements using Int64 keys.
type GenericTree[K cmp.Ordered] struct {
	root  genericNode[K]
	order int
	rootMutex sync.Mutex
}

// NewGenericTree returns a newly initialized GenericTree of the specified
// order.
func NewGenericTree[K cmp.Ordered](order int) (*GenericTree[K], error) {
	if err := checkOrder(order); err != nil {
		return nil, err
	}
	return &GenericTree[K]{
		root: &genericLeafNode[K]{
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

	if !t.root.deleteKey(t.order, key) || t.root.count() > 1 {
		// Root is only too small when fewer than 2 Children
		return
	}
	// Root might be an internal or a leaf node. If leaf node, the root is
	// already as small as can be.
	if root, ok := t.root.(*genericInternalNode[K]); ok {
		// Root has outlived its usefulness when it has only a single child.
		t.root = root.Children[0]
	}
}

// Insert inserts the key-value pair into the tree, replacing the existing value
// with the new value if the key is already in the tree.
func (t *GenericTree[K]) Insert(key K, value any) {
	t.rootMutex.Lock()
	defer t.rootMutex.Unlock()

	var zeroValue K

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
		t.root = &genericInternalNode[K]{
			Runts:    []K{leftSmallest, rightSmallest},
			Children: []genericNode[K]{left, right},
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
		parent := n.(*genericInternalNode[K])
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
			parent.Runts = append(parent.Runts, zeroValue)
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

	ln := n.(*genericLeafNode[K])

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
	ln.Runts = append(ln.Runts, zeroValue)
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
		parent := n.(*genericInternalNode[K])
		child := parent.Children[searchLessThanOrEqualTo(key, parent.Runts)]
		child.lock()
		parent.unlock()
		n = child
	}
	l := n.(*genericLeafNode[K])

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

	var zeroValue K

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
		t.root = &genericInternalNode[K]{
			Runts:    []K{leftSmallest, rightSmallest},
			Children: []genericNode[K]{left, right}, // 511
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
		parent := n.(*genericInternalNode[K])
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
			parent.Runts = append(parent.Runts, zeroValue)
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

	ln := n.(*genericLeafNode[K])

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
	ln.Runts = append(ln.Runts, zeroValue)
	ln.Values = append(ln.Values, nil)
	// Shift elements to the right to make room for new data
	copy(ln.Runts[index+1:], ln.Runts[index:])
	copy(ln.Values[index+1:], ln.Values[index:])
	// Store the new data
	ln.Runts[index] = key
	ln.Values[index] = callback(nil, false)
	ln.unlock()
}

// NewScanner returns a cursor that iteratively returns key-value pairs from the
// tree in ascending order starting at key, or if key is not found the next key,
// and ending after all successive pairs have been returned. To enumerate all
// Values in a GenericTree, invoke with key set to math.MinInt64.
//
// NOTE: This function exists still holding the lock on one of the tree's leaf
// nodes, which may block other operations on the tree that require modification
// of the locked node. The leaf node is only unlocked either by closing the
// Cursor, or after all key-value pairs have been visited using Scan.
func (t *GenericTree[K]) NewScanner(key K) *GenericCursor[K] {
	n := t.root
	n.lock()
	for n.isInternal() {
		parent := n.(*genericInternalNode[K])
		child := parent.Children[searchLessThanOrEqualTo(key, parent.Runts)]
		child.lock()
		parent.unlock()
		n = child
	}
	ln := n.(*genericLeafNode[K])
	return newGenericCursor(ln, searchGreaterThanOrEqualTo(key, ln.Runts))
}

// GenericCursor is used to enumerate key-value pairs from the tree in
// ascending order.
type GenericCursor[K cmp.Ordered] struct {
	l *genericLeafNode[K]
	i int
}

func newGenericCursor[K cmp.Ordered](l *genericLeafNode[K], i int) *GenericCursor[K] {
	// Initialize cursor with index one smaller than requested, so initial scan
	// lines up the cursor to reference the desired key-value pair.
	return &GenericCursor[K]{l: l, i: i - 1}
}

// Close releases the lock on the leaf node under the cursor. This method is
// provided to signal no further intention of scanning the remainder key-value
// pairs in the tree. It is not necessary to call Close if Scan is called
// repeatedly until Scan returns false.
func (c *GenericCursor[K]) Close() error {
	if c.l != nil {
		c.l.unlock()
		c.l = nil
	}
	return nil
}

// Pair returns the key-value pair referenced by the cursor.
func (c *GenericCursor[K]) Pair() (K, any) {
	return c.l.Runts[c.i], c.l.Values[c.i]
}

// Scan advances the cursor to reference the next key-value pair in the tree in
// ascending order, and returns true when there is at least one more key-value
// pair to be observed with the Pair method. If the final key-value pair has
// already been observed, this unlocks the final leaf in the tree and returns
// false.
func (c *GenericCursor[K]) Scan() bool {
	if c.i++; c.i == len(c.l.Runts) {
		if c.l.Next == nil {
			c.l.unlock()
			c.l = nil
			return false
		}
		n := c.l.Next
		n.lock()
		c.l.unlock()
		c.l = n
		c.i = 0
	}
	return true
}
