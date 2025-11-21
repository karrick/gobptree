package gobptree

// NOTE: Because many insertion loops successively insert larger numbers, when
// splitting nodes, rather than splitting a node evenly, consider splitting it
// in a way that puts an extra node on the left side, so the next node to be
// added will end up on the right side, and they both remain balanced.

import (
	"cmp"
	"fmt"
	"io"
	"sync"
)

// node represents either an internal or a leaf node for a GenericTree with
// keys of a cmp.Ordered type, and values of any type.
type node[K cmp.Ordered, V any] interface {
	absorbFromRight(node[K, V])
	adoptFromLeft(node[K, V])
	adoptFromRight(node[K, V])
	count() int
	deleteKey(func([]K, K) (int, bool), int, K) (int, K)
	isInternal() bool
	lock()
	render(io.Writer, string) // TESTING
	rlock()
	runlock()
	runts() []K  // DEBUG
	smallest() K // TODO now only rebalance needs this?
	unlock()
	updateKey(func([]K, K) (int, bool), K, int, bool, func(V, bool) V) node[K, V]
}

// GenericTree is a B+Tree of elements using key whose type satisfy the
// cmp.Ordered constraint.
type GenericTree[K cmp.Ordered, V any] struct {
	root           node[K, V]
	insertionIndex func([]K, K) (int, bool)
	order          int // order is the maximum number of elements each node may have
	minSize        int // minSize is the minimum number of elements each node may have
	rootMutex      sync.RWMutex
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
		insertionIndex: insertionIndexSelect[K](),
		minSize:        order >> 1, // each node should store be at least half full
		order:          order,
	}, nil
}

const genericTreeLocking = false

func (t *GenericTree[K, V]) lock() {
	if genericTreeLocking {
		t.rootMutex.Lock()
	}
}

func (t *GenericTree[K, V]) rlock() {
	if genericTreeLocking {
		t.rootMutex.RLock()
	}
}

func (t *GenericTree[K, V]) runlock() {
	if genericTreeLocking {
		t.rootMutex.RUnlock()
	}
}

func (t *GenericTree[K, V]) unlock() {
	if genericTreeLocking {
		t.rootMutex.Unlock()
	}
}

// Delete removes the key-value pair from the tree, or returns without an
// error if the key was not a member of the tree.
func (t *GenericTree[K, V]) Delete(key K) {
	debug := newDebug(true, "GenericTree.Delete(key=%v, order=%d)", key, t.order)

	debug("BEFORE deleteKey keys: %v\n", t.getKeys())

	// Because a delete operation may result in removal of the root node, need
	// to acquire exclusive lock for the entire tree before begin, then
	// release the lock upon method completion.
	t.lock()
	defer t.unlock()

	// Before visiting each node, must acquire its lock. Because a delete
	// might modify all nodes from the root of the tree to the leaf node, need
	// to obtain an exclusive lock to each node.
	t.root.lock()
	defer t.root.unlock()

	// NOTE: Before invoking count method, we know we can return without
	// combining nodes when deleteKey returns true. If deleteKey returns
	// false, then root node no longer has the minimum number of items.
	rootSize, _ := t.root.deleteKey(t.insertionIndex, t.minSize, key)
	enough := rootSize >= t.minSize

	// debug("AFTER deleteKey enough=%t keys: %v\n", enough, t.getKeys())

	if enough {
		return // root node is large enough
	}

	switch tv := t.root.(type) {
	case *internalNode[K, V]:
		if tv.count() == 1 {
			// When the root points to an internal node that has a single
			// child, update the root to point to that child.
			//
			// NOTE: This is why mutex needs to be held throughout lifetime.
			t.root = tv.Children[0]
		} else {
			// When the root points to an internal node that has multiple
			// children, there is nothing to be done.
		}
	case *leafNode[K, V]:
		// When root points to a single leaf node, there is nothing to be
		// done. The tree is already the smallest it could be.
	default:
		// Cannot get here unless bug introduced in library.
		panic(fmt.Errorf("BUG: GOT: %#v; WANT: node[K,V]", t.root))
	}
}

// getKeys is method used while debugging to return slice of keys present in
// tree.
func (tree *GenericTree[K, V]) getKeys() []K {
	var keys []K
	s := tree.NewScannerAll()
	for s.Scan() {
		k, _ := s.Pair()
		keys = append(keys, k)
	}
	if err := s.Close(); err != nil {
		panic(err)
	}
	return keys
}

// Insert inserts the key-value pair into the tree, replacing any existing
// value with the new value when the key is already in the tree.
func (t *GenericTree[K, V]) Insert(key K, value V) {
	// NOTE: This has the Same logic as Update, and rather than duplicate that
	// logic, merely invoke Update method with a callback that ignores its
	// arguments and returns the value to be stored.
	t.Update(key, func(_ V, _ bool) V { return value })
}

// Rebalance will rebalance the tree while ensuring that each node has no more
// than the number of elements provided as an argument to the method. For
// instance, to rebalance an order 64 tree so each node contains exactly 32
// children (except perhaps the final leaf node and its ancestors), one would
// invoke Rebalance(32). This could also fully pack a tree so each node is as
// full as possible, Rebalance(64). Both of these calls would speed up all
// tree traversals by ensuring a balanced tree. However, they can also leave
// room for additional growth throughout the tree's structure.
//
// NOTE: count must be between 2 and the tree order, inclusive: [2, order].
func (t *GenericTree[K, V]) Rebalance(count int) error {
	debug := newDebug(false, "GenericTree.Rebalance(count=%v, order=%d)", count, t.order)

	if false {
		// Enforce strict compliance with B+Tree properties.
		if count < t.minSize {
			return fmt.Errorf("cannot rebalance with count less than half the tree order: %d < %d", count, t.minSize)
		}
	}
	if count < 2 {
		return fmt.Errorf("cannot rebalance with count less than 2: %d", count)
	}
	if count > t.order {
		return fmt.Errorf("cannot rebalance with count higher than tree tree order: %d > %d", count, t.order)
	}

	var bottomNodes []node[K, V]

	targetLeaf := &leafNode[K, V]{
		Runts:  make([]K, t.order),
		Values: make([]V, t.order),
	}

	// Because this replaces the root node, must acquire and hold an exclusive
	// lock to the tree.
	t.lock()
	defer t.unlock()

	// Even though this is holding an exclusive lock to the tree, that only
	// prevents other mutators from starting. There is a chance that other
	// goroutines are lazily traversing the tree in a way that does not
	// require an exclusive lock on the entire tree. Therefore, when visiting
	// each node, must acquire read lock for that node, and release it only
	// after acquiring read lock for the next node to visit.
	n := t.root
	n.rlock()

	// Find the left most leaf node from which pairs will be copied.
	sourceLeaf := t.findAndLockFirstLeaf(n)

	var sourceCopyOffset int
	var targetCopyOffset int

	// Create new linked-list of leaf nodes by copying from source to target
	// nodes.
	for sourceLeaf != nil {
		debug("TOP OF LOOP\n")
		debug("source node remaining: %v\n", sourceLeaf.Runts[sourceCopyOffset:])
		debug("target node so far:    %v\n", targetLeaf.Runts[:targetCopyOffset])

		space := count - targetCopyOffset // space is how much more slots available in target leaf

		if space == 0 {
			// Shorten the length of the slices to the actual number of
			// elements copied from source leaves.
			targetLeaf.Runts = targetLeaf.Runts[:targetCopyOffset]
			targetLeaf.Values = targetLeaf.Values[:targetCopyOffset]
			targetCopyOffset = 0

			debug("FINISHED TARGET LEAF: %v\n", targetLeaf.Runts)

			// Create a new target leaf node.
			targetLeafNext := &leafNode[K, V]{
				Runts:  make([]K, t.order),
				Values: make([]V, t.order),
			}
			targetLeaf.Next = targetLeafNext

			bottomNodes = append(bottomNodes, targetLeaf)
			targetLeaf = targetLeafNext
			space = count // new target leaf can accommodate count items
		}

		debug("target space remaining: %d\n", space)

		runtsCopied := copy(targetLeaf.Runts[targetCopyOffset:count], sourceLeaf.Runts[sourceCopyOffset:])
		valuesCopied := copy(targetLeaf.Values[targetCopyOffset:count], sourceLeaf.Values[sourceCopyOffset:])

		if runtsCopied != valuesCopied {
			panic(fmt.Errorf("BUG: copied different number of runts and values: %d != %d", runtsCopied, valuesCopied))
		}

		sourceCopyOffset += runtsCopied
		targetCopyOffset += runtsCopied

		debug("copy(targetleaf.Runts[%d:%d], sourceLeaf.Runts[%d:]) -> %d items copied\n", targetCopyOffset, count, sourceCopyOffset, runtsCopied)
		debug("target after: len=%d cap=%d %v\n", len(targetLeaf.Runts), cap(targetLeaf.Runts), targetLeaf.Runts[:targetCopyOffset])

		// POST: either target is full, or source is empty
		if targetCopyOffset > count {
			panic(fmt.Errorf("BUG: copied too many runts to target leaf: %d", targetCopyOffset))
		}

		if sourceCopyOffset > len(sourceLeaf.Runts) {
			panic(fmt.Errorf("BUG: source copy offset > len source runts: %d > %d", sourceCopyOffset, len(sourceLeaf.Runts)))
		}

		if sourceCopyOffset == len(sourceLeaf.Runts) {
			debug("source copy offset == len source runts: %d == %d (need new source node)\n", sourceCopyOffset, len(sourceLeaf.Runts))

			// Advance to next leaf node, locking it first, then unlocking the
			// current node before advancement.
			sourceLeafNext := sourceLeaf.Next
			if sourceLeafNext != nil {
				debug("found another source leaf node\n")
				sourceCopyOffset = 0
				sourceLeafNext.rlock()
			} else {
				debug("did not find another source leaf node\n")
			}
			sourceLeaf.runlock()
			sourceLeaf = sourceLeafNext
		} else {
			debug("source copy offset < len source runts: %d < %d (need new target node)\n", sourceCopyOffset, len(sourceLeaf.Runts))
		}
	}

	if targetCopyOffset > 0 {
		targetLeaf.Runts = targetLeaf.Runts[:targetCopyOffset]
		targetLeaf.Values = targetLeaf.Values[:targetCopyOffset]
		debug("FINISHED LEAF: %v\n", targetLeaf.Runts)
		bottomNodes = append(bottomNodes, targetLeaf)
	}

	var topNodes []node[K, V]

	internal := &internalNode[K, V]{
		Runts:    make([]K, 0, t.order),
		Children: make([]node[K, V], 0, t.order),
	}

	// Continue building new layers on top of bottom nodes until bottom nodes
	// has only a single element.
	for len(bottomNodes) > 1 {
		for _, n := range bottomNodes {
			debug("BOTTOM NODE: %v\n", n)
		}
		for _, bottomNode := range bottomNodes {
			if len(internal.Runts) == count {
				debug("FINISHED INTERNAL A: %v\n", internal)
				for _, c := range internal.Children {
					debug("\tCHILD: %v\n", c)
				}
				topNodes = append(topNodes, internal)
				internal = &internalNode[K, V]{
					Runts:    make([]K, 0, t.order),
					Children: make([]node[K, V], 0, t.order),
				}
			}
			internal.Runts = append(internal.Runts, bottomNode.smallest())
			internal.Children = append(internal.Children, bottomNode)
		}
		if len(internal.Runts) > 0 {
			debug("FINISHED INTERNAL B: %v\n", internal)
			for _, c := range internal.Children {
				debug("\tCHILD: %v\n", c)
			}
			topNodes = append(topNodes, internal)
			internal = &internalNode[K, V]{
				Runts:    make([]K, 0, t.order),
				Children: make([]node[K, V], 0, t.order),
			}
		}
		for _, n := range topNodes {
			debug("TOP NODE: %v\n", n)
		}
		bottomNodes = topNodes
		topNodes = topNodes[:0]
		// topNodes = nil
	}

	t.root = bottomNodes[0]

	// IMPORTANT: If refactor so this tree always starts with internal node at
	// the root, then following logic must be updated when only a single leaf
	// node.

	return nil
}

func (t *GenericTree[K, V]) render(iow io.Writer, prefix string) {
	t.rlock()
	t.root.render(iow, prefix)
	t.runlock()
}

// Search returns the value associated with key from the tree. The second
// return value will be true when the key is in the tree, or will be false
// when the key is not a member of the tree.
func (t *GenericTree[K, V]) Search(key K) (V, bool) {
	t.rlock()   // Before can load root field must acquire read lock
	n := t.root // Load pointer to root of tree
	t.runlock() // Release read lock on tree

	// As walk tree and visit each node, need to acquire its read-lock.
	n.rlock()

	for {
		switch tv := n.(type) {

		case *internalNode[K, V]:
			child := tv.Children[searchLessThanOrEqualTo(key, tv.Runts)]
			child.rlock() // Acquire the read-lock for the child node
			tv.runlock()  // Release the read-lock for this node
			n = child     // Visit child node next

		case *leafNode[K, V]:
			var value V
			var ok bool

			if len(tv.Runts) > 0 {
				i := searchGreaterThanOrEqualTo(key, tv.Runts)
				ok = key == tv.Runts[i]
				if ok {
					value = tv.Values[i]
				}
			}

			tv.runlock() // Release the read-lock for this node

			return value, ok

		default:
			// Cannot get here unless bug introduced in library.
			panic(fmt.Errorf("BUG: GOT: %#v; WANT: node[K,V]", t.root))

		}
	}
	// NOT-REACHED
}

// Update searches for key and invokes callback with key's associated value,
// waits for callback to return a new value, and stores callback's return
// value as the new value for key. When key is not found, callback will be
// invoked with nil and false to signify the key was not found. After this
// method returns, the key will exist in the tree with the new value returned
// by the callback function.
func (t *GenericTree[K, V]) Update(key K, callback func(V, bool) V) {
	debug := newDebug(false, "GenericTree.Update(key=%v, order=%d)", key, t.order)

	// Because updating the tree may change the tree's pointer to the root
	// node, first acquire an exclusive lock to the tree.
	t.lock()
	defer t.unlock()

	newSibling := t.root.updateKey(t.insertionIndex, key, t.order, false, callback)
	if newSibling == nil {
		debug("no root split\n")
		return
	}

	debug("root split\n", key)

	// POST: root has a new sibling; must create new internal node to hold
	// them both.
	newRoot := &internalNode[K, V]{
		Runts:    make([]K, 2, t.order),
		Children: make([]node[K, V], 2, t.order),
	}

	newRoot.Runts[0] = t.root.smallest()
	newRoot.Children[0] = t.root

	newRoot.Runts[1] = newSibling.smallest()
	newRoot.Children[1] = newSibling

	t.root = newRoot
}

// NewScanner returns a cursor that iteratively returns key-value pairs from
// the tree in ascending order starting at the specified key, or, if key is
// not found, the next key; and ending after all successive pairs have been
// returned. To enumerate all values in a tree, use NewScannerAll, which is
// faster than invoking this method with the minimum key value.
//
// NOTE: This function exits still holding a read-lock on one of the tree's
// leaf nodes, which will block other operations on the tree that require
// modification of the locked node. The leaf node is only unlocked after
// closing the Cursor or after Scan returns false.
//
//	var l int
//	cursor := s.tree.NewScanner(13)
//	for cursor.Scan() {
//		l++
//	}
//	if err := cursor.Close(); err != nil {
//		return err
//	}
func (t *GenericTree[K, V]) NewScanner(key K) *GenericCursor[K, V] {
	t.rlock()   // Before can load root field must acquire read lock
	n := t.root // Load pointer to root of tree
	t.runlock() // Release read lock on tree

	// As walk tree and visit each node, need to acquire its read-lock.
	n.rlock()

	for {
		switch tv := n.(type) {

		case *internalNode[K, V]:
			// Next node to visit is the child node
			child := tv.Children[searchLessThanOrEqualTo(key, tv.Runts)]
			child.rlock() // Acquire the read-lock for the child node
			tv.runlock()  // Release the read-lock for this node
			n = child     // Visit child node next

		case *leafNode[K, V]:
			// NOTE: The read-lock for the leaf node will be released when
			// scanner is closed.
			return newGenericCursor(tv, searchGreaterThanOrEqualTo(key, tv.Runts))

		default:
			// Cannot get here unless bug introduced in library.
			panic(fmt.Errorf("BUG: GOT: %#v; WANT: node[K,V]", n))

		}
	}
	// NOT-REACHED
}

// findAndLockFirstLeaf walks the tree to the first leaf node, acquires a read
// lock, then returns it.
//
// NOTE: Must have either read or exclusive lock for n.
func (t *GenericTree[K, V]) findAndLockFirstLeaf(n node[K, V]) *leafNode[K, V] {
	for {
		switch tv := n.(type) {
		case *internalNode[K, V]:
			child := tv.Children[0] // Next node to visit is the child node
			child.rlock()           // Acquire the read-lock for the child node
			tv.runlock()            // Release the read-lock for this node
			n = child               // Visit child node next

		case *leafNode[K, V]:
			// NOTE: The read-lock for the leaf node will be released when
			// scanner is closed.
			return tv

		default:
			// Cannot get here unless bug introduced in library.
			panic(fmt.Errorf("BUG: GOT: %#v; WANT: node[K,V]", n))

		}
	}
	// NOT-REACHED
}

// NewScannerAll returns a cursor that iteratively returns all key-value pairs
// from the tree in ascending order. To start scanning at a particular key
// value, use NewScanner. This method is faster than invoking NewScanner with
// the minimum key value.
//
// NOTE: This function exits still holding a read-lock on one of the tree's
// leaf nodes, which will block other operations on the tree that require
// modification of the locked node. The leaf node is only unlocked after
// closing the Cursor or after Scan returns false.
//
//	var l int
//	cursor := s.tree.NewScannerAll()
//	for cursor.Scan() {
//		l++
//	}
//	if err := cursor.Close(); err != nil {
//		return err
//	}
func (t *GenericTree[K, V]) NewScannerAll() *GenericCursor[K, V] {
	t.rlock()   // Before can load root field must acquire read lock
	n := t.root // Load pointer to root of tree
	t.runlock() // Release read lock on tree

	// As walk tree and visit each node, need to acquire its read-lock.
	n.rlock()

	leaf := t.findAndLockFirstLeaf(n)

	// NOTE: The read lock for the leaf node will be released when scanner is
	// closed.
	return newGenericCursor(leaf, 0) // start at left most value
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
// pairs in the tree, useful when caller does not intend to invoke Scan again,
// but there are more elements to be returned.
//
// It is not necessary to invoke this method if the Scan method was invoked
// and returned false. It is however safe to invoke this method multiple times
// or after Scan returned false to signal no more items to be visited.
func (c *GenericCursor[K, V]) Close() error {
	if c.leaf != nil {
		c.leaf.runlock()
		c.leaf = nil
	}
	return nil
}

// Pair returns the key-value pair referenced by the cursor.
//
// This method will panic when invoked before the Scan method has been invoked
// at least once, and can potentially cause a panic or race condition if
// invoked after the Scan method returned false, or after the cursor's Close
// method was invoked.
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
	c.index++

	if c.index == len(c.leaf.Runts) {
		n := c.leaf.Next
		c.leaf.runlock()
		if n == nil {
			return false
		}
		n.rlock()
		c.leaf = n
		c.index = 0
	}

	return true
}
