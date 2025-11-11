package gobptree

// NOTE: Because many insertion loops successively insert larger numbers, when
// splitting nodes, rather than splitting a node evenly, consider splitting it
// in a way that puts an extra node on the left side, so the next node to be
// added will end up on the right side, and they both remain balanced.

import (
	"cmp"
	"errors"
	"fmt"
	"os"
	"sync"
)

// searchGreaterThanOrEqualTo returns the index of the first value from values
// that is greater than or equal to key.  search for index of runt that is
// greater than or equal to key.
func searchGreaterThanOrEqualTo[K cmp.Ordered](key K, keys []K) int {
	var lo int

	hi := len(keys)
	if hi <= 1 {
		return 0
	}
	hi--

loop:
	i := (lo + hi) >> 1
	if key < keys[i] {
		if hi = i; lo < hi {
			goto loop
		}
		return lo
	}
	if keys[i] < key {
		if lo = i + 1; lo < hi {
			goto loop
		}
		return lo
	}
	return i // match
}

// searchLessThanOrEqualTo returns the index of the first value from values
// that is less than or equal to key.
func searchLessThanOrEqualTo[K cmp.Ordered](key K, keys []K) int {
	index := searchGreaterThanOrEqualTo(key, keys)
	// convert result to less than or equal to
	if index == len(keys) || key < keys[index] {
		if index > 0 {
			return index - 1
		}
	}
	return index
}

// node represents either an internal or a leaf node for a GenericTree with
// keys of a cmp.Ordered type, and values of any type.
type node[K cmp.Ordered, V any] interface {
	absorbFromRight(node[K, V])
	adoptFromLeft(node[K, V])
	adoptFromRight(node[K, V])
	count() int
	deleteKey(int, K) bool
	isInternal() bool
	lock()
	maybeSplit(order int) (node[K, V], node[K, V])
	rlock()
	runlock()
	runts() []K // DEBUG
	smallest() K
	unlock()
}

// GenericTree is a B+Tree of elements using key whose type satisfy the
// cmp.Ordered constraint.
type GenericTree[K cmp.Ordered, V any] struct {
	root      node[K, V]
	order     int // order is the maximum number of elements each node may have
	minSize   int // minSize is the minimum number of elements each node may have
	rootMutex sync.RWMutex
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
		minSize: order >> 1, // each node should store be at least half full
		order:   order,
	}, nil
}

func (t *GenericTree[K, V]) lock()    { t.rootMutex.Lock() }
func (t *GenericTree[K, V]) rlock()   { t.rootMutex.RLock() }
func (t *GenericTree[K, V]) runlock() { t.rootMutex.RUnlock() }
func (t *GenericTree[K, V]) unlock()  { t.rootMutex.Unlock() }

// Delete removes the key-value pair from the tree.
func (t *GenericTree[K, V]) Delete(key K) {
	// Because delete operation may result in removal of the root node, need
	// to acquire exclusive lock for the entire tree before begin, then
	// release the lock upon method completion.
	t.lock()
	defer t.unlock()

	if false { // DEBUG
		fmt.Fprintf(os.Stderr, "GenericTree.Delete(%v) BEFORE deleteKey keys: %v\n", key, t.getKeys())
	}

	// NOTE: Before invoking count method, we know we can return without
	// combining nodes when deleteKey returns true. If deleteKey returns
	// false, then root node no longer has the minimum number of items.
	enough := t.root.deleteKey(t.minSize, key)

	if false { // DEBUG
		fmt.Fprintf(os.Stderr, "GenericTree.Delete(%v) AFTER deleteKey enough=%t keys: %v\n", key, enough, t.getKeys())
	}

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
		// There is no way get here unless bug in library.
		panic(fmt.Errorf("BUG: GOT: %#v; WANT: internalNode | leafNode", t.root))
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
	return keys
}

// Insert inserts the key-value pair into the tree, replacing the existing
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
	const debug = false

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
		if debug {
			fmt.Fprintf(os.Stderr, "TOP OF LOOP\n")
			fmt.Fprintf(os.Stderr, "source node remaining: %v\n", sourceLeaf.Runts[sourceCopyOffset:])
			fmt.Fprintf(os.Stderr, "target node so far:    %v\n", targetLeaf.Runts[:targetCopyOffset])
		}

		space := count - targetCopyOffset // space is how much more slots available in target leaf

		if space == 0 {
			// Shorten the length of the slices to the actual number of
			// elements copied from source leaves.
			targetLeaf.Runts = targetLeaf.Runts[:targetCopyOffset]
			targetLeaf.Values = targetLeaf.Values[:targetCopyOffset]
			targetCopyOffset = 0

			if debug {
				fmt.Fprintf(os.Stderr, "FINISHED TARGET LEAF: %v\n", targetLeaf.Runts)
			}

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

		if debug {
			fmt.Fprintf(os.Stderr, "target space remaining: %d\n", space)
		}

		runtsCopied := copy(targetLeaf.Runts[targetCopyOffset:count], sourceLeaf.Runts[sourceCopyOffset:])
		valuesCopied := copy(targetLeaf.Values[targetCopyOffset:count], sourceLeaf.Values[sourceCopyOffset:])

		if runtsCopied != valuesCopied {
			panic(fmt.Errorf("BUG: copied different number of runts and values: %d != %d", runtsCopied, valuesCopied))
		}

		sourceCopyOffset += runtsCopied
		targetCopyOffset += runtsCopied

		if debug {
			fmt.Fprintf(os.Stderr, "copy(targetleaf.Runts[%d:%d], sourceLeaf.Runts[%d:]) -> %d items copied\n", targetCopyOffset, count, sourceCopyOffset, runtsCopied)

			fmt.Fprintf(os.Stderr, "target after: len=%d cap=%d %v\n", len(targetLeaf.Runts), cap(targetLeaf.Runts), targetLeaf.Runts[:targetCopyOffset])
		}

		// POST: either target is full, or source is empty
		if targetCopyOffset > count {
			panic(fmt.Errorf("BUG: copied too many runts to target leaf: %d", targetCopyOffset))
		}

		if sourceCopyOffset > len(sourceLeaf.Runts) {
			panic(fmt.Errorf("BUG: source copy offset > len source runts: %d > %d", sourceCopyOffset, len(sourceLeaf.Runts)))
		}

		if sourceCopyOffset == len(sourceLeaf.Runts) {
			if debug {
				fmt.Fprintf(os.Stderr, "source copy offset == len source runts: %d == %d (need new source node)\n", sourceCopyOffset, len(sourceLeaf.Runts))
			}

			// Advance to next leaf node, locking it first, then unlocking the
			// current node before advancement.
			sourceLeafNext := sourceLeaf.Next
			if sourceLeafNext != nil {
				if debug {
					fmt.Fprintf(os.Stderr, "found another source leaf node\n")
				}
				sourceCopyOffset = 0
				sourceLeafNext.rlock()
			} else {
				if debug {
					fmt.Fprintf(os.Stderr, "did not find another source leaf node\n")
				}
			}
			sourceLeaf.runlock()
			sourceLeaf = sourceLeafNext
		} else if debug {
			fmt.Fprintf(os.Stderr, "source copy offset < len source runts: %d < %d (need new target node)\n", sourceCopyOffset, len(sourceLeaf.Runts))
		}
	}

	if targetCopyOffset > 0 {
		targetLeaf.Runts = targetLeaf.Runts[:targetCopyOffset]
		targetLeaf.Values = targetLeaf.Values[:targetCopyOffset]
		if debug {
			fmt.Fprintf(os.Stderr, "FINISHED LEAF: %v\n", targetLeaf.Runts)
		}
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
		if debug {
			for _, n := range bottomNodes {
				fmt.Fprintf(os.Stderr, "BOTTOM NODE: %v\n", n)
			}
		}
		for _, bottomNode := range bottomNodes {
			if len(internal.Runts) == count {
				if debug {
					fmt.Fprintf(os.Stderr, "FINISHED INTERNAL A: %v\n", internal)
					for _, c := range internal.Children {
						fmt.Fprintf(os.Stderr, "\tCHILD: %v\n", c)
					}
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
			if debug {
				fmt.Fprintf(os.Stderr, "FINISHED INTERNAL B: %v\n", internal)
				for _, c := range internal.Children {
					fmt.Fprintf(os.Stderr, "\tCHILD: %v\n", c)
				}
			}
			topNodes = append(topNodes, internal)
			internal = &internalNode[K, V]{
				Runts:    make([]K, 0, t.order),
				Children: make([]node[K, V], 0, t.order),
			}
		}
		if debug {
			for _, n := range topNodes {
				fmt.Fprintf(os.Stderr, "TOP NODE: %v\n", n)
			}
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

// Search returns the value associated with key from the tree.
func (t *GenericTree[K, V]) Search(key K) (V, bool) {
	var value V
	var ok bool

	t.rlock()
	defer t.runlock()

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

	// Because updating the tree may change the tree's pointer to the root
	// node, first acquire an exclusive lock to the tree.
	t.lock()

	// Because there might be another goroutine that is visiting internal and
	// leaf nodes, we need to acquire exclusive lock on each node we visit
	// because we might need to update that node.
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
	defer t.unlock()

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
	t.rlock()   // Before can load root field must acquire read lock
	n := t.root // Load pointer to root of tree
	t.runlock() // Release read lock on tree

	n.rlock() // Acquire read lock to root node.

	for {
		switch tv := n.(type) {
		case *internalNode[K, V]:
			// Next node to visit is the child node
			child := tv.Children[searchLessThanOrEqualTo(key, tv.Runts)]
			child.rlock() // Acquire read lock for the child node
			tv.runlock()  // Release read lock for this node
			n = child
		case *leafNode[K, V]:
			tv.rlock()
			// NOTE: lock was acquired either above when at this leaf's
			// parent, or if n was a leaf, before this loop.
			//
			// NOTE: The read lock for the leaf node will be released when
			// scanner is closed.
			return newGenericCursor(tv, searchGreaterThanOrEqualTo(key, tv.Runts))
		default:
			panic(fmt.Errorf("GOT: %#v; WANT: node", n))
		}
	}
}

// findAndLockFirstLeaf walks the tree to the first leaf node, acquires a read
// lock, then returns it.
//
// NOTE: Must have either read or exclusive lock for n.
func (t *GenericTree[K, V]) findAndLockFirstLeaf(n node[K, V]) *leafNode[K, V] {
	for {
		switch tv := n.(type) {
		case *internalNode[K, V]:
			// panic("HERE")
			child := tv.Children[0] // Next node to visit is the child node
			child.rlock()           // Acquire read lock for the child node
			tv.runlock()            // Release read lock for this node
			n = child
		case *leafNode[K, V]:
			// panic("THERE")
			// NOTE: lock was acquired either above when at this leaf's
			// parent, or if n was a leaf, before this method was invoked.
			return tv
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
	t.rlock()   // Before can load root field must acquire read lock
	n := t.root // Load pointer to root of tree
	t.runlock() // Release read lock on tree

	n.rlock() // Acquire read lock to node.

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
// pairs in the tree. It is necessary to invoke this method in order to
// release the lock the cursor holds on one of the leaf nodes in the tree.
func (c *GenericCursor[K, V]) Close() error {
	if c.leaf == nil {
		return errors.New("cannot Close a closed Scanner")
	}
	c.leaf.runlock()
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
	c.index++

	if c.index == len(c.leaf.Runts) {
		n := c.leaf.Next
		if n == nil {
			return false
		}
		n.rlock()
		c.leaf.runlock()
		c.leaf = n
		c.index = 0
	}

	return true
}
