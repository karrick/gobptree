package gobptree

import (
	"fmt"
	"sync"
)

// stringSearchGreaterThanOrEqualTo returns the index of the first value from
// values that is greater than or equal to key.
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

// stringNode represents either an internal or a leaf node for a tree using
// string keys.
type stringNode interface {
	IsInternal() bool
	MaybeSplit(order int) (stringNode, stringNode)
	Smallest() string
	Lock()
	Unlock()
}

// stringInternalNode represents either an internal node for a tree using string
// keys.
type stringInternalNode struct {
	runts []string

	// children[i] points to child node with values greater than or equal to
	// runts[i]
	children []stringNode

	lock sync.Mutex
}

func (i *stringInternalNode) IsInternal() bool { return true }

func (i *stringInternalNode) Lock() { i.lock.Lock() }

func (i *stringInternalNode) Unlock() { i.lock.Unlock() }

// MaybeSplit splits the node, giving half of its values to its new sibling,
// when the node is too full to accept any more values.
//
// NOTE: This loop assumes the tree's order is a multiple of 2, which must be
// guarded for at tree instantiation time.
func (i *stringInternalNode) MaybeSplit(order int) (stringNode, stringNode) {
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

func (i *stringInternalNode) Smallest() string {
	if len(i.runts) == 0 {
		panic("internal node has no children")
	}
	return i.runts[0]
}

// stringLeafNode represents either a leaf node for a tree using string keys.
type stringLeafNode struct {
	runts  []string
	values []interface{}
	next   *stringLeafNode // points to next leaf to allow enumeration
	lock   sync.Mutex
}

func (l *stringLeafNode) IsInternal() bool { return false }

func (l *stringLeafNode) Lock() { l.lock.Lock() }

func (l *stringLeafNode) Unlock() { l.lock.Unlock() }

// MaybeSplit splits the node, giving half of its values to its new sibling,
// when the node is too full to accept any more values.
//
// NOTE: This loop assumes the tree's order is a multiple of 2, which must be
// guarded for at tree instantiation time.
func (l *stringLeafNode) MaybeSplit(order int) (stringNode, stringNode) {
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

func (l *stringLeafNode) Smallest() string {
	if len(l.runts) == 0 {
		panic("leaf node has no children")
	}
	return l.runts[0]
}

// StringTree is a B+Tree of elements using string keys.
type StringTree struct {
	root  stringNode
	order int
}

// NewStringTree returns a newly initialized StringTree of the specified order.
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

// Insert inserts the key-value pair into the tree, replacing the existing value
// with the new value if the key is already in the tree.
func (t *StringTree) Insert(key string, value interface{}) {
	n := t.root
	n.Lock()

	for n.IsInternal() {
		parent := n.(*stringInternalNode)
		index := stringSearchLessThanOrEqualTo(key, parent.runts)
		child := parent.children[index]
		child.Lock()

		if index == 0 {
			if smallest := child.Smallest(); key < smallest {
				// preemptively update smallest
				parent.runts[0] = key
			}
		}

		c, s := child.MaybeSplit(t.order)
		if s != nil {
			// insert sibling to the right of current node
			parent.runts = append(parent.runts, "")
			parent.children = append(parent.children, nil)
			copy(parent.runts[index+2:], parent.runts[index+1:])
			copy(parent.children[index+2:], parent.children[index+1:])
			sSmallest := s.Smallest()
			parent.children[index+1] = s
			// decide whether we need to go to original child or sibling
			if key < sSmallest {
				child = c
			} else {
				child.Unlock() // release lock on child
				s.Lock()       // and grab lock on its new sibling
				if key < sSmallest {
					sSmallest = key
				}
				child = s
			}
			parent.runts[index+1] = sSmallest
		}
		// POST: tail end recursion to intended child
		parent.Unlock() // release lock on this node before go to child locked above
		n = child
	}
	// POST: at bottom level, which is a leaf node
	ln := n.(*stringLeafNode)

	c, s := ln.MaybeSplit(t.order)
	if s != nil {
		// Only possible to get here if the root is a full leaf, because if
		// there were a parent node, it would have already split this node when
		// it was the parent's child.
		cSmallest := c.Smallest()
		sSmallest := s.Smallest()
		if key < cSmallest {
			cSmallest = key
		}
		t.root = &stringInternalNode{
			runts:    []string{cSmallest, sSmallest},
			children: []stringNode{c, s},
		}
		if key < sSmallest {
			ln = c.(*stringLeafNode)
		} else {
			ln.Unlock() // release lock on previous leaf
			ln = s.(*stringLeafNode)
			ln.Lock() // acquire lock on leaf's new sibling
		}
	}

	// When the new value will become the first element in a leaf, which is only
	// possible for an empty tree, or when new key comes after final leaf runt,
	// a simple append will suffice.
	if len(ln.runts) == 0 || key > ln.runts[len(ln.runts)-1] {
		ln.runts = append(ln.runts, key)
		ln.values = append(ln.values, value)
		ln.Unlock()
		return
	}

	index := stringSearchGreaterThanOrEqualTo(key, ln.runts)

	if ln.runts[index] == key {
		// When the key matches the runt, merely need to update the value.
		ln.values[index] = value
		ln.Unlock()
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
	ln.Unlock()
}

// Search returns the value associated with key from the tree.
func (t *StringTree) Search(key string) (interface{}, bool) {
	var value interface{}
	var ok bool
	n := t.root
	n.Lock()
	for n.IsInternal() {
		parent := n.(*stringInternalNode)
		child := parent.children[stringSearchLessThanOrEqualTo(key, parent.runts)]
		child.Lock()
		parent.Unlock()
		n = child
	}
	l := n.(*stringLeafNode)
	i := stringSearchGreaterThanOrEqualTo(key, l.runts)
	if l.runts[i] == key {
		value = l.values[i]
		ok = true
	}
	l.Unlock()
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
	n.Lock()

	for n.IsInternal() {
		parent := n.(*stringInternalNode)
		index := stringSearchLessThanOrEqualTo(key, parent.runts)
		child := parent.children[index]
		child.Lock()

		if index == 0 {
			if smallest := child.Smallest(); key < smallest {
				// preemptively update smallest value
				parent.runts[0] = key
			}
		}

		c, s := child.MaybeSplit(t.order)
		if s != nil {
			// insert sibling to the right of current node
			parent.runts = append(parent.runts, "")
			parent.children = append(parent.children, nil)
			copy(parent.runts[index+2:], parent.runts[index+1:])
			copy(parent.children[index+2:], parent.children[index+1:])
			sSmallest := s.Smallest()
			parent.children[index+1] = s
			// decide whether we need to go to original child or sibling
			if key < sSmallest {
				child = c
			} else {
				child.Unlock() // release lock on child
				s.Lock()       // and grab lock on its new sibling
				if key < sSmallest {
					sSmallest = key
				}
				child = s
			}
			parent.runts[index+1] = sSmallest
		}
		// POST: tail end recursion to intended child
		parent.Unlock() // release lock on this node before go to child locked above
		n = child
	}
	// POST: at bottom level, which is a leaf node
	ln := n.(*stringLeafNode)

	c, s := ln.MaybeSplit(t.order)
	if s != nil {
		// Only possible to get here if the root is a full leaf, because if
		// there were a parent node, it would have already split this node when
		// it was the parent's child.
		cSmallest := c.Smallest()
		sSmallest := s.Smallest()
		if key < cSmallest {
			cSmallest = key
		}
		t.root = &stringInternalNode{
			runts:    []string{cSmallest, sSmallest},
			children: []stringNode{c, s},
		}
		if key < sSmallest {
			ln = c.(*stringLeafNode)
		} else {
			ln.Unlock() // release lock on previous leaf
			ln = s.(*stringLeafNode)
			ln.Lock() // acquire lock on leaf's new sibling
		}
	}

	// When the new value will become the first element in a leaf, which is only
	// possible for an empty tree, or when new key comes after final leaf runt,
	// a simple append will suffice.
	if len(ln.runts) == 0 || key > ln.runts[len(ln.runts)-1] {
		value := callback(nil, false)
		ln.runts = append(ln.runts, key)
		ln.values = append(ln.values, value)
		ln.Unlock()
		return
	}

	index := stringSearchGreaterThanOrEqualTo(key, ln.runts)

	if ln.runts[index] == key {
		// When the key matches the runt, merely need to update the value.
		ln.values[index] = callback(ln.values[index], true)
		ln.Unlock()
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
	ln.Unlock()
}

// NewScanner returns a cursor that iteratively returns key-value pairs from the
// tree in ascending order starting at key, or if key is not found the next key,
// and ending after all successive pairs have been returned.
//
// NOTE: This function exists still holding the lock on one of the tree's leaf
// nodes, which may block other operations on the tree that require modification
// of the locked node. The leaf node is only unlocked either by closing the
// Cursor, or after all key-value pairs have been visited using Scan.
func (t *StringTree) NewScanner(key string) *StringCursor {
	n := t.root
	n.Lock()
	for n.IsInternal() {
		parent := n.(*stringInternalNode)
		child := parent.children[stringSearchLessThanOrEqualTo(key, parent.runts)]
		child.Lock()
		parent.Unlock()
		n = child
	}
	ln := n.(*stringLeafNode)
	return newStringCursor(ln, stringSearchGreaterThanOrEqualTo(key, ln.runts))
}

// StringCursor is used to enumerate key-value pairs from the tree in ascending
// order.
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
		c.l.Unlock()
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
			c.l.Unlock()
			c.l = nil
			return false
		}
		n := c.l.next
		n.Lock()
		c.l.Unlock()
		c.l = n
		c.i = 0
	}
	return true
}
