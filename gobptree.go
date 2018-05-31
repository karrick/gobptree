package gobptree

import (
	"fmt"
	"sync"
)

type Comparable interface {
	Less(interface{}) bool
	Greater(interface{}) bool
	ZeroValue() Comparable
}

type String string

func (a String) Less(b interface{}) bool {
	bs, ok := b.(String)
	return ok && string(a) < string(bs)
}

func (a String) Greater(b interface{}) bool {
	bs, ok := b.(String)
	return ok && string(a) > string(bs)
}

func (_ String) ZeroValue() Comparable { return String("") }

type Int int

func (a Int) Less(b interface{}) bool {
	bi, ok := b.(Int)
	return ok && int(a) < int(bi)
}

func (a Int) Greater(b interface{}) bool {
	bi, ok := b.(Int)
	return ok && int(a) > int(bi)
}

func (_ Int) ZeroValue() Comparable { return Int(0) }

// searchGreaterThanOrEqualTo returns the index of the first value from values
// that is greater than or equal to key.
func searchGreaterThanOrEqualTo(key Comparable, values []Comparable) int {
	// search for index of runt that is greater than or equal to key
	var low int
	var high = len(values) - 1
	for low < high {
		index := (low + high) >> 1
		value := values[index]
		// fmt.Fprintf(os.Stderr, "key: %v; low: %d; high: %d; index: %d; value: %v\n", key, low, high, index, value)
		if key.Less(value) {
			// fmt.Fprintf(os.Stderr, "key < value: %v < %v\n", key, value)
			high = index
		} else if key.Greater(value) {
			// fmt.Fprintf(os.Stderr, "key > value: %v > %v; %T > %T\n", key, value, key, value)
			low = index + 1
		} else {
			// fmt.Fprintf(os.Stderr, "key = value: %v = %v; %T = %T\n", key, value, key, value)
			return index
		}
	}
	return low
}

func searchLessThanOrEqualTo(key Comparable, values []Comparable) int {
	index := searchGreaterThanOrEqualTo(key, values)
	// convert result to less than or equal to
	if index == len(values) || key.Less(values[index]) {
		if index > 0 {
			return index - 1
		}
	}
	return index
}

// node represents either an internal or a leaf node
type node interface {
	IsInternal() bool
	MaybeSplit(order int) (node, node)
	Smallest() Comparable
	Lock()
	Unlock()
}

// internal node is an in-memory internal node
type internal struct {
	runts []Comparable

	// children[i] points to child node with values greater than or equal to runts[i]
	children []node

	lock sync.Mutex
}

func (i *internal) IsInternal() bool { return true }

func (i *internal) Lock() { i.lock.Lock() }

func (i *internal) Unlock() { i.lock.Unlock() }

// MaybeSplit splits the node, giving half of its values to its new sibling,
// when the node is too full to accept any more values.
//
// NOTE: This loop assumes the tree's order is a multiple of 2, which must be
// guarded for at tree instantiation time.
func (i *internal) MaybeSplit(order int) (node, node) {
	if len(i.runts) < order {
		return i, nil
	}
	newNodeRunts := order >> 1
	sibling := &internal{
		runts:    make([]Comparable, newNodeRunts, order),
		children: make([]node, newNodeRunts, order),
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

func (i *internal) Smallest() Comparable {
	if len(i.runts) == 0 {
		panic("internal node has no children")
	}
	return i.runts[0]
}

// leaf represents a node that contains keys and values
type leaf struct {
	runts  []Comparable
	values []interface{}
	next   *leaf // points to next leaf to allow enumeration
	lock   sync.Mutex
}

func (l *leaf) IsInternal() bool { return false }

func (l *leaf) Lock() { l.lock.Lock() }

func (l *leaf) Unlock() { l.lock.Unlock() }

// MaybeSplit splits the node, giving half of its values to its new sibling,
// when the node is too full to accept any more values.
//
// NOTE: This loop assumes the tree's order is a multiple of 2, which must be
// guarded for at tree instantiation time.
func (l *leaf) MaybeSplit(order int) (node, node) {
	if len(l.runts) < order {
		return l, nil
	}
	newNodeRunts := order >> 1
	sibling := &leaf{
		runts:  make([]Comparable, newNodeRunts, order),
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

func (l *leaf) Smallest() Comparable {
	if len(l.runts) == 0 {
		panic("leaf node has no children")
	}
	return l.runts[0]
}

type tree struct {
	root  node
	order int
}

func NewTree(order int) (*tree, error) {
	if order <= 0 || order%2 == 1 {
		return nil, fmt.Errorf("cannot create tree when order is not a multiple of 2: %d", order)
	}
	return &tree{
		root: &leaf{
			runts:  make([]Comparable, 0, order),
			values: make([]interface{}, 0, order),
		},
		order: order,
	}, nil
}

func (t *tree) Insert(key Comparable, value interface{}) {
	n := t.root
	n.Lock()

	for n.IsInternal() {
		parent := n.(*internal)
		index := searchLessThanOrEqualTo(key, parent.runts)
		child := parent.children[index]
		child.Lock()

		if index == 0 {
			if smallest := child.Smallest(); key.Less(smallest) {
				// preemptively update smallest
				parent.runts[0] = key
			}
		}

		c, s := child.MaybeSplit(t.order)
		if s != nil {
			// insert sibling to the right of current node
			parent.runts = append(parent.runts, key.ZeroValue())
			parent.children = append(parent.children, nil)
			copy(parent.runts[index+2:], parent.runts[index+1:])
			copy(parent.children[index+2:], parent.children[index+1:])
			sSmallest := s.Smallest()
			parent.children[index+1] = s
			// decide whether we need to go to original child or sibling
			if key.Less(sSmallest) {
				child = c
			} else {
				child.Unlock() // release lock on child
				s.Lock()       // and grab lock on its new sibling
				if key.Less(sSmallest) {
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
	ln := n.(*leaf)

	c, s := ln.MaybeSplit(t.order)
	if s != nil {
		// Only possible to get here if the root is a full leaf, because if
		// there were a parent node, it would have already split this node when
		// it was the parent's child.
		cSmallest := c.Smallest()
		sSmallest := s.Smallest()
		if key.Less(cSmallest) {
			cSmallest = key
		}
		t.root = &internal{
			runts:    []Comparable{cSmallest, sSmallest},
			children: []node{c, s},
		}
		if key.Less(sSmallest) {
			ln = c.(*leaf)
		} else {
			ln.Unlock() // release lock on previous leaf
			ln = s.(*leaf)
			ln.Lock() // acquire lock on leaf's new sibling
		}
	}

	// When the new value will become the first element in a leaf, which is only
	// possible for an empty tree, or when new key comes after final leaf runt,
	// a simple append will suffice.
	if len(ln.runts) == 0 || key.Greater(ln.runts[len(ln.runts)-1]) {
		ln.runts = append(ln.runts, key)
		ln.values = append(ln.values, value)
		ln.Unlock()
		return
	}

	index := searchGreaterThanOrEqualTo(key, ln.runts)

	if ln.runts[index] == key {
		// When the key matches the runt, merely need to update the value.
		ln.values[index] = value
		ln.Unlock()
		return
	}

	// Make room for and insert the new key-value pair into leaf.

	// Append zero values to make room in arrays
	ln.runts = append(ln.runts, key.ZeroValue())
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
func (t *tree) Search(key Comparable) (interface{}, bool) {
	var value interface{}
	var ok bool
	n := t.root
	n.Lock()
	for n.IsInternal() {
		parent := n.(*internal)
		child := parent.children[searchLessThanOrEqualTo(key, parent.runts)]
		child.Lock()
		parent.Unlock()
		n = child
	}
	l := n.(*leaf)
	i := searchGreaterThanOrEqualTo(key, l.runts)
	if l.runts[i] == key {
		value = l.values[i]
		ok = true
	}
	l.Unlock()
	return value, ok
}

// NewScanner returns a Cursor that iteratively returns key-value pairs from the
// tree in ascending order starting at key, or if key is not found the next key,
// and ending after all successive pairs have been returned.
//
// NOTE: This function exists still holding the lock on one of the tree's leaf
// nodes, which may block other operations on the tree that require modification
// of the locked node. The leaf node is only unlocked either by closing the
// Cursor, or after all key-value pairs have been visited using Scan.
func (t *tree) NewScanner(key Comparable) *Cursor {
	n := t.root
	n.Lock()
	for n.IsInternal() {
		parent := n.(*internal)
		child := parent.children[searchLessThanOrEqualTo(key, parent.runts)]
		child.Lock()
		parent.Unlock()
		n = child
	}
	ln := n.(*leaf)
	return newCursor(ln, searchGreaterThanOrEqualTo(key, ln.runts))
}

// Cursor is used to enumerate key-value pairs from the tree in ascending order.
type Cursor struct {
	l *leaf
	i int
}

func newCursor(l *leaf, i int) *Cursor {
	// Initialize cursor with index one smaller than requested, so initial scan
	// lines up the cursor to reference the desired key-value pair.
	return &Cursor{l: l, i: i - 1}
}

// Close releases the lock on the leaf node under the cursor. This method is
// provided to signal no further intention of scanning the remainder key-value
// pairs in the tree. It is not necessary to call Close if Scan is called
// repeatedly until Scan returns false.
func (c *Cursor) Close() error {
	if c.l != nil {
		c.l.Unlock()
		c.l = nil
	}
	return nil
}

// Pair returns the key-value pair referenced by the Cursor.
func (c *Cursor) Pair() (Comparable, interface{}) {
	return c.l.runts[c.i], c.l.values[c.i]
}

// Scan advances the cursor to reference the next key-value pair in the tree in
// ascending order, and returns true when there is at least one more key-value
// pair to be observed with the Pair method. If the final key-value pair has
// already been observed, this unlocks the final leaf in the tree and returns false.
func (c *Cursor) Scan() bool {
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
