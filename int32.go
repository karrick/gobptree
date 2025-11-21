package gobptree

// Int32Tree is a B+Tree of elements using Int32 keys.
type Int32Tree struct {
	g *GenericTree[int32, any]
}

// NewInt32Tree returns a newly initialized Int32Tree of the specified
// order.
func NewInt32Tree(order int) (*Int32Tree, error) {
	tree, err := NewGenericTree[int32, any](order)
	if err != nil {
		return nil, err
	}
	return &Int32Tree{tree}, nil
}

// Delete removes the key-value pair from the tree.
func (t *Int32Tree) Delete(key int32) {
	t.g.Delete(key)
}

// Insert inserts the key-value pair into the tree, replacing the existing
// value with the new value if the key is already in the tree.
func (t *Int32Tree) Insert(key int32, value any) {
	t.g.Insert(key, value)
}

// Search returns the value associated with key from the tree.
func (t *Int32Tree) Search(key int32) (any, bool) {
	return t.g.Search(key)
}

// Update searches for key and invokes callback with key's associated value,
// waits for callback to return a new value, and stores callback's return
// value as the new value for key. When key is not found, callback will be
// invoked with nil and false to signify the key was not found. After this
// method returns, the key will exist in the tree with the new value returned
// by the callback function.
func (t *Int32Tree) Update(key int32, callback func(any, bool) any) {
	t.g.Update(key, func(value any, ok bool) (any, error) {
		v := callback(value, ok)
		return v, nil
	})
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
func (t *Int32Tree) NewScanner(key int32) *Int32Cursor {
	return &Int32Cursor{t.g.NewScanner(key)}
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
func (t *Int32Tree) NewScannerAll() *Int32Cursor {
	return &Int32Cursor{t.g.NewScannerAll()}
}

// Int32Cursor is used to enumerate key-value pairs from the tree in
// ascending order.
type Int32Cursor struct {
	g *GenericCursor[int32, any]
}

// Close releases the lock on the leaf node under the cursor. This method is
// provided to signal no further intention of scanning the remainder key-value
// pairs in the tree. It is necessary to invoke this method in order to
// release the lock the cursor holds on one of the leaf nodes in the tree.
func (c *Int32Cursor) Close() error { return c.g.Close() }

// Pair returns the key-value pair referenced by the cursor. This method will
// panic when invoked before invoking the Scan method at least once.
func (c *Int32Cursor) Pair() (int32, any) { return c.g.Pair() }

// Scan advances the cursor to reference the next key-value pair in the tree
// in ascending order, and returns true when there is at least one more
// key-value pair to be observed with the Pair method. If the final key-value
// pair has already been observed, this unlocks the final leaf in the tree and
// returns false. This method must be invoked at least once before invoking
// the Pair method.
func (c *Int32Cursor) Scan() bool { return c.g.Scan() }
