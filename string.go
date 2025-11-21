package gobptree

// StringTree is a B+Tree of elements using string keys.
type StringTree struct {
	g *GenericTree[string, any]
}

// NewStringTree returns a newly initialized StringTree of the specified
// order.
func NewStringTree(order int) (*StringTree, error) {
	tree, err := NewGenericTree[string, any](order)
	if err != nil {
		return nil, err
	}
	return &StringTree{tree}, nil
}

// Delete removes the key-value pair from the tree.
func (t *StringTree) Delete(key string) {
	t.g.Delete(key)
}

// Insert inserts the key-value pair into the tree, replacing the existing
// value with the new value if the key is already in the tree.
func (t *StringTree) Insert(key string, value any) {
	t.g.Insert(key, value)
}

// Search returns the value associated with key from the tree.
func (t *StringTree) Search(key string) (any, bool) {
	return t.g.Search(key)
}

// Update searches for key and invokes callback with key's associated value,
// waits for callback to return a new value, and stores callback's return
// value as the new value for key. When key is not found, callback will be
// invoked with nil and false to signify the key was not found. After this
// method returns, the key will exist in the tree with the new value returned
// by the callback function.
func (t *StringTree) Update(key string, callback func(any, bool) any) {
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
func (t *StringTree) NewScanner(key string) *StringCursor {
	return &StringCursor{t.g.NewScanner(key)}
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
func (t *StringTree) NewScannerAll() *StringCursor {
	return &StringCursor{t.g.NewScannerAll()}
}

// StringCursor is used to enumerate key-value pairs from the tree in
// ascending order.
type StringCursor struct {
	g *GenericCursor[string, any]
}

// Close releases the lock on the leaf node under the cursor. This method is
// provided to signal no further intention of scanning the remainder key-value
// pairs in the tree. It is necessary to invoke this method in order to
// release the lock the cursor holds on one of the leaf nodes in the tree.
func (c *StringCursor) Close() error { return c.g.Close() }

// Pair returns the key-value pair referenced by the cursor. This method will
// panic when invoked before invoking the Scan method at least once.
func (c *StringCursor) Pair() (string, any) { return c.g.Pair() }

// Scan advances the cursor to reference the next key-value pair in the tree
// in ascending order, and returns true when there is at least one more
// key-value pair to be observed with the Pair method. If the final key-value
// pair has already been observed, this unlocks the final leaf in the tree and
// returns false. This method must be invoked at least once before invoking
// the Pair method.
func (c *StringCursor) Scan() bool { return c.g.Scan() }
