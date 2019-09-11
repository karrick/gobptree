package gobptree

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

// fileInternalNode consumes (order * 16) bytes. For example, when order is 64,
// each internal node would consume 1024 bytes. There are advantages to having
// the size of this data structure an exact factor of the chosen block size,
// however when serialized to secondary storage, we will need to write a count
// of the keys. In most cases, the smallest block size will be 4096 bytes.
type fileInternalNode struct {
	keys    []int64
	offsets []int64
}

func newFileInternalNode(order int) *fileInternalNode {
	return &fileInternalNode{
		keys:    make([]int64, 0, order), // requires 8 * 64 = 512 bytes
		offsets: make([]int64, 0, order), // requires 8 * 64 = 512 bytes
	}
}

func (fin *fileInternalNode) Append(child fileNode, smallest int64) {
	// Smallest value could be found by querying child, but I think we already
	// have it in invoking function, so just pass it into here.
	fin.keys = append(fin.keys, smallest)
	fin.offsets = append(find.offsets, child) // TODO
}

// FileInt64Tree is a B+Tree optimized for write-once, read-many scenarios,
// especially well suited for applications where secondary storage is used on
// media that limit the number of times a block may be written to.
type FileInt64Tree struct {
	fp             *os.File             // underlying file pointer
	bw             *bufio.Writer        // but write blocks via this buffered writer (might not need after all)
	leaf           *fileLeafNode        // bulk inserts go to this leaf node first
	previousKey    int64                // return error when upstream attempts to build an out of order tree
	previousOffset int64                // offset of last bulk insert
	bytesWritten   int64                // after every block written, reflects size of file
	blockSize      int                  // how many bytes in a single block
	order          int                  // how many keys per node
	internals      []*int64InternalNode // root is always final element
	pathname       string               // not needed, but handy for error messages
}

// TODO: consider merging into tree itself, since there is only one of them, and
// most of the tree level methods need access to this leaf's data, and because
// having a leaf type has required creating one-off methods just for the tree
// level to invoke.
type fileLeafNode struct {
	count     int64  // initialize: 0
	next      int64  // initialize: order<<4 + 2
	buffer    []byte // initialize: make([]byte, blockSize)
	blockSize int    // initialize: blockSize
	order     int    // initialize: max number of keys
	kOffset   int    // initialize: 16
	oOffset   int    // initialize: order<<3 + 2
}

// NewReadFileInt64Tree returns a data structure used to read values from a
// previously written FileInt64Tree.
func NewReadFileInt64Tree() (*FileInt64Tree, error) {
	return nil, errors.New("TODO")
}

// NewWriteFileInt64Tree returns a data structure used to create and write a new
// database file.
func NewWriteFileInt64Tree(c *FileTreeConfig) (*FileInt64Tree, error) {
	// TODO: ponder about how block size and order work together, and consider
	// how configured.
	if c.BlockSize < MinimumBlockSize {
		return nil, fmt.Errorf("cannot create tree when BlockSize is less than MinimumBlockSize: %d < %d", c.BlockSize, MinimumBlockSize)
	}
	if c.Order <= 0 || c.Order%2 == 1 {
		return nil, fmt.Errorf("cannot create tree when order is not a multiple of 2: %d", c.Order)
	}

	// TODO: Check NodeSize, once I remember what that is for.

	fm := c.FileMode
	if fm == 0 {
		fm = os.ModePerm
	}

	fp, err := os.OpenFile(c.Pathname, os.O_WRONLY, fm)
	if err != nil {
		return nil, err
	}

	return &FileInt64Tree{
		fp:        fp,
		bw:        bufio.NewWriterSize(fp, c.BlockSize),
		pathname:  c.Pathname,
		order:     c.Order,
		blockSize: c.BlockSize,
		internals: make([]*fileInternalNode, 0, c.Order),
		leaf:      new(fileLeafNode),
	}, nil
}

// BulkInsert is called in key order to append items to the B+Tree. It is meant
// to be called with keys in ascending order, and will return an error when
// given a key that is less than or equal to the previous key it was called
// with.
func (t *FileInt64Tree) BulkInsert(key int64, value Serializable) error {
	if key <= t.previousKey {
		return fmt.Errorf("cannot insert new value when key is less than or equal to previous key: %d <= %d", key, t.previousKey)
	}
	t.previousKey = key

	// Ask client to serialize value to a slice of bytes. It seems odd to do
	// this when client could have merely invoked this function with a slice of
	// bytes. However, the Serializable interface will also be used to
	// deserialize the data when read from disk.
	buf, err := value.MarshalBinary()
	if err != nil {
		return errors.Wrapf(err, "cannot serialize value: %v", value)
	}

	// Append new key and value to in-memory chunk of things ready to be flushed
	// to disk.
	t.leaf.Append(key, buf)

	// t.previousOffset += int64(len(buf))

	// If current in-memory leaf node is full, add a pointer to it to the
	// current in-memory internal node.
	if t.leaf.count < t.order {
		return nil
	}

	// TODO: leaf node's next field is not easily known until the next leaf is
	// written in the cases where the parent node is full. This might require a
	// complete restructure of the below algorithm. We should know how many
	// internal nodes will need to be written immediately following the writing
	// of this leaf node.

	// Write this leaf node and all of its data. Must ensure rounds size up to
	// nearest block size.
	nw, err := t.leaf.WriteTo(t.fp)
	if err != nil {
		return err
	}

	// TODO: if leaf buffer too large, then re-allocate it

	// TODO: Maintain a stack of in-memory internal nodes, all which will
	// eventually require flushing to the file once they are full.

	// TODO: add reference to this leaf node to final internal node, and bubble up.
	key := t.leaf.keys[0]
	previousOffset := t.bytesWritten

	// starting at 0th element, bubble up reference to this leaf node
	for i := 0; i < len(t.internals); i++ {
		t.internals[i].keys = append(t.internals[i].keys, key)

		// The new leaf was written at the previous end of file.
		t.internals[i].offsets = append(t.internals[i].offsets, previousOffset)

		if len(t.internals[i].keys) < t.order {
			t.bytesWritten += nw
			return nil
		}

		// Internal node is full; write it out then add it to level above; if no
		// level above, create new level above this.
		nw, err = t.internals[i].WriteTo(t.bw)
		if err != nil {
			return err
		}
		if err = t.bw.Flush(); err != nil {
			return err
		}

		key = t.internals[i].keys[0]

		// clear out previous internal node for this level
		t.internals[i].keys = t.internals[i].keys[:0]
		t.internals[i].offsets = t.internals[i].offsets[:0]
	}

	// need new root node
	newRoot := &fileInternalNode{
		keys:    make([]int64, 1, t.order),
		offsets: make([]int64, 1, t.order),
	}
	newRoot.keys[0] = key
	newRoot.offsets[0] = offset
	t.internals = append(t.internals, newRoot)

	t.bytesWritten += nw
	return nil
}

func (t *FileInt64Tree) append(key int64, serialzedValue []byte) {
	binary.BigEndian.PutInt64(t.leaf.buffer[t.leaf.kOffset:], key)
	t.leaf.kOffset += 8
	binary.BigEndian.PutInt64(t.leaf.buffer[t.leaf.oOffset:], t.leaf.next)
	t.leaf.oOffset += 8

	t.leaf.next += len(serialzedValue)
	t.leaf.count++

	// Attempt to use copy builtin to copy the entire serialized value, however
	// if the underlying buffer is not large enough, then use append builtin to
	// append the extra bytes to the buffer, growing it as necessary.
	if t.leaf.next+len(serialzedValue) > cap(t.leaf.buffer) {
		// Expand buffer to hold another block of bytes.
		t.leaf.buffer = growByteSlice(t.leaf.buffer, len(t.leaf.buffer)+t.leaf.blockSize)
	}
	_ = copy(l.buffer[l.next:], serializedValue)
}

// growByteSlice will return a byte slice with a capacity at least equal to the
// specified size.
func growByteSlice(buf []byte, need int) []byte {
	// This program will never need to grow buffer by more than double its size,
	// but this functionality might become necessary eventually.
	if need > cap(buf)<<1 {
		t := make([]byte, need)
		copy(t, buf)
		return t
	}
	for cap(buf) < need {
		buf = append(buf[:cap(buf)], 0)
	}
	return buf[:cap(buf)]
}

// WriteTo writes the leaf node to w.
func (l *fileLeafNode) WriteTo(w io.Writer) (int64, error) {
	// PRE: number of keys match number of offsets
	lenKeys := len(l.keys)
	if lenKeys != len(l.offsets) {
		return 0, fmt.Errorf("cannot write leaf when number of keys (%d) does not match number of offsets (%d)", lenKeys, len(l.offsets))
	}
	if lenKeys != len(l.serializedValues) {

		return 0, fmt.Errorf("cannot write leaf when number of keys (%d) does not match number of serialized values (%d)", lenKeys, len(l.serializedValues))
	}
	// PRE: offsetOfNextLeaf already set, otherwise cannot really write this
	// leaf yet.
	if l.offsetOfNextLeaf == 0 {
		return 0, errors.New("cannot write leaf without offset of next leaf set")
	}

	keyBuffer := make([]byte, 8*lenKeys)
	offsetBuffer := make([]byte, 8*lenKeys)
	var buf [8]byte
	var bufOffset int

	for i := 0; i < lenKeys; i++ {
		binary.BigEndian.PutInt64(keyBuffer[bufOffset:], l.keys[i])
		binary.BigEndian.PutInt64(offsetBuffer[bufOffset:], l.offsets[i])
		bufOffset += 8
	}

	binary.BigEndian.PutInt64(buf[:], l.offsetOfNextLeaf)

	var tw int64 // total bytes written

	// keys
	nw, err := w.Write(keyBuffer)
	tw += nw
	if err != nil {
		return tw, err
	}
	// offsets
	nw, err = w.Write(offsetBuffer)
	tw += nw
	if err != nil {
		return tw, err
	}
	// offset of next leaf
	nw, err = w.Write(buf[:])
	tw += nw
	if err != nil {
		return tw, err
	}
	// serialized values
	for _, sv := range l.serializedValues {
		nw, err = w.Write(sv)
		tw += nw
		if err != nil {
			return tw, err
		}
	}

	return tw, nil
}
