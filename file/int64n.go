package gobptree

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
)

type FileInt64Tree2 struct {
	fp        *os.File
	bw        *bufio.Writer // wraps file pointer
	pathname  string
	order     int
	blockSize int
	internals []*fileInternalNode
	isize     int // isize is the size of an internal node

	//
	// leaf being appended to
	//
	previousKey int64
	keys        []int64
	values      [][]byte

	valuesLength int // valuesLength is the sum of all the value lengths

	// nextLeafOffset is the calculated byte offset for next leaf node, after
	// required internal nodes have been written.
	nextLeafOffset int64

	// dirty is true when data must be written during flush prior to closing
	// database file.
	dirty bool
}

func NewFileInt64Tree2(c *FileTreeConfig) (*FileInt64Tree2, error) {
	// TODO: ponder about how block size and order work together, and consider
	// how configured.
	if c.BlockSize < MinimumBlockSize {
		return nil, fmt.Errorf("cannot create tree when BlockSize is less than MinimumBlockSize: %d < %d", c.BlockSize, MinimumBlockSize)
	}
	if c.Order <= 0 || c.Order%2 == 1 {
		return nil, fmt.Errorf("cannot create tree when order is not a multiple of 2: %d", c.Order)
	}

	fm := c.FileMode
	if fm == 0 {
		fm = os.ModePerm
	}

	fp, err := os.OpenFile(c.Pathname, os.O_WRONLY, fm)
	if err != nil {
		return nil, err
	}

	t := &FileInt64Tree{
		fp:        fp,
		bw:        bufio.NewWriterSize(fp, c.BlockSize),
		pathname:  c.Pathname,
		order:     c.Order,
		blockSize: c.BlockSize,
		internals: append(nil, newFileInternalNode(c.Order)),
		isize:     (c.Order << 4) + 8, // ??? order * (8 bytes per order) * (2 per keys and offsets) + 8 for length

		keys:   make([]int64, 0, c.Order),
		values: make([][]byte, 0, c.Order),
	}
	return t, nil
}

func (t *FileInt64Tree2) BulkInsert(key int64, value Serializable) error {
	// Ensure key greater than previous key
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
		return errors.Wrapf(err, "cannot serialize value for key: %q; %v", key, value)
	}

	// Append datum
	t.keys = append(t.keys, key)
	t.values = append(t.values, buf)
	t.valuesLength += len(buf)

	t.dirty = true // Mark tree as dirty every time data is appended to it

	// If not full, return
	if len(t.keys) < t.order {
		return
	}
	oneFewerThanFull := t.order - 1

	// Count number of internal nodes that must be written. ??? what about while
	// first leaf being accumulated?
	var icount int
	for i := 0; i < len(t.internals); i++ {
		if len(t.internals[i].keys) < oneFewerThanFull {
			break
		}
		icount++
	}

	{
		//
		// Write leaf node
		//

		leafHeaderLength := ((len(t.keys)*3 + 2) << 3) // everything except the values: need to store 3 values, each 8 bytes, for each key, plus the count and next values
		leafLength := leafHeaderLength + t.valuesLength

		// padding to next cache-line (64-byte) boundary
		leafLength += paddingLength(leafLength, cacheLineLength)

		// ??? consider having a longer lived buffer
		buf := make([]byte, leafHeaderLength, leafLength) // write metadata into leaf header, but append values

		valueOffset := t.nextLeafOffset + leafHeaderLength
		keyBase := 16                          // initialize to byte offset for first key
		extentBase := len(t.keys)<<3 + keyBase // initialize to byte offset for first extent

		// Figure out where the next leaf will be written
		t.nextLeafOffset += leafLength       // add space for this leaf node
		t.nextLeafOffset += icount * t.isize // add space for the internal nodes that must be written

		binary.BigEndian.PutInt64(buf, len(t.keys))          // how many keys in this leaf
		binary.BigEndian.PutInt64(buf[8:], t.nextLeafOffset) // offset of the next leaf node

		for i := 0; i < len(t.keys); i++ {
			valueLength := len(t.values[i])

			binary.BigEndian.PutInt64(buf[keyBase:], t.keys[i])        // element key
			binary.BigEndian.PutInt64(buf[extentBase:], valueOffset)   // element value offset
			binary.BigEndian.PutInt64(buf[extentBase+8:], valueLength) // element value length
			buf = append(buf, t.values[i]...)                          // append value to buffer

			keyBase += 8
			extentBase += 16
			valueOffset += valueLength
		}

		if _, err = t.bw.Write(buf); err != nil {
			return err
		}
	}

	// Write each required internal node, updating parent internal node with
	// displacement of internal nodes written.
	for i := 0; i < icount; i++ {

	}

	// TODO: clear internal nodes after writing them out?

	// If the root was not updated, return
	if icount < len(t.internals) {
		return
	}

	t.dirty = false // after leaf and requisite internal nodes written, mark tree as clean.

	// Create new inodes, one more than previous time

}

func paddingLength(length, boundary int) int {
	return boundary - length%boundary
}
