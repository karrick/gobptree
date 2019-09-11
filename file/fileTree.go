package gobptree

import "os"

// MinimumBlockSize specifies the minimum block size to be used when creating a
// new database.
const MinimumBlockSize = 4096

const cacheLineLength = 64 // cacheLineLength is number of bytes in a cache line
const fileOrder = 64       // ??? is this still needed

type FileTreeConfig struct {
	// BlockSize specifies the size of the blocks written to secondary
	// storage. The size used ought to be optimized for the type of secondary
	// storage used. For example, 512 KiB ought to be used for SSD drives, and
	// perhaps no smaller than 4 KiB ought to be used for spinning disks.
	//
	// CAUTION: Creating a new database with a block size smaller than the
	// device's native block size will result in needless wear on the device,
	// and may shorten its lifespan. However, reading a database written for a
	// device with a different block size will not adversely impact its
	// lifespan.
	BlockSize int

	// Pathname specifies the name of the secondary storage file. Required.
	Pathname string

	// FileMode specifies the OS permissions to be applied to the file when
	// creating a new database.
	FileMode os.FileMode

	NodeSize int // ???

	// Order specifies the number of elements in each node. Required.
	Order int
}

// Serializable interface allows this library to serialize and deserialize user
// defined data types. Values to be stored in the database must be Serializable.
type Serializable interface {
	MarshalBinary() func() ([]byte, error)
	UnmarshalBinary() func([]byte) error
}
