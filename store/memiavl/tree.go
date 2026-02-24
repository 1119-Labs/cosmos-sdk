package memiavl

import (
	"crypto/sha256"
	"errors"
	"math"
	"sync"
)

var emptyHash = sha256.New().Sum(nil)

// Tree is an in-memory IAVL tree with copy-on-write versioning.
type Tree struct {
	version        uint32
	root           Node
	initialVersion uint32
	cowVersion     uint32
	cachedRootHash []byte // set by RootHash(), cleared on mutation
	mtx            sync.RWMutex
	snapshot       *Snapshot // non-nil when loaded from a snapshot (Phase R)
}

// NewEmptyTree creates an empty tree at an arbitrary version.
func NewEmptyTree(version uint64, initialVersion uint32) *Tree {
	if version >= math.MaxUint32 {
		panic("version overflows uint32")
	}
	return &Tree{
		version:        uint32(version),
		initialVersion: initialVersion,
	}
}

// New creates an empty tree at genesis version.
func New() *Tree {
	return NewEmptyTree(0, 0)
}

// NewWithInitialVersion creates an empty tree with an initial version.
func NewWithInitialVersion(initialVersion uint32) *Tree {
	return NewEmptyTree(0, initialVersion)
}

// NewFromSnapshot creates a tree backed by a persisted snapshot.
// The snapshot's nodes are accessed via mmap zero-copy reads.
func NewFromSnapshot(snapshot *Snapshot) *Tree {
	tree := &Tree{
		version:  snapshot.version,
		snapshot: snapshot,
	}
	if !snapshot.IsEmpty() {
		root := snapshot.RootNode()
		tree.root = root
	}
	return tree
}

func (t *Tree) IsEmpty() bool {
	return t.root == nil
}

// SetInitialVersion sets the initial version for the tree.
func (t *Tree) SetInitialVersion(initialVersion int64) error {
	if initialVersion < 0 || initialVersion > math.MaxUint32 {
		return errors.New("version overflows uint32")
	}
	t.initialVersion = uint32(initialVersion)
	return nil
}

// Copy returns a snapshot of the tree that won't be modified by further changes.
// The returned tree can be accessed concurrently with the original.
func (t *Tree) Copy() *Tree {
	t.mtx.RLock()
	defer t.mtx.RUnlock()
	if _, ok := t.root.(*MemNode); ok {
		// protect existing MemNodes from in-place modification
		t.cowVersion = t.version
	}
	return &Tree{
		version:        t.version,
		root:           t.root,
		initialVersion: t.initialVersion,
		cowVersion:     t.cowVersion,
		snapshot:       t.snapshot,
	}
}

// Close releases resources associated with the tree (e.g., mmap handles).
func (t *Tree) Close() error {
	if t.snapshot != nil {
		err := t.snapshot.Close()
		t.snapshot = nil
		return err
	}
	return nil
}

// set is the internal implementation without locking.
func (t *Tree) set(key, value []byte) {
	if value == nil {
		value = []byte{}
	}
	// Clone key/value so the tree owns its data. Callers may pass ephemeral
	// buffers (e.g., arena-allocated slices in Block-STM) that get recycled.
	key = cloneBytes(key)
	value = cloneBytes(value)
	t.cachedRootHash = nil // invalidate hash cache
	t.root, _ = setRecursive(t.root, key, value, t.version+1, t.cowVersion)
}

// remove is the internal implementation without locking.
func (t *Tree) remove(key []byte) {
	t.cachedRootHash = nil // invalidate hash cache
	_, t.root, _ = removeRecursive(t.root, key, t.version+1, t.cowVersion)
}

// Set inserts or updates a key-value pair.
func (t *Tree) Set(key, value []byte) {
	t.mtx.Lock()
	defer t.mtx.Unlock()
	t.set(key, value)
}

// Remove deletes a key from the tree.
func (t *Tree) Remove(key []byte) {
	t.mtx.Lock()
	defer t.mtx.Unlock()
	t.remove(key)
}

// SaveVersion increments the version and optionally computes hashes.
func (t *Tree) SaveVersion(updateHash bool) ([]byte, int64, error) {
	if t.version >= math.MaxUint32 {
		return nil, 0, errors.New("version overflows uint32")
	}

	var hash []byte
	if updateHash {
		hash = t.RootHash()
	}

	t.version = nextVersionU32(t.version, t.initialVersion)
	return hash, int64(t.version), nil
}

// Version returns the current tree version.
func (t *Tree) Version() int64 {
	return int64(t.version)
}

// RootHash computes and returns the root hash.
// Uses batch hashing (Phase O) to hash all dirty nodes with a single reused hasher.
func (t *Tree) RootHash() []byte {
	t.mtx.RLock()
	defer t.mtx.RUnlock()
	if t.cachedRootHash != nil {
		return t.cachedRootHash
	}
	if t.root == nil {
		return emptyHash
	}
	hashParallelSubtrees(t.root)
	t.cachedRootHash = t.root.SafeHash()
	return t.cachedRootHash
}

// Get returns the value for a key, or nil if not found.
func (t *Tree) Get(key []byte) []byte {
	t.mtx.RLock()
	defer t.mtx.RUnlock()
	if t.root == nil {
		return nil
	}
	value, _ := t.root.Get(key)
	return value
}

// Has returns whether a key exists in the tree.
func (t *Tree) Has(key []byte) bool {
	return t.Get(key) != nil
}

// Iterator returns an ascending or descending iterator over [start, end).
func (t *Tree) Iterator(start, end []byte, ascending bool) *Iterator {
	t.mtx.RLock()
	defer t.mtx.RUnlock()
	return NewIterator(start, end, ascending, t.root)
}

// UnsafeIterator returns an iterator where Key()/Value() return references
// without cloning. For Block-STM use where MVKVStore handles cloning.
func (t *Tree) UnsafeIterator(start, end []byte, ascending bool) *Iterator {
	t.mtx.RLock()
	defer t.mtx.RUnlock()
	return NewUnsafeIterator(start, end, ascending, t.root)
}

// Size returns the number of keys in the tree.
func (t *Tree) Size() int64 {
	t.mtx.RLock()
	defer t.mtx.RUnlock()
	if t.root == nil {
		return 0
	}
	return t.root.Size()
}

// nextVersionU32 is compatible with the cosmos/iavl version increment logic.
func nextVersionU32(v uint32, initialVersion uint32) uint32 {
	if v == 0 && initialVersion > 1 {
		return initialVersion
	}
	return v + 1
}
