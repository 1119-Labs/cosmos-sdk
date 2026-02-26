package memiavl

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const (
	// SnapshotFileMagic is little-endian encoded b"IAVL".
	SnapshotFileMagic = 1280721225

	// SnapshotFormat is the current snapshot format version.
	SnapshotFormat = 0

	// SizeMetadata is magic(4) + format(4) + version(4).
	SizeMetadata = 12

	FileNameNodes    = "nodes"
	FileNameLeaves   = "leaves"
	FileNameKVs      = "kvs"
	FileNameMetadata = "metadata"
)

// Snapshot manages mmap-ed files for a single tree's persisted state.
type Snapshot struct {
	nodesMap  *MmapFile
	leavesMap *MmapFile
	kvsMap    *MmapFile

	nodes  []byte
	leaves []byte
	kvs    []byte

	version uint32

	nodesLayout  Nodes
	leavesLayout Leaves

	// nil means empty snapshot.
	root *PersistedNode
}

// NewEmptySnapshot creates an empty snapshot at the given version.
func NewEmptySnapshot(version uint32) *Snapshot {
	return &Snapshot{version: version}
}

// OpenSnapshot reads metadata, validates the format, and mmaps the data files.
func OpenSnapshot(snapshotDir string) (*Snapshot, error) {
	bz, err := os.ReadFile(filepath.Join(filepath.Clean(snapshotDir), FileNameMetadata))
	if err != nil {
		return nil, err
	}
	if len(bz) != SizeMetadata {
		return nil, fmt.Errorf("wrong metadata file size, expected: %d, found: %d", SizeMetadata, len(bz))
	}

	magic := binary.LittleEndian.Uint32(bz)
	if magic != SnapshotFileMagic {
		return nil, fmt.Errorf("invalid metadata file magic: %d", magic)
	}
	format := binary.LittleEndian.Uint32(bz[4:])
	if format != SnapshotFormat {
		return nil, fmt.Errorf("unknown snapshot format: %d", format)
	}
	version := binary.LittleEndian.Uint32(bz[8:])

	var nodesMap, leavesMap, kvsMap *MmapFile
	cleanupHandles := func(origErr error) error {
		errs := []error{origErr}
		if nodesMap != nil {
			errs = append(errs, nodesMap.Close())
		}
		if leavesMap != nil {
			errs = append(errs, leavesMap.Close())
		}
		if kvsMap != nil {
			errs = append(errs, kvsMap.Close())
		}
		return errors.Join(errs...)
	}

	if nodesMap, err = NewMmap(filepath.Join(snapshotDir, FileNameNodes)); err != nil {
		return nil, cleanupHandles(err)
	}
	if leavesMap, err = NewMmap(filepath.Join(snapshotDir, FileNameLeaves)); err != nil {
		return nil, cleanupHandles(err)
	}
	if kvsMap, err = NewMmap(filepath.Join(snapshotDir, FileNameKVs)); err != nil {
		return nil, cleanupHandles(err)
	}

	nodes := nodesMap.Data()
	leaves := leavesMap.Data()
	kvs := kvsMap.Data()

	if len(nodes)%SizeNode != 0 {
		return nil, cleanupHandles(
			fmt.Errorf("corrupted snapshot, nodes file size %d is not a multiple of %d", len(nodes), SizeNode),
		)
	}
	if len(leaves)%SizeLeaf != 0 {
		return nil, cleanupHandles(
			fmt.Errorf("corrupted snapshot, leaves file size %d is not a multiple of %d", len(leaves), SizeLeaf),
		)
	}

	nodesLen := len(nodes) / SizeNode
	leavesLen := len(leaves) / SizeLeaf
	if (leavesLen > 0 && nodesLen+1 != leavesLen) || (leavesLen == 0 && nodesLen != 0) {
		return nil, cleanupHandles(
			fmt.Errorf("corrupted snapshot, branch nodes %d don't match leaves %d", nodesLen, leavesLen),
		)
	}

	nodesData, err := NewNodes(nodes)
	if err != nil {
		return nil, cleanupHandles(err)
	}
	leavesData, err := NewLeaves(leaves)
	if err != nil {
		return nil, cleanupHandles(err)
	}

	snapshot := &Snapshot{
		nodesMap:     nodesMap,
		leavesMap:    leavesMap,
		kvsMap:       kvsMap,
		nodes:        nodes,
		leaves:       leaves,
		kvs:          kvs,
		version:      version,
		nodesLayout:  nodesData,
		leavesLayout: leavesData,
	}

	if nodesLen > 0 {
		snapshot.root = &PersistedNode{
			snapshot: snapshot,
			isLeaf:  false,
			index:   uint32(nodesLen - 1),
		}
	} else if leavesLen > 0 {
		snapshot.root = &PersistedNode{
			snapshot: snapshot,
			isLeaf:  true,
			index:   0,
		}
	}

	return snapshot, nil
}

// Close releases all mmap and file handles.
func (snapshot *Snapshot) Close() error {
	var errs []error
	if snapshot.nodesMap != nil {
		errs = append(errs, snapshot.nodesMap.Close())
	}
	if snapshot.leavesMap != nil {
		errs = append(errs, snapshot.leavesMap.Close())
	}
	if snapshot.kvsMap != nil {
		errs = append(errs, snapshot.kvsMap.Close())
	}
	*snapshot = *NewEmptySnapshot(snapshot.version)
	return errors.Join(errs...)
}

// IsEmpty returns true if the snapshot has no nodes.
func (snapshot *Snapshot) IsEmpty() bool {
	return snapshot.root == nil
}

// Version returns the snapshot version.
func (snapshot *Snapshot) SnapshotVersion() uint32 {
	return snapshot.version
}

// RootNode returns the root PersistedNode. Panics if empty.
func (snapshot *Snapshot) RootNode() PersistedNode {
	if snapshot.IsEmpty() {
		panic("RootNode not supported on empty snapshot")
	}
	return *snapshot.root
}

// RootHash returns the root hash, or emptyHash for an empty snapshot.
func (snapshot *Snapshot) RootHash() []byte {
	if snapshot.IsEmpty() {
		return emptyHash
	}
	return snapshot.RootNode().Hash()
}

// Node returns a branch PersistedNode by index.
func (snapshot *Snapshot) Node(index uint32) PersistedNode {
	return PersistedNode{snapshot: snapshot, index: index, isLeaf: false}
}

// Leaf returns a leaf PersistedNode by index.
func (snapshot *Snapshot) Leaf(index uint32) PersistedNode {
	return PersistedNode{snapshot: snapshot, index: index, isLeaf: true}
}

// Key returns a zero-copy key slice by KVs offset.
func (snapshot *Snapshot) Key(offset uint64) []byte {
	keyLen := binary.LittleEndian.Uint32(snapshot.kvs[offset:])
	offset += 4
	return snapshot.kvs[offset : offset+uint64(keyLen)]
}

// KeyValue returns zero-copy key and value slices by KVs offset.
func (snapshot *Snapshot) KeyValue(offset uint64) ([]byte, []byte) {
	kLen := uint64(binary.LittleEndian.Uint32(snapshot.kvs[offset:]))
	offset += 4
	key := snapshot.kvs[offset : offset+kLen]
	offset += kLen
	vLen := uint64(binary.LittleEndian.Uint32(snapshot.kvs[offset:]))
	offset += 4
	value := snapshot.kvs[offset : offset+vLen]
	return key, value
}

// LeafKey returns the key for a leaf by index.
func (snapshot *Snapshot) LeafKey(index uint32) []byte {
	leaf := snapshot.leavesLayout.Leaf(index)
	offset := leaf.KeyOffset() + 4 // skip key length prefix
	return snapshot.kvs[offset : offset+uint64(leaf.KeyLength())]
}

// LeafKeyValue returns key and value for a leaf by index.
func (snapshot *Snapshot) LeafKeyValue(index uint32) ([]byte, []byte) {
	leaf := snapshot.leavesLayout.Leaf(index)
	offset := leaf.KeyOffset() + 4
	length := uint64(leaf.KeyLength())
	key := snapshot.kvs[offset : offset+length]
	offset += length
	vLen := uint64(binary.LittleEndian.Uint32(snapshot.kvs[offset:]))
	offset += 4
	return key, snapshot.kvs[offset : offset+vLen]
}

// PrepareForRandomRead switches all mmap files to random access mode.
func (snapshot *Snapshot) PrepareForRandomRead() {
	if snapshot.nodesMap != nil {
		snapshot.nodesMap.PrepareForRandomRead()
	}
	if snapshot.leavesMap != nil {
		snapshot.leavesMap.PrepareForRandomRead()
	}
	if snapshot.kvsMap != nil {
		snapshot.kvsMap.PrepareForRandomRead()
	}
}

// --- Snapshot Writing ---

// snapshotWriter writes a tree to snapshot files using buffered I/O.
type snapshotWriter struct {
	ctx    context.Context
	cancel context.CancelFunc

	nodesFile  *os.File
	leavesFile *os.File
	kvsFile    *os.File

	nodesBuf  *bufio.Writer
	leavesBuf *bufio.Writer
	kvsBuf    *bufio.Writer

	branchCounter uint32
	leafCounter   uint32
	kvsOffset     uint64
}

func newSnapshotWriter(ctx context.Context, dir string) (*snapshotWriter, error) {
	ctx, cancel := context.WithCancel(ctx)

	nodesFile, err := os.Create(filepath.Join(dir, FileNameNodes))
	if err != nil {
		cancel()
		return nil, err
	}
	leavesFile, err := os.Create(filepath.Join(dir, FileNameLeaves))
	if err != nil {
		cancel()
		_ = nodesFile.Close()
		return nil, err
	}
	kvsFile, err := os.Create(filepath.Join(dir, FileNameKVs))
	if err != nil {
		cancel()
		_ = nodesFile.Close()
		_ = leavesFile.Close()
		return nil, err
	}

	return &snapshotWriter{
		ctx:        ctx,
		cancel:     cancel,
		nodesFile:  nodesFile,
		leavesFile: leavesFile,
		kvsFile:    kvsFile,
		nodesBuf:   bufio.NewWriterSize(nodesFile, 1<<20), // 1MB buffer
		leavesBuf:  bufio.NewWriterSize(leavesFile, 1<<20),
		kvsBuf:     bufio.NewWriterSize(kvsFile, 1<<20),
	}, nil
}

// writeRecursive performs a depth-first post-order traversal, writing each node.
// Returns the leaf index of the smallest leaf in this subtree.
func (w *snapshotWriter) writeRecursive(node Node) (uint32, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}

	if node.IsLeaf() {
		return w.writeLeaf(node)
	}

	// Recurse left, then right (post-order).
	leftFirstLeaf, err := w.writeRecursive(node.Left())
	if err != nil {
		return 0, err
	}
	rightFirstLeaf, err := w.writeRecursive(node.Right())
	if err != nil {
		return 0, err
	}

	// keyLeaf = first leaf of the right subtree (= the branch node's key).
	if err := w.writeBranch(node, rightFirstLeaf); err != nil {
		return 0, err
	}

	return leftFirstLeaf, nil
}

// writeLeaf writes a leaf node to the leaves and kvs files.
func (w *snapshotWriter) writeLeaf(node Node) (uint32, error) {
	key := node.Key()
	value := node.Value()
	hash := node.SafeHash()

	keyOffset := w.kvsOffset

	// Write key to kvs: uint32(keyLen) + key + uint32(valueLen) + value
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(key)))
	if _, err := w.kvsBuf.Write(lenBuf[:]); err != nil {
		return 0, err
	}
	if _, err := w.kvsBuf.Write(key); err != nil {
		return 0, err
	}
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(value)))
	if _, err := w.kvsBuf.Write(lenBuf[:]); err != nil {
		return 0, err
	}
	if _, err := w.kvsBuf.Write(value); err != nil {
		return 0, err
	}
	w.kvsOffset += 4 + uint64(len(key)) + 4 + uint64(len(value))

	// Write leaf record.
	var buf [SizeLeaf]byte
	EncodeLeafNode(buf[:], node.Version(), uint32(len(key)), keyOffset, hash)
	if _, err := w.leavesBuf.Write(buf[:]); err != nil {
		return 0, err
	}

	idx := w.leafCounter
	w.leafCounter++
	return idx, nil
}

// writeBranch writes a branch node to the nodes file.
func (w *snapshotWriter) writeBranch(node Node, keyLeaf uint32) error {
	preTrees := uint8(w.leafCounter - w.branchCounter)
	hash := node.SafeHash()

	var buf [SizeNode]byte
	EncodeBranchNode(buf[:], node.Height(), preTrees, node.Version(), uint32(node.Size()), keyLeaf, hash)
	if _, err := w.nodesBuf.Write(buf[:]); err != nil {
		return err
	}

	w.branchCounter++
	return nil
}

// flush flushes all buffers and syncs files.
func (w *snapshotWriter) flush() error {
	if err := w.nodesBuf.Flush(); err != nil {
		return err
	}
	if err := w.leavesBuf.Flush(); err != nil {
		return err
	}
	if err := w.kvsBuf.Flush(); err != nil {
		return err
	}
	if err := w.nodesFile.Sync(); err != nil {
		return err
	}
	if err := w.leavesFile.Sync(); err != nil {
		return err
	}
	return w.kvsFile.Sync()
}

// close closes all files.
func (w *snapshotWriter) close() error {
	w.cancel()
	return errors.Join(
		w.nodesFile.Close(),
		w.leavesFile.Close(),
		w.kvsFile.Close(),
	)
}

// WriteSnapshot writes the entire tree to disk in snapshot format.
func WriteSnapshot(ctx context.Context, dir string, version uint32, root Node) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	w, err := newSnapshotWriter(ctx, dir)
	if err != nil {
		return err
	}
	defer w.close()

	// Write tree data.
	if root != nil {
		// Ensure all hashes are computed before writing.
		hashParallelSubtrees(root)

		if _, err := w.writeRecursive(root); err != nil {
			return err
		}
	}

	if err := w.flush(); err != nil {
		return err
	}

	// Write metadata last (atomic finalization marker).
	return writeSnapshotMetadata(dir, version)
}

// writeSnapshotMetadata writes the metadata file.
func writeSnapshotMetadata(dir string, version uint32) error {
	var buf [SizeMetadata]byte
	binary.LittleEndian.PutUint32(buf[0:], SnapshotFileMagic)
	binary.LittleEndian.PutUint32(buf[4:], SnapshotFormat)
	binary.LittleEndian.PutUint32(buf[8:], version)

	metadataPath := filepath.Join(dir, FileNameMetadata)
	tmpPath := metadataPath + ".tmp"
	if err := os.WriteFile(tmpPath, buf[:], 0o644); err != nil {
		return err
	}
	return os.Rename(tmpPath, metadataPath)
}

// WriteTreeSnapshot is a convenience method to write a tree's snapshot.
func (t *Tree) WriteSnapshot(ctx context.Context, dir string) error {
	t.mtx.RLock()
	root := t.root
	version := t.version
	t.mtx.RUnlock()

	return WriteSnapshot(ctx, dir, version, root)
}

// ---- IO helpers ----

// writeAll is a helper that writes all of p to w, similar to io.Copy.
func writeAll(w io.Writer, p []byte) error {
	for len(p) > 0 {
		n, err := w.Write(p)
		p = p[n:]
		if err != nil {
			return err
		}
	}
	return nil
}
