package memiavl

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"hash"
	"sync"
)

// Node interface encapsulates both PersistedNode (Phase 2) and MemNode.
type Node interface {
	Height() uint8
	IsLeaf() bool
	Size() int64
	Version() uint32
	Key() []byte
	Value() []byte
	Left() Node
	Right() Node
	Hash() []byte

	// SafeHash returns a byte slice safe to retain.
	SafeHash() []byte

	// Mutate clones (PersistedNode) or modifies in-place (MemNode, if version > cowVersion).
	Mutate(version, cowVersion uint32) *MemNode

	// Get returns the value for a key and its index.
	Get(key []byte) ([]byte, uint32)
	GetByIndex(uint32) ([]byte, []byte)
}

// setRecursive performs a set operation, always returning a new MemNode.
// Returns (newNode, updated) where updated=true means an existing key was overwritten.
func setRecursive(node Node, key, value []byte, version, cowVersion uint32) (*MemNode, bool) {
	if node == nil {
		return newLeafNode(key, value, version), true
	}

	nodeKey := node.Key()
	if node.IsLeaf() {
		switch bytes.Compare(key, nodeKey) {
		case -1:
			return newBranchNode(1, 2, version, nodeKey, newLeafNode(key, value, version), node), false
		case 1:
			return newBranchNode(1, 2, version, key, node, newLeafNode(key, value, version)), false
		default:
			newNode := node.Mutate(version, cowVersion)
			newNode.value = value
			return newNode, true
		}
	}

	var (
		newChild, newNode *MemNode
		updated           bool
	)
	if bytes.Compare(key, nodeKey) == -1 {
		newChild, updated = setRecursive(node.Left(), key, value, version, cowVersion)
		newNode = node.Mutate(version, cowVersion)
		newNode.left = newChild
	} else {
		newChild, updated = setRecursive(node.Right(), key, value, version, cowVersion)
		newNode = node.Mutate(version, cowVersion)
		newNode.right = newChild
	}

	if !updated {
		newNode.updateHeightSize()
		newNode = newNode.reBalance(version, cowVersion)
	}

	return newNode, updated
}

// removeRecursive returns (value, newNode, newKey).
// - (nil, origNode, nil) means nothing changed
// - (value, nil, nil) means leaf was removed
// - (value, newNode, newKey) means subtree changed
func removeRecursive(node Node, key []byte, version, cowVersion uint32) ([]byte, Node, []byte) {
	if node == nil {
		return nil, nil, nil
	}

	if node.IsLeaf() {
		if bytes.Equal(node.Key(), key) {
			return node.Value(), nil, nil
		}
		return nil, node, nil
	}

	if bytes.Compare(key, node.Key()) == -1 {
		value, newLeft, newKey := removeRecursive(node.Left(), key, version, cowVersion)
		if value == nil {
			return nil, node, nil
		}
		if newLeft == nil {
			return value, node.Right(), node.Key()
		}
		newNode := node.Mutate(version, cowVersion)
		newNode.left = newLeft
		newNode.updateHeightSize()
		return value, newNode.reBalance(version, cowVersion), newKey
	}

	value, newRight, newKey := removeRecursive(node.Right(), key, version, cowVersion)
	if value == nil {
		return nil, node, nil
	}
	if newRight == nil {
		return value, node.Left(), nil
	}

	newNode := node.Mutate(version, cowVersion)
	newNode.right = newRight
	if newKey != nil {
		newNode.key = newKey
	}
	newNode.updateHeightSize()
	return value, newNode.reBalance(version, cowVersion), nil
}

var hasherPool = sync.Pool{
	New: func() interface{} { return sha256.New() },
}

// buildPreimage writes the hash preimage of a node into buf and returns the length.
// The encoding is identical to cosmos/iavl for hash compatibility:
//
//	varint(height) + varint(size) + varint(version)
//	leaf:   encodeBytes(key) + encodeBytes(sha256(value))
//	branch: encodeBytes(leftHash) + encodeBytes(rightHash)
//
// Branch preimage max: 10 + 10 + 10 + (10+32) + (10+32) = 114 bytes
// Leaf preimage max: 10 + 10 + 10 + (10+key_len) + (10+32) = 82 + key_len
// For keys < 174 bytes, fits in [256]byte.
func buildPreimage(node Node, buf []byte) int {
	n := 0
	n += binary.PutVarint(buf[n:], int64(node.Height()))
	n += binary.PutVarint(buf[n:], node.Size())
	n += binary.PutVarint(buf[n:], int64(node.Version()))

	if node.IsLeaf() {
		// encodeBytes(key)
		key := node.Key()
		n += binary.PutUvarint(buf[n:], uint64(len(key)))
		n += copy(buf[n:], key)
		// encodeBytes(sha256(value)) — pooled hasher
		vh := hasherPool.Get().(hash.Hash)
		vh.Reset()
		vh.Write(node.Value())
		var vbuf [32]byte
		vh.Sum(vbuf[:0])
		hasherPool.Put(vh)
		n += binary.PutUvarint(buf[n:], 32)
		n += copy(buf[n:], vbuf[:])
	} else {
		// encodeBytes(leftHash)
		lh := node.Left().Hash()
		n += binary.PutUvarint(buf[n:], uint64(len(lh)))
		n += copy(buf[n:], lh)
		// encodeBytes(rightHash)
		rh := node.Right().Hash()
		n += binary.PutUvarint(buf[n:], uint64(len(rh)))
		n += copy(buf[n:], rh)
	}
	return n
}

// buildPreimageLargeKey handles leaf nodes with keys >= 174 bytes that don't fit in [256]byte.
func buildPreimageLargeKey(node Node, h hash.Hash) {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutVarint(buf[:], int64(node.Height()))
	h.Write(buf[:n])
	n = binary.PutVarint(buf[:], node.Size())
	h.Write(buf[:n])
	n = binary.PutVarint(buf[:], int64(node.Version()))
	h.Write(buf[:n])

	// encodeBytes(key)
	key := node.Key()
	n = binary.PutUvarint(buf[:], uint64(len(key)))
	h.Write(buf[:n])
	h.Write(key)

	// encodeBytes(sha256(value))
	vh := hasherPool.Get().(hash.Hash)
	vh.Reset()
	vh.Write(node.Value())
	var vbuf [32]byte
	vh.Sum(vbuf[:0])
	hasherPool.Put(vh)
	n = binary.PutUvarint(buf[:], 32)
	h.Write(buf[:n])
	h.Write(vbuf[:])
}

// HashNode computes the SHA-256 hash of a node using pooled hashers.
func HashNode(node Node) []byte {
	if node == nil {
		return nil
	}
	h := hasherPool.Get().(hash.Hash)
	h.Reset()

	if node.IsLeaf() && len(node.Key()) >= 174 {
		buildPreimageLargeKey(node, h)
	} else {
		var buf [256]byte
		n := buildPreimage(node, buf[:])
		h.Write(buf[:n])
	}

	var hashBuf [32]byte
	h.Sum(hashBuf[:0])
	hasherPool.Put(h)
	result := make([]byte, 32) // must heap-alloc — stored in node.hash
	copy(result, hashBuf[:])
	return result
}

// collectDirtyPostOrder collects dirty MemNodes (hash==nil) in post-order.
// Post-order ensures children are hashed before their parents.
func collectDirtyPostOrder(node Node, out *[]*MemNode) {
	if node == nil {
		return
	}
	mn, ok := node.(*MemNode)
	if !ok || mn.hash != nil {
		return // already hashed or not a MemNode → skip subtree
	}
	if !mn.IsLeaf() {
		collectDirtyPostOrder(mn.left, out)
		collectDirtyPostOrder(mn.right, out)
	}
	*out = append(*out, mn)
}

// hashDirtyBatch hashes all dirty nodes under root using a single reused hasher.
// This avoids per-node hasherPool.Get/Put overhead.
func hashDirtyBatch(root Node) {
	var dirty []*MemNode
	collectDirtyPostOrder(root, &dirty)
	if len(dirty) == 0 {
		return
	}
	hashDirtySlice(dirty)
}

// hashDirtySlice hashes a pre-collected post-order slice of dirty MemNodes.
func hashDirtySlice(dirty []*MemNode) {
	h := hasherPool.Get().(hash.Hash)
	vh := hasherPool.Get().(hash.Hash) // for leaf value hashing
	defer hasherPool.Put(h)
	defer hasherPool.Put(vh)

	var buf [256]byte
	var hashBuf [32]byte

	for _, node := range dirty {
		h.Reset()
		if node.IsLeaf() && len(node.key) >= 174 {
			// Large key doesn't fit in [256]byte — use multi-write path
			buildPreimageLargeKeyMem(node, h, vh)
		} else {
			n := buildPreimageMem(node, buf[:], vh)
			h.Write(buf[:n])
		}
		h.Sum(hashBuf[:0])
		node.hash = make([]byte, 32)
		copy(node.hash, hashBuf[:])
	}
}

const (
	parallelHashMinDirty = 64 // only parallelize if > 64 dirty nodes
	subtreeDepth         = 3  // collect subtree roots at depth 3 → max 8 subtrees
)

// collectDirtyAtDepth collects dirty MemNode subtree roots at the given depth.
// Nodes at shallower depths that are dirty are NOT collected — only at exactly the target depth
// or at the first dirty node encountered if depth hasn't been reached.
func collectDirtyAtDepth(node Node, depth int, out *[]*MemNode) {
	if node == nil {
		return
	}
	mn, ok := node.(*MemNode)
	if !ok || mn.hash != nil {
		return // already hashed → skip
	}
	if depth == 0 || mn.IsLeaf() {
		*out = append(*out, mn)
		return
	}
	collectDirtyAtDepth(mn.left, depth-1, out)
	collectDirtyAtDepth(mn.right, depth-1, out)
}

// hashParallelSubtrees hashes all dirty nodes under root.
// For large trees (>64 dirty nodes), splits work across goroutines at subtreeDepth.
func hashParallelSubtrees(root Node) {
	var dirty []*MemNode
	collectDirtyPostOrder(root, &dirty)
	if len(dirty) == 0 {
		return
	}

	if len(dirty) < parallelHashMinDirty {
		hashDirtySlice(dirty)
		return
	}

	// Collect subtree roots at subtreeDepth
	var subtreeRoots []*MemNode
	collectDirtyAtDepth(root, subtreeDepth, &subtreeRoots)

	if len(subtreeRoots) <= 1 {
		// Degenerate case (e.g. long chain) — just batch serial
		hashDirtySlice(dirty)
		return
	}

	// Hash subtrees in parallel — each subtree is disjoint
	var wg sync.WaitGroup
	for _, sr := range subtreeRoots {
		wg.Add(1)
		go func(n *MemNode) {
			defer wg.Done()
			hashDirtyBatch(n)
		}(sr)
	}
	wg.Wait()

	// Hash remaining top-level nodes (children are now done)
	// Re-collect only the top-level dirty nodes
	hashDirtyBatch(root)
}

// buildPreimageLargeKeyMem handles leaf MemNodes with keys >= 174 bytes via multi-write.
func buildPreimageLargeKeyMem(node *MemNode, h, vh hash.Hash) {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutVarint(buf[:], int64(node.height))
	h.Write(buf[:n])
	n = binary.PutVarint(buf[:], node.size)
	h.Write(buf[:n])
	n = binary.PutVarint(buf[:], int64(node.version))
	h.Write(buf[:n])

	n = binary.PutUvarint(buf[:], uint64(len(node.key)))
	h.Write(buf[:n])
	h.Write(node.key)

	vh.Reset()
	vh.Write(node.value)
	var vbuf [32]byte
	vh.Sum(vbuf[:0])
	n = binary.PutUvarint(buf[:], 32)
	h.Write(buf[:n])
	h.Write(vbuf[:])
}

// buildPreimageMem builds the hash preimage for a *MemNode directly (no interface dispatch).
// For leaf nodes, uses the provided value-hasher vh instead of acquiring from pool.
// Caller must ensure buf is at least 256 bytes for keys < 174 bytes.
func buildPreimageMem(node *MemNode, buf []byte, vh hash.Hash) int {
	n := 0
	n += binary.PutVarint(buf[n:], int64(node.height))
	n += binary.PutVarint(buf[n:], node.size)
	n += binary.PutVarint(buf[n:], int64(node.version))

	if node.IsLeaf() {
		// encodeBytes(key)
		n += binary.PutUvarint(buf[n:], uint64(len(node.key)))
		n += copy(buf[n:], node.key)
		// encodeBytes(sha256(value))
		vh.Reset()
		vh.Write(node.value)
		var vbuf [32]byte
		vh.Sum(vbuf[:0])
		n += binary.PutUvarint(buf[n:], 32)
		n += copy(buf[n:], vbuf[:])
	} else {
		// Children are guaranteed hashed (post-order traversal)
		lh := node.left.Hash()
		n += binary.PutUvarint(buf[n:], uint64(len(lh)))
		n += copy(buf[n:], lh)
		rh := node.right.Hash()
		n += binary.PutUvarint(buf[n:], uint64(len(rh)))
		n += copy(buf[n:], rh)
	}
	return n
}
