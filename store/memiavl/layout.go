package memiavl

import (
	"crypto/sha256"
	"encoding/binary"
)

// Snapshot file layout constants.
const (
	// Branch node offsets (total 48 bytes).
	OffsetHeight   = 0
	OffsetPreTrees = OffsetHeight + 1
	// 2 bytes padding at offset 2-3
	OffsetVersion = OffsetHeight + 4
	OffsetSize    = OffsetVersion + 4
	OffsetKeyLeaf = OffsetSize + 4

	OffsetHash          = OffsetKeyLeaf + 4
	SizeHash            = sha256.Size // 32
	SizeNodeWithoutHash = OffsetHash
	SizeNode            = SizeNodeWithoutHash + SizeHash // 48

	// Leaf node offsets (total 48 bytes).
	OffsetLeafVersion   = 0
	OffsetLeafKeyLen    = OffsetLeafVersion + 4
	OffsetLeafKeyOffset = OffsetLeafKeyLen + 4
	OffsetLeafHash      = OffsetLeafKeyOffset + 8
	SizeLeafWithoutHash = OffsetLeafHash
	SizeLeaf            = SizeLeafWithoutHash + SizeHash // 48
)

// Nodes wraps a contiguous buffer of branch node records.
type Nodes struct {
	data []byte
}

// NewNodes wraps the raw byte buffer as Nodes.
func NewNodes(data []byte) (Nodes, error) {
	return Nodes{data: data}, nil
}

// Node returns the branch node layout at index i.
func (nodes Nodes) Node(i uint32) NodeLayout {
	offset := int(i) * SizeNode
	return NodeLayout{data: (*[SizeNode]byte)(nodes.data[offset : offset+SizeNode])}
}

// NodeLayout provides zero-copy access to a branch node's fields.
type NodeLayout struct {
	data *[SizeNode]byte
}

func (node NodeLayout) Height() uint8 {
	return node.data[OffsetHeight]
}

func (node NodeLayout) PreTrees() uint8 {
	return node.data[OffsetPreTrees]
}

func (node NodeLayout) Version() uint32 {
	return binary.LittleEndian.Uint32(node.data[OffsetVersion : OffsetVersion+4])
}

func (node NodeLayout) Size() uint32 {
	return binary.LittleEndian.Uint32(node.data[OffsetSize : OffsetSize+4])
}

func (node NodeLayout) KeyLeaf() uint32 {
	return binary.LittleEndian.Uint32(node.data[OffsetKeyLeaf : OffsetKeyLeaf+4])
}

func (node NodeLayout) Hash() []byte {
	return node.data[OffsetHash : OffsetHash+SizeHash]
}

// Leaves wraps a contiguous buffer of leaf node records.
type Leaves struct {
	data []byte
}

// NewLeaves wraps the raw byte buffer as Leaves.
func NewLeaves(data []byte) (Leaves, error) {
	return Leaves{data: data}, nil
}

// Leaf returns the leaf layout at index i.
func (leaves Leaves) Leaf(i uint32) LeafLayout {
	offset := int(i) * SizeLeaf
	return LeafLayout{data: (*[SizeLeaf]byte)(leaves.data[offset : offset+SizeLeaf])}
}

// LeafLayout provides zero-copy access to a leaf node's fields.
type LeafLayout struct {
	data *[SizeLeaf]byte
}

func (leaf LeafLayout) Version() uint32 {
	return binary.LittleEndian.Uint32(leaf.data[OffsetLeafVersion : OffsetLeafVersion+4])
}

func (leaf LeafLayout) KeyLength() uint32 {
	return binary.LittleEndian.Uint32(leaf.data[OffsetLeafKeyLen : OffsetLeafKeyLen+4])
}

func (leaf LeafLayout) KeyOffset() uint64 {
	return binary.LittleEndian.Uint64(leaf.data[OffsetLeafKeyOffset : OffsetLeafKeyOffset+8])
}

func (leaf LeafLayout) Hash() []byte {
	return leaf.data[OffsetLeafHash : OffsetLeafHash+SizeHash]
}

// EncodeBranchNode encodes a branch node into buf (must be SizeNode bytes).
func EncodeBranchNode(buf []byte, height, preTrees uint8, version, size, keyLeaf uint32, hash []byte) {
	buf[OffsetHeight] = height
	buf[OffsetPreTrees] = preTrees
	buf[2] = 0 // padding
	buf[3] = 0 // padding
	binary.LittleEndian.PutUint32(buf[OffsetVersion:], version)
	binary.LittleEndian.PutUint32(buf[OffsetSize:], size)
	binary.LittleEndian.PutUint32(buf[OffsetKeyLeaf:], keyLeaf)
	copy(buf[OffsetHash:], hash)
}

// EncodeLeafNode encodes a leaf node into buf (must be SizeLeaf bytes).
func EncodeLeafNode(buf []byte, version, keyLen uint32, keyOffset uint64, hash []byte) {
	binary.LittleEndian.PutUint32(buf[OffsetLeafVersion:], version)
	binary.LittleEndian.PutUint32(buf[OffsetLeafKeyLen:], keyLen)
	binary.LittleEndian.PutUint64(buf[OffsetLeafKeyOffset:], keyOffset)
	copy(buf[OffsetLeafHash:], hash)
}
