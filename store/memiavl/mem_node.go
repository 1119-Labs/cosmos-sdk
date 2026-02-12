package memiavl

import (
	"bytes"
	"math"
)

// MemNode is an in-memory IAVL node with copy-on-write support.
type MemNode struct {
	height  uint8
	version uint32
	size    int64
	key     []byte
	value   []byte
	left    Node
	right   Node
	hash    []byte
}

var _ Node = (*MemNode)(nil)

func newBranchNode(height uint8, size int64, version uint32, key []byte, left, right Node) *MemNode {
	return &MemNode{
		height:  height,
		size:    size,
		version: version,
		key:     key,
		left:    left,
		right:   right,
	}
}

func newLeafNode(key, value []byte, version uint32) *MemNode {
	return &MemNode{
		key: key, value: value, version: version, size: 1,
	}
}

func (node *MemNode) Height() uint8   { return node.height }
func (node *MemNode) IsLeaf() bool    { return node.height == 0 }
func (node *MemNode) Size() int64     { return node.size }
func (node *MemNode) Version() uint32 { return node.version }
func (node *MemNode) Key() []byte     { return node.key }
func (node *MemNode) Value() []byte   { return node.value }
func (node *MemNode) Left() Node      { return node.left }
func (node *MemNode) Right() Node     { return node.right }

// Mutate clones the node if version <= cowVersion (copy-on-write), otherwise modifies in-place.
func (node *MemNode) Mutate(version, cowVersion uint32) *MemNode {
	n := node
	if node.version <= cowVersion {
		cloned := *node
		n = &cloned
	}
	n.version = version
	n.hash = nil
	return n
}

func (node *MemNode) SafeHash() []byte { return node.Hash() }

// Hash computes and caches the node hash. Must be called after children hashes are computed.
func (node *MemNode) Hash() []byte {
	if node == nil {
		return nil
	}
	if node.hash != nil {
		return node.hash
	}
	node.hash = HashNode(node)
	return node.hash
}

func (node *MemNode) updateHeightSize() {
	node.height = maxUInt8(node.left.Height(), node.right.Height()) + 1
	node.size = node.left.Size() + node.right.Size()
}

func (node *MemNode) calcBalance() int {
	return int(node.left.Height()) - int(node.right.Height())
}

func calcBalance(node Node) int {
	return int(node.Left().Height()) - int(node.Right().Height())
}

// rotateRight performs a right rotation. Invariant: node is returned by Mutate(version).
//
//	   S               L
//	  / \      =>     / \
//	 L                   S
//	/ \                 / \
//	  LR               LR
func (node *MemNode) rotateRight(version, cowVersion uint32) *MemNode {
	newSelf := node.left.Mutate(version, cowVersion)
	node.left = node.left.Right()
	newSelf.right = node
	node.updateHeightSize()
	newSelf.updateHeightSize()
	return newSelf
}

// rotateLeft performs a left rotation. Invariant: node is returned by Mutate(version).
//
//	 S              R
//	/ \     =>     / \
//	    R         S
//	   / \       / \
//	 RL             RL
func (node *MemNode) rotateLeft(version, cowVersion uint32) *MemNode {
	newSelf := node.right.Mutate(version, cowVersion)
	node.right = node.right.Left()
	newSelf.left = node
	node.updateHeightSize()
	newSelf.updateHeightSize()
	return newSelf
}

// reBalance re-balances the node using AVL rotations.
func (node *MemNode) reBalance(version, cowVersion uint32) *MemNode {
	balance := node.calcBalance()
	switch {
	case balance > 1:
		leftBalance := calcBalance(node.left)
		if leftBalance >= 0 {
			return node.rotateRight(version, cowVersion)
		}
		node.left = node.left.Mutate(version, cowVersion).rotateLeft(version, cowVersion)
		return node.rotateRight(version, cowVersion)
	case balance < -1:
		rightBalance := calcBalance(node.right)
		if rightBalance <= 0 {
			return node.rotateLeft(version, cowVersion)
		}
		node.right = node.right.Mutate(version, cowVersion).rotateRight(version, cowVersion)
		return node.rotateLeft(version, cowVersion)
	default:
		return node
	}
}

// Get returns the value for a key and its index within the tree.
func (node *MemNode) Get(key []byte) ([]byte, uint32) {
	if node.IsLeaf() {
		switch bytes.Compare(node.key, key) {
		case -1:
			return nil, 1
		case 1:
			return nil, 0
		default:
			return node.value, 0
		}
	}

	if bytes.Compare(key, node.key) == -1 {
		return node.Left().Get(key)
	}
	right := node.Right()
	value, index := right.Get(key)
	size := node.Size() - right.Size()
	if size < 0 || size > math.MaxUint32 {
		panic("size under/overflows uint32")
	}
	return value, index + uint32(size)
}

// GetByIndex returns the key-value pair at the given index.
func (node *MemNode) GetByIndex(index uint32) ([]byte, []byte) {
	if node.IsLeaf() {
		if index == 0 {
			return node.key, node.value
		}
		return nil, nil
	}

	left := node.Left()
	leftSizei64 := left.Size()
	if leftSizei64 < 0 || leftSizei64 > math.MaxUint32 {
		panic("left size under/overflows uint32")
	}
	leftSize := uint32(leftSizei64)
	if index < leftSize {
		return left.GetByIndex(index)
	}
	return node.Right().GetByIndex(index - leftSize)
}

func maxUInt8(a, b uint8) uint8 {
	if a > b {
		return a
	}
	return b
}

// cloneBytes returns a copy of the byte slice.
func cloneBytes(bz []byte) []byte {
	if bz == nil {
		return nil
	}
	cp := make([]byte, len(bz))
	copy(cp, bz)
	return cp
}
