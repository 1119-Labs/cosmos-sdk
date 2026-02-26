package memiavl

import (
	"bytes"
	"math"
	"sort"
)

// PersistedNode is backed by mmap-ed snapshot files.
// Branch nodes and leaf nodes have different layouts (see layout.go).
type PersistedNode struct {
	snapshot *Snapshot
	isLeaf   bool
	index    uint32
}

var _ Node = PersistedNode{}

func (node PersistedNode) branchNode() NodeLayout {
	return node.snapshot.nodesLayout.Node(node.index)
}

func (node PersistedNode) leafNode() LeafLayout {
	return node.snapshot.leavesLayout.Leaf(node.index)
}

func (node PersistedNode) Height() uint8 {
	if node.isLeaf {
		return 0
	}
	return node.branchNode().Height()
}

func (node PersistedNode) IsLeaf() bool {
	return node.isLeaf
}

func (node PersistedNode) Version() uint32 {
	if node.isLeaf {
		return node.leafNode().Version()
	}
	return node.branchNode().Version()
}

func (node PersistedNode) Size() int64 {
	if node.isLeaf {
		return 1
	}
	return int64(node.branchNode().Size())
}

func (node PersistedNode) Key() []byte {
	if node.isLeaf {
		return node.snapshot.LeafKey(node.index)
	}
	return node.snapshot.LeafKey(node.branchNode().KeyLeaf())
}

func (node PersistedNode) Value() []byte {
	if !node.isLeaf {
		return nil
	}
	_, value := node.snapshot.LeafKeyValue(node.index)
	return value
}

func (node PersistedNode) Left() Node {
	if node.isLeaf {
		panic("can't call Left on leaf node")
	}
	data := node.branchNode()
	preTrees := uint32(data.PreTrees())
	startLeaf := getStartLeaf(node.index, data.Size(), preTrees)
	keyLeaf := data.KeyLeaf()
	if startLeaf+1 == keyLeaf {
		return PersistedNode{snapshot: node.snapshot, index: startLeaf, isLeaf: true}
	}
	return PersistedNode{snapshot: node.snapshot, index: getLeftBranch(keyLeaf, preTrees)}
}

func (node PersistedNode) Right() Node {
	if node.isLeaf {
		panic("can't call Right on leaf node")
	}
	data := node.branchNode()
	keyLeaf := data.KeyLeaf()
	preTrees := uint32(data.PreTrees())
	if keyLeaf == getEndLeaf(node.index, preTrees) {
		return PersistedNode{snapshot: node.snapshot, index: keyLeaf, isLeaf: true}
	}
	return PersistedNode{snapshot: node.snapshot, index: node.index - 1}
}

func (node PersistedNode) SafeHash() []byte {
	h := node.Hash()
	cp := make([]byte, len(h))
	copy(cp, h)
	return cp
}

func (node PersistedNode) Hash() []byte {
	if node.isLeaf {
		return node.leafNode().Hash()
	}
	return node.branchNode().Hash()
}

// Mutate creates a mutable MemNode from the persisted node.
func (node PersistedNode) Mutate(version, _ uint32) *MemNode {
	if node.isLeaf {
		key, value := node.snapshot.LeafKeyValue(node.index)
		return newLeafNode(cloneBytes(key), cloneBytes(value), version)
	}
	data := node.branchNode()
	return newBranchNode(data.Height(), int64(data.Size()), version, cloneBytes(node.Key()), node.Left(), node.Right())
}

// Get performs a binary search on the leaf node array (O(log n) for persisted subtrees).
func (node PersistedNode) Get(key []byte) ([]byte, uint32) {
	var start, count uint32
	if node.isLeaf {
		start = node.index
		count = 1
	} else {
		data := node.branchNode()
		preTrees := uint32(data.PreTrees())
		count = data.Size()
		start = getStartLeaf(node.index, count, preTrees)
	}

	if int64(count) > math.MaxInt32 {
		panic("node size exceeds int32")
	}

	res := sort.Search(int(count), func(i int) bool {
		leafKey := node.snapshot.LeafKey(start + uint32(i))
		return bytes.Compare(leafKey, key) >= 0
	})

	i := uint32(res)
	leaf := i + start
	if leaf >= start+count {
		return nil, i
	}

	nodeKey, value := node.snapshot.LeafKeyValue(leaf)
	if !bytes.Equal(nodeKey, key) {
		return nil, i
	}
	return value, i
}

func (node PersistedNode) GetByIndex(leafIndex uint32) ([]byte, []byte) {
	if node.isLeaf {
		if leafIndex != 0 {
			return nil, nil
		}
		return node.snapshot.LeafKeyValue(node.index)
	}
	data := node.branchNode()
	preTrees := uint32(data.PreTrees())
	startLeaf := getStartLeaf(node.index, data.Size(), preTrees)
	endLeaf := getEndLeaf(node.index, preTrees)

	i := startLeaf + leafIndex
	if i > endLeaf {
		return nil, nil
	}
	return node.snapshot.LeafKeyValue(i)
}

// getStartLeaf returns the index of the first leaf in the node's subtree.
// Derivation: leafCounter = preTrees + bi, firstLeaf = leafCounter - size
func getStartLeaf(index, size, preTrees uint32) uint32 {
	return index + preTrees - size
}

// getEndLeaf returns the index of the last leaf in the node's subtree.
// Derivation: lastLeaf = leafCounter - 1 = preTrees + bi - 1
func getEndLeaf(index, preTrees uint32) uint32 {
	return index + preTrees - 1
}

// getLeftBranch returns the index of the left child branch node.
// Derivation: leftBranch = bi - 1 - (rightBranches) = keyLeaf - preTrees
func getLeftBranch(keyLeaf, preTrees uint32) uint32 {
	return keyLeaf - preTrees
}
