package memiavl

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
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

// writeHashBytes writes the hash preimage of a node to the writer.
// The encoding is identical to cosmos/iavl for hash compatibility:
//
//	varint(height) + varint(size) + varint(version)
//	leaf:   encodeBytes(key) + encodeBytes(sha256(value))
//	branch: encodeBytes(leftHash) + encodeBytes(rightHash)
func writeHashBytes(node Node, w io.Writer) error {
	var (
		n   int
		buf [binary.MaxVarintLen64]byte
	)

	n = binary.PutVarint(buf[:], int64(node.Height()))
	if _, err := w.Write(buf[0:n]); err != nil {
		return fmt.Errorf("writing height, %w", err)
	}
	n = binary.PutVarint(buf[:], node.Size())
	if _, err := w.Write(buf[0:n]); err != nil {
		return fmt.Errorf("writing size, %w", err)
	}
	n = binary.PutVarint(buf[:], int64(node.Version()))
	if _, err := w.Write(buf[0:n]); err != nil {
		return fmt.Errorf("writing version, %w", err)
	}

	if node.IsLeaf() {
		if err := encodeBytes(w, node.Key()); err != nil {
			return fmt.Errorf("writing key, %w", err)
		}
		valueHash := sha256.Sum256(node.Value())
		if err := encodeBytes(w, valueHash[:]); err != nil {
			return fmt.Errorf("writing value, %w", err)
		}
	} else {
		if err := encodeBytes(w, node.Left().Hash()); err != nil {
			return fmt.Errorf("writing left hash, %w", err)
		}
		if err := encodeBytes(w, node.Right().Hash()); err != nil {
			return fmt.Errorf("writing right hash, %w", err)
		}
	}

	return nil
}

// HashNode computes the SHA-256 hash of a node.
func HashNode(node Node) []byte {
	if node == nil {
		return nil
	}
	h := sha256.New()
	if err := writeHashBytes(node, h); err != nil {
		panic(err)
	}
	return h.Sum(nil)
}
