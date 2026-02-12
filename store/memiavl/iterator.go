package memiavl

import "bytes"

// Iterator is a stack-based DFS iterator over the in-memory IAVL tree.
// It implements the cosmos-db Iterator interface.
type Iterator struct {
	start, end []byte
	ascending  bool
	key, value []byte
	valid      bool
	unsafeCopy bool // if true, Key()/Value() return references without cloning
	stack      []Node
}

// NewIterator creates a new iterator over [start, end).
// The first key-value pair is cached immediately via Next().
func NewIterator(start, end []byte, ascending bool, root Node) *Iterator {
	iter := &Iterator{
		start:     start,
		end:       end,
		ascending: ascending,
		valid:     true,
	}
	if root != nil {
		iter.stack = []Node{root}
	}
	// cache the first key-value
	iter.Next()
	return iter
}

func (iter *Iterator) Domain() ([]byte, []byte) { return iter.start, iter.end }
func (iter *Iterator) Valid() bool               { return iter.valid }
func (iter *Iterator) Error() error              { return nil }

func (iter *Iterator) Key() []byte {
	if iter.unsafeCopy {
		return iter.key
	}
	return cloneBytes(iter.key)
}

func (iter *Iterator) Value() []byte {
	if iter.unsafeCopy {
		return iter.value
	}
	return cloneBytes(iter.value)
}

// NewUnsafeIterator creates an iterator where Key()/Value() return references
// without cloning. For use by Block-STM where MVKVStore handles its own cloning.
func NewUnsafeIterator(start, end []byte, ascending bool, root Node) *Iterator {
	iter := NewIterator(start, end, ascending, root)
	iter.unsafeCopy = true
	return iter
}

// Next advances the iterator to the next key-value pair.
func (iter *Iterator) Next() {
	for len(iter.stack) > 0 {
		// pop node
		node := iter.stack[len(iter.stack)-1]
		iter.stack = iter.stack[:len(iter.stack)-1]

		key := node.Key()
		startCmp := bytes.Compare(iter.start, key)
		afterStart := iter.start == nil || startCmp < 0
		beforeEnd := iter.end == nil || bytes.Compare(key, iter.end) < 0

		if node.IsLeaf() {
			startOrAfter := afterStart || startCmp == 0
			if startOrAfter && beforeEnd {
				iter.key = key
				iter.value = node.Value()
				return
			}
		} else {
			if iter.ascending {
				if beforeEnd {
					iter.stack = append(iter.stack, node.Right())
				}
				if afterStart {
					iter.stack = append(iter.stack, node.Left())
				}
			} else {
				if afterStart {
					iter.stack = append(iter.stack, node.Left())
				}
				if beforeEnd {
					iter.stack = append(iter.stack, node.Right())
				}
			}
		}
	}

	iter.valid = false
}

// Close releases resources held by the iterator.
func (iter *Iterator) Close() error {
	iter.valid = false
	iter.stack = nil
	return nil
}
