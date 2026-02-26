package memiavl

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
)

// NamedTree pairs a tree with its store name.
type NamedTree struct {
	*Tree
	Name string
}

// MultiTree manages multiple named IAVL trees with coordinated versioning.
type MultiTree struct {
	trees          []NamedTree
	treesByName    map[string]int
	initialVersion atomic.Uint32
	lastVersion    int64
}

// NewMultiTree creates a MultiTree from the given named trees.
func NewMultiTree(trees []NamedTree) *MultiTree {
	mt := &MultiTree{
		trees:       make([]NamedTree, len(trees)),
		treesByName: make(map[string]int, len(trees)),
	}
	copy(mt.trees, trees)
	sort.Slice(mt.trees, func(i, j int) bool {
		return mt.trees[i].Name < mt.trees[j].Name
	})
	for i, t := range mt.trees {
		mt.treesByName[t.Name] = i
	}
	return mt
}

// NewEmptyMultiTree creates a MultiTree with empty trees for the given store names.
func NewEmptyMultiTree(storeNames []string) *MultiTree {
	trees := make([]NamedTree, len(storeNames))
	for i, name := range storeNames {
		trees[i] = NamedTree{Tree: New(), Name: name}
	}
	return NewMultiTree(trees)
}

// TreeByName returns the tree with the given name, or nil.
func (mt *MultiTree) TreeByName(name string) *Tree {
	if idx, ok := mt.treesByName[name]; ok {
		return mt.trees[idx].Tree
	}
	return nil
}

// Trees returns all named trees (sorted by name).
func (mt *MultiTree) Trees() []NamedTree {
	return mt.trees
}

// Version returns the current version across all trees.
func (mt *MultiTree) Version() int64 {
	return mt.lastVersion
}

// SetInitialVersion sets the initial version for all trees.
func (mt *MultiTree) SetInitialVersion(initialVersion int64) error {
	if initialVersion < 0 {
		return fmt.Errorf("initial version must be non-negative: %d", initialVersion)
	}
	mt.initialVersion.Store(uint32(initialVersion))
	for _, entry := range mt.trees {
		if err := entry.Tree.SetInitialVersion(initialVersion); err != nil {
			return err
		}
	}
	return nil
}

// ApplyChangeSet applies a set of key-value changes to the named tree.
func (mt *MultiTree) ApplyChangeSet(name string, pairs []KVPair) error {
	tree := mt.TreeByName(name)
	if tree == nil {
		return fmt.Errorf("tree not found: %s", name)
	}
	tree.mtx.Lock()
	defer tree.mtx.Unlock()
	for _, pair := range pairs {
		if pair.Delete {
			tree.remove(pair.Key)
		} else {
			tree.set(pair.Key, pair.Value)
		}
	}
	return nil
}

// ApplyChangeSets applies multiple named changesets.
func (mt *MultiTree) ApplyChangeSets(changeSets []NamedChangeSet) error {
	for _, cs := range changeSets {
		if err := mt.ApplyChangeSet(cs.Name, cs.Pairs); err != nil {
			return err
		}
	}
	return nil
}

// SaveVersion commits all trees, incrementing their version.
// If updateHash is true, computes root hashes for all trees.
// When there are >4 trees, hashing is parallelized.
func (mt *MultiTree) SaveVersion(updateHash bool) (int64, error) {
	if len(mt.trees) > 4 && updateHash {
		return mt.saveVersionParallel()
	}
	return mt.saveVersionSerial(updateHash)
}

func (mt *MultiTree) saveVersionSerial(updateHash bool) (int64, error) {
	var version int64
	for i := range mt.trees {
		_, v, err := mt.trees[i].Tree.SaveVersion(updateHash)
		if err != nil {
			return 0, fmt.Errorf("save version for %s: %w", mt.trees[i].Name, err)
		}
		version = v
	}
	mt.lastVersion = version
	return version, nil
}

func (mt *MultiTree) saveVersionParallel() (int64, error) {
	type result struct {
		version int64
		err     error
	}
	results := make([]result, len(mt.trees))

	var wg sync.WaitGroup
	for i := range mt.trees {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, v, err := mt.trees[idx].Tree.SaveVersion(true)
			results[idx] = result{version: v, err: err}
		}(i)
	}
	wg.Wait()

	var version int64
	for i, r := range results {
		if r.err != nil {
			return 0, fmt.Errorf("save version for %s: %w", mt.trees[i].Name, r.err)
		}
		version = r.version
	}
	mt.lastVersion = version
	return version, nil
}

// Copy returns a CoW copy of all trees.
func (mt *MultiTree) Copy() *MultiTree {
	trees := make([]NamedTree, len(mt.trees))
	for i, entry := range mt.trees {
		trees[i] = NamedTree{
			Tree: entry.Tree.Copy(),
			Name: entry.Name,
		}
	}
	newMT := NewMultiTree(trees)
	newMT.lastVersion = mt.lastVersion
	return newMT
}

// WriteSnapshot writes all trees to snapshot directory.
// Each tree gets its own subdirectory.
func (mt *MultiTree) WriteSnapshot(ctx context.Context, dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	// Parallelize snapshot writing across trees.
	var wg sync.WaitGroup
	errs := make([]error, len(mt.trees))

	for i := range mt.trees {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			treeDir := filepath.Join(dir, mt.trees[idx].Name)
			errs[idx] = mt.trees[idx].Tree.WriteSnapshot(ctx, treeDir)
		}(i)
	}
	wg.Wait()

	return errors.Join(errs...)
}

// WriteMultiTreeMetadata writes a metadata file for the multi-tree snapshot.
func (mt *MultiTree) WriteMultiTreeMetadata(dir string) error {
	// Write a simple metadata file listing tree names and the overall version.
	f, err := os.Create(filepath.Join(dir, "__metadata"))
	if err != nil {
		return err
	}
	defer f.Close()

	// Format: version (8 bytes LE) + count (4 bytes LE) + names (len-prefixed)
	var buf [12]byte
	binary.LittleEndian.PutUint64(buf[:8], uint64(mt.lastVersion))
	binary.LittleEndian.PutUint32(buf[8:], uint32(len(mt.trees)))
	if _, err := f.Write(buf[:]); err != nil {
		return err
	}
	for _, entry := range mt.trees {
		var nameLenBuf [4]byte
		binary.LittleEndian.PutUint32(nameLenBuf[:], uint32(len(entry.Name)))
		if _, err := f.Write(nameLenBuf[:]); err != nil {
			return err
		}
		if _, err := f.Write([]byte(entry.Name)); err != nil {
			return err
		}
	}
	return f.Sync()
}

// LoadMultiTree loads a MultiTree from a snapshot directory.
func LoadMultiTree(ctx context.Context, dir string) (*MultiTree, error) {
	// Read metadata.
	metaBz, err := os.ReadFile(filepath.Join(dir, "__metadata"))
	if err != nil {
		return nil, fmt.Errorf("read multi-tree metadata: %w", err)
	}
	if len(metaBz) < 12 {
		return nil, fmt.Errorf("multi-tree metadata too short: %d bytes", len(metaBz))
	}

	version := int64(binary.LittleEndian.Uint64(metaBz[:8]))
	count := binary.LittleEndian.Uint32(metaBz[8:12])
	offset := 12

	names := make([]string, 0, count)
	for i := uint32(0); i < count; i++ {
		if offset+4 > len(metaBz) {
			return nil, fmt.Errorf("multi-tree metadata truncated at name %d", i)
		}
		nameLen := int(binary.LittleEndian.Uint32(metaBz[offset:]))
		offset += 4
		if offset+nameLen > len(metaBz) {
			return nil, fmt.Errorf("multi-tree metadata truncated at name %d data", i)
		}
		names = append(names, string(metaBz[offset:offset+nameLen]))
		offset += nameLen
	}

	// Load each tree from its subdirectory.
	trees := make([]NamedTree, len(names))
	loadErrs := make([]error, len(names))

	var wg sync.WaitGroup
	for i, name := range names {
		wg.Add(1)
		go func(idx int, storeName string) {
			defer wg.Done()
			select {
			case <-ctx.Done():
				loadErrs[idx] = ctx.Err()
				return
			default:
			}

			treeDir := filepath.Join(dir, storeName)
			snapshot, err := OpenSnapshot(treeDir)
			if err != nil {
				loadErrs[idx] = fmt.Errorf("load tree %s: %w", storeName, err)
				return
			}

			tree := NewFromSnapshot(snapshot)
			trees[idx] = NamedTree{Tree: tree, Name: storeName}
		}(i, name)
	}
	wg.Wait()

	if err := errors.Join(loadErrs...); err != nil {
		// Close any successfully loaded snapshots.
		for _, t := range trees {
			if t.Tree != nil && t.Tree.snapshot != nil {
				_ = t.Tree.snapshot.Close()
			}
		}
		return nil, err
	}

	mt := NewMultiTree(trees)
	mt.lastVersion = version
	return mt, nil
}

// --- Types ---

// KVPair represents a single key-value mutation.
type KVPair struct {
	Key    []byte
	Value  []byte
	Delete bool
}

// NamedChangeSet groups mutations for a single store.
type NamedChangeSet struct {
	Name  string
	Pairs []KVPair
}
