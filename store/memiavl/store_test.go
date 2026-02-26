package memiavl

import (
	"fmt"
	"testing"

	dbm "github.com/cosmos/cosmos-db"
	"github.com/cosmos/iavl"
	"github.com/stretchr/testify/require"

	"cosmossdk.io/log"
)

// TestHashCompatibility verifies that MemIAVL produces byte-identical root hashes
// compared to cosmos/iavl for the same key-value insertions at each version.
func TestHashCompatibility(t *testing.T) {
	// cosmos/iavl tree backed by in-memory DB
	db := dbm.NewMemDB()
	iavlTree := iavl.NewMutableTree(db, 0, true, log.NewNopLogger())

	// MemIAVL tree
	memTree := New()

	// Version 1: insert keys
	pairs := []struct{ k, v string }{
		{"alice", "100"},
		{"bob", "200"},
		{"charlie", "300"},
		{"dave", "400"},
		{"eve", "500"},
	}
	for _, p := range pairs {
		_, err := iavlTree.Set([]byte(p.k), []byte(p.v))
		require.NoError(t, err)
		memTree.Set([]byte(p.k), []byte(p.v))
	}

	iavlHash1, iavlVer1, err := iavlTree.SaveVersion()
	require.NoError(t, err)
	memHash1, memVer1, err := memTree.SaveVersion(true)
	require.NoError(t, err)

	require.Equal(t, iavlVer1, memVer1, "version mismatch at v1")
	require.Equal(t, iavlHash1, memHash1, "hash mismatch at v1")

	// Version 2: update and delete
	_, err = iavlTree.Set([]byte("alice"), []byte("150"))
	require.NoError(t, err)
	memTree.Set([]byte("alice"), []byte("150"))

	_, _, err = iavlTree.Remove([]byte("charlie"))
	require.NoError(t, err)
	memTree.Remove([]byte("charlie"))

	_, err = iavlTree.Set([]byte("frank"), []byte("600"))
	require.NoError(t, err)
	memTree.Set([]byte("frank"), []byte("600"))

	iavlHash2, iavlVer2, err := iavlTree.SaveVersion()
	require.NoError(t, err)
	memHash2, memVer2, err := memTree.SaveVersion(true)
	require.NoError(t, err)

	require.Equal(t, iavlVer2, memVer2, "version mismatch at v2")
	require.Equal(t, iavlHash2, memHash2, "hash mismatch at v2")

	// Version 3: more operations to stress rotations
	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("key_%03d", i))
		val := []byte(fmt.Sprintf("val_%03d", i))
		_, err = iavlTree.Set(key, val)
		require.NoError(t, err)
		memTree.Set(key, val)
	}

	iavlHash3, iavlVer3, err := iavlTree.SaveVersion()
	require.NoError(t, err)
	memHash3, memVer3, err := memTree.SaveVersion(true)
	require.NoError(t, err)

	require.Equal(t, iavlVer3, memVer3, "version mismatch at v3")
	require.Equal(t, iavlHash3, memHash3, "hash mismatch at v3")
}

// TestEmptyTreeHash verifies that empty tree hashes match cosmos/iavl.
func TestEmptyTreeHash(t *testing.T) {
	db := dbm.NewMemDB()
	iavlTree := iavl.NewMutableTree(db, 0, true, log.NewNopLogger())

	memTree := New()

	iavlHash := iavlTree.WorkingHash()
	memHash := memTree.RootHash()

	require.Equal(t, iavlHash, memHash, "empty tree hash mismatch")
}

// TestKVStoreConformance tests the Store adapter's KVStore interface.
func TestKVStoreConformance(t *testing.T) {
	s := NewStore()

	// Set and Get
	s.Set([]byte("key1"), []byte("value1"))
	require.Equal(t, []byte("value1"), s.Get([]byte("key1")))

	// Has
	require.True(t, s.Has([]byte("key1")))
	require.False(t, s.Has([]byte("nonexistent")))

	// Update
	s.Set([]byte("key1"), []byte("value1_updated"))
	require.Equal(t, []byte("value1_updated"), s.Get([]byte("key1")))

	// Delete
	s.Delete([]byte("key1"))
	require.Nil(t, s.Get([]byte("key1")))
	require.False(t, s.Has([]byte("key1")))

	// Delete nonexistent (should not panic)
	s.Delete([]byte("nonexistent"))

	// Get nonexistent
	require.Nil(t, s.Get([]byte("nonexistent")))
}

// TestIterator tests ascending and descending iteration.
func TestIterator(t *testing.T) {
	s := NewStore()
	s.Set([]byte("a"), []byte("1"))
	s.Set([]byte("b"), []byte("2"))
	s.Set([]byte("c"), []byte("3"))
	s.Set([]byte("d"), []byte("4"))
	s.Set([]byte("e"), []byte("5"))

	// Full ascending
	iter := s.Iterator(nil, nil)
	keys := collectKeys(iter)
	require.Equal(t, []string{"a", "b", "c", "d", "e"}, keys)

	// Full descending
	iter = s.ReverseIterator(nil, nil)
	keys = collectKeys(iter)
	require.Equal(t, []string{"e", "d", "c", "b", "a"}, keys)

	// Range ascending [b, d)
	iter = s.Iterator([]byte("b"), []byte("d"))
	keys = collectKeys(iter)
	require.Equal(t, []string{"b", "c"}, keys)

	// Range descending [b, d)
	iter = s.ReverseIterator([]byte("b"), []byte("d"))
	keys = collectKeys(iter)
	require.Equal(t, []string{"c", "b"}, keys)

	// Empty range
	iter = s.Iterator([]byte("f"), []byte("z"))
	require.False(t, iter.Valid())
	iter.Close()
}

func collectKeys(iter interface{ Valid() bool; Key() []byte; Next(); Close() error }) []string {
	defer iter.Close()
	var keys []string
	for ; iter.Valid(); iter.Next() {
		keys = append(keys, string(iter.Key()))
	}
	return keys
}

// TestCommit tests that Commit bumps version and returns hash.
func TestCommit(t *testing.T) {
	s := NewStore()

	s.Set([]byte("key"), []byte("value"))

	cid1 := s.Commit()
	require.Equal(t, int64(1), cid1.Version)
	require.NotEmpty(t, cid1.Hash)

	s.Set([]byte("key2"), []byte("value2"))

	cid2 := s.Commit()
	require.Equal(t, int64(2), cid2.Version)
	require.NotEmpty(t, cid2.Hash)
	require.NotEqual(t, cid1.Hash, cid2.Hash)

	// LastCommitID should match
	require.Equal(t, cid2, s.LastCommitID())
}

// TestWorkingHash tests that WorkingHash returns the root hash before commit.
func TestWorkingHash(t *testing.T) {
	s := NewStore()

	s.Set([]byte("key"), []byte("value"))

	wh := s.WorkingHash()
	require.NotEmpty(t, wh)

	// Commit should return the same hash
	cid := s.Commit()
	require.Equal(t, wh, cid.Hash)
}

// TestSetBatch tests the BatchSettable interface.
func TestSetBatch(t *testing.T) {
	s := NewStore()

	// Pre-populate
	s.Set([]byte("existing"), []byte("old_value"))

	pairs := []iavl.BatchPair{
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("b"), Value: []byte("2")},
		{Key: []byte("existing"), Value: []byte("new_value")},
		{Key: []byte("to_delete"), Value: []byte("temp")},
	}
	err := s.SetBatch(pairs)
	require.NoError(t, err)

	require.Equal(t, []byte("1"), s.Get([]byte("a")))
	require.Equal(t, []byte("2"), s.Get([]byte("b")))
	require.Equal(t, []byte("new_value"), s.Get([]byte("existing")))

	// Delete via SetBatch
	deletePairs := []iavl.BatchPair{
		{Key: []byte("to_delete"), Delete: true},
	}
	err = s.SetBatch(deletePairs)
	require.NoError(t, err)
	require.Nil(t, s.Get([]byte("to_delete")))
}

// TestSetBatchHashCompat verifies that SetBatch produces the same hash as individual Set calls.
func TestSetBatchHashCompat(t *testing.T) {
	// Store using individual Set calls
	s1 := NewStore()
	s1.Set([]byte("a"), []byte("1"))
	s1.Set([]byte("b"), []byte("2"))
	s1.Set([]byte("c"), []byte("3"))
	cid1 := s1.Commit()

	// Store using SetBatch
	s2 := NewStore()
	err := s2.SetBatch([]iavl.BatchPair{
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("b"), Value: []byte("2")},
		{Key: []byte("c"), Value: []byte("3")},
	})
	require.NoError(t, err)
	cid2 := s2.Commit()

	require.Equal(t, cid1.Hash, cid2.Hash)
}

// TestCopy verifies that Copy creates an independent snapshot.
// CoW is version-based, so SaveVersion must be called before Copy to activate it.
func TestCopy(t *testing.T) {
	tree := New()
	tree.Set([]byte("a"), []byte("1"))
	tree.Set([]byte("b"), []byte("2"))
	tree.SaveVersion(false) // bump version to activate CoW on next Copy

	snap := tree.Copy()

	// Modify original
	tree.Set([]byte("c"), []byte("3"))
	tree.Remove([]byte("a"))

	// Snapshot should be unchanged
	require.Equal(t, []byte("1"), snap.Get([]byte("a")))
	require.Equal(t, []byte("2"), snap.Get([]byte("b")))
	require.Nil(t, snap.Get([]byte("c")))
}

// TestStoreType verifies GetStoreType returns StoreTypeIAVL.
func TestStoreType(t *testing.T) {
	s := NewStore()
	require.Equal(t, "StoreTypeIAVL", s.GetStoreType().String())
}

// TestSetInitialVersion tests setting an initial version.
func TestSetInitialVersion(t *testing.T) {
	s := NewStore()
	s.SetInitialVersion(10)

	s.Set([]byte("key"), []byte("value"))
	cid := s.Commit()
	require.Equal(t, int64(10), cid.Version)

	s.Set([]byte("key2"), []byte("value2"))
	cid2 := s.Commit()
	require.Equal(t, int64(11), cid2.Version)
}
