package memiavl

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// --- Snapshot Tests ---

// TestSnapshotWriteReadRoundTrip writes a tree to snapshot and verifies it loads back correctly.
func TestSnapshotWriteReadRoundTrip(t *testing.T) {
	tree := New()
	pairs := []struct{ k, v string }{
		{"alice", "100"},
		{"bob", "200"},
		{"charlie", "300"},
		{"dave", "400"},
		{"eve", "500"},
	}
	for _, p := range pairs {
		tree.Set([]byte(p.k), []byte(p.v))
	}
	hash1, ver1, err := tree.SaveVersion(true)
	require.NoError(t, err)
	require.Equal(t, int64(1), ver1)

	// Write snapshot.
	dir := t.TempDir()
	snapshotDir := filepath.Join(dir, "snap1")
	err = tree.WriteSnapshot(context.Background(), snapshotDir)
	require.NoError(t, err)

	// Load snapshot.
	snapshot, err := OpenSnapshot(snapshotDir)
	require.NoError(t, err)
	defer snapshot.Close()

	require.False(t, snapshot.IsEmpty())
	require.Equal(t, uint32(1), snapshot.SnapshotVersion())
	require.Equal(t, hash1, snapshot.RootHash())

	// Create tree from snapshot and verify all keys.
	loaded := NewFromSnapshot(snapshot)
	for _, p := range pairs {
		require.Equal(t, []byte(p.v), loaded.Get([]byte(p.k)), "key: %s", p.k)
	}
	require.Nil(t, loaded.Get([]byte("nonexistent")))
	require.Equal(t, ver1, loaded.Version())
}

// TestSnapshotEmpty verifies snapshot of an empty tree.
func TestSnapshotEmpty(t *testing.T) {
	tree := New()
	tree.SaveVersion(true)

	dir := t.TempDir()
	snapshotDir := filepath.Join(dir, "snap-empty")
	err := tree.WriteSnapshot(context.Background(), snapshotDir)
	require.NoError(t, err)

	snapshot, err := OpenSnapshot(snapshotDir)
	require.NoError(t, err)
	defer snapshot.Close()

	require.True(t, snapshot.IsEmpty())
}

// TestSnapshotSingleKey verifies snapshot with a single leaf.
func TestSnapshotSingleKey(t *testing.T) {
	tree := New()
	tree.Set([]byte("only"), []byte("key"))
	hash, _, err := tree.SaveVersion(true)
	require.NoError(t, err)

	dir := t.TempDir()
	snapshotDir := filepath.Join(dir, "snap-single")
	err = tree.WriteSnapshot(context.Background(), snapshotDir)
	require.NoError(t, err)

	snapshot, err := OpenSnapshot(snapshotDir)
	require.NoError(t, err)
	defer snapshot.Close()

	require.Equal(t, hash, snapshot.RootHash())

	loaded := NewFromSnapshot(snapshot)
	require.Equal(t, []byte("key"), loaded.Get([]byte("only")))
}

// TestSnapshotAfterMutations writes a snapshot after set/delete operations.
func TestSnapshotAfterMutations(t *testing.T) {
	tree := New()
	for i := 0; i < 50; i++ {
		tree.Set([]byte{byte(i)}, []byte{byte(i * 2)})
	}
	tree.SaveVersion(true)

	// Mutate: delete some, update some, add new
	for i := 0; i < 10; i++ {
		tree.Remove([]byte{byte(i)})
	}
	for i := 10; i < 20; i++ {
		tree.Set([]byte{byte(i)}, []byte{byte(i * 3)})
	}
	for i := 50; i < 60; i++ {
		tree.Set([]byte{byte(i)}, []byte{byte(i)})
	}
	hash2, _, err := tree.SaveVersion(true)
	require.NoError(t, err)

	dir := t.TempDir()
	snapshotDir := filepath.Join(dir, "snap-mutated")
	err = tree.WriteSnapshot(context.Background(), snapshotDir)
	require.NoError(t, err)

	snapshot, err := OpenSnapshot(snapshotDir)
	require.NoError(t, err)
	defer snapshot.Close()

	require.Equal(t, hash2, snapshot.RootHash())

	loaded := NewFromSnapshot(snapshot)
	// Deleted keys (0-9) should be absent.
	for i := 0; i < 10; i++ {
		require.Nil(t, loaded.Get([]byte{byte(i)}), "deleted key %d present", i)
	}
	// Updated keys (10-19).
	for i := 10; i < 20; i++ {
		require.Equal(t, []byte{byte(i * 3)}, loaded.Get([]byte{byte(i)}), "key %d", i)
	}
	// Unchanged keys (20-49).
	for i := 20; i < 50; i++ {
		require.Equal(t, []byte{byte(i * 2)}, loaded.Get([]byte{byte(i)}), "key %d", i)
	}
	// New keys (50-59).
	for i := 50; i < 60; i++ {
		require.Equal(t, []byte{byte(i)}, loaded.Get([]byte{byte(i)}), "key %d", i)
	}
}

// TestSnapshotMutateLoaded verifies that a tree loaded from snapshot can be mutated
// and produces correct hashes (CoW with persisted nodes).
func TestSnapshotMutateLoaded(t *testing.T) {
	// Create original tree.
	tree := New()
	tree.Set([]byte("a"), []byte("1"))
	tree.Set([]byte("b"), []byte("2"))
	tree.Set([]byte("c"), []byte("3"))
	tree.SaveVersion(true)

	dir := t.TempDir()
	snapshotDir := filepath.Join(dir, "snap-cow")
	err := tree.WriteSnapshot(context.Background(), snapshotDir)
	require.NoError(t, err)

	snapshot, err := OpenSnapshot(snapshotDir)
	require.NoError(t, err)

	loaded := NewFromSnapshot(snapshot)

	// Mutate the loaded tree.
	loaded.Set([]byte("d"), []byte("4"))
	loaded.Remove([]byte("a"))
	loaded.Set([]byte("b"), []byte("updated"))

	loadedHash, _, err := loaded.SaveVersion(true)
	require.NoError(t, err)
	require.NotEmpty(t, loadedHash)

	// Verify values.
	require.Nil(t, loaded.Get([]byte("a")))
	require.Equal(t, []byte("updated"), loaded.Get([]byte("b")))
	require.Equal(t, []byte("3"), loaded.Get([]byte("c")))
	require.Equal(t, []byte("4"), loaded.Get([]byte("d")))
}

// --- WAL Tests ---

// TestWALWriteReadSync tests synchronous WAL write/read.
func TestWALWriteReadSync(t *testing.T) {
	dir := t.TempDir()
	wal, err := OpenWAL(dir, 0) // sync mode
	require.NoError(t, err)

	// Write entries.
	for v := int64(1); v <= 5; v++ {
		entry := WALEntry{
			Version: v,
			ChangeSets: []NamedChangeSet{
				{
					Name: "store1",
					Pairs: []KVPair{
						{Key: []byte("key"), Value: []byte{byte(v)}},
					},
				},
			},
		}
		require.NoError(t, wal.Write(entry))
	}
	require.NoError(t, wal.Close())

	// Reopen and read.
	wal2, err := OpenWAL(dir, 0)
	require.NoError(t, err)
	defer wal2.Close()

	entries, err := wal2.ReadAll()
	require.NoError(t, err)
	require.Len(t, entries, 5)

	for i, e := range entries {
		require.Equal(t, int64(i+1), e.Version)
		require.Len(t, e.ChangeSets, 1)
		require.Equal(t, "store1", e.ChangeSets[0].Name)
		require.Len(t, e.ChangeSets[0].Pairs, 1)
		require.Equal(t, []byte{byte(i + 1)}, e.ChangeSets[0].Pairs[0].Value)
	}
}

// TestWALWriteReadAsync tests async WAL write/read.
func TestWALWriteReadAsync(t *testing.T) {
	dir := t.TempDir()
	wal, err := OpenWAL(dir, 10) // async mode, buffer=10
	require.NoError(t, err)

	for v := int64(1); v <= 20; v++ {
		entry := WALEntry{
			Version: v,
			ChangeSets: []NamedChangeSet{
				{
					Name: "bank",
					Pairs: []KVPair{
						{Key: []byte("balance"), Value: []byte{byte(v)}},
					},
				},
			},
		}
		require.NoError(t, wal.Write(entry))
	}

	// Close flushes pending writes.
	require.NoError(t, wal.Close())

	// Read back.
	wal2, err := OpenWAL(dir, 0)
	require.NoError(t, err)
	defer wal2.Close()

	entries, err := wal2.ReadAll()
	require.NoError(t, err)
	require.Len(t, entries, 20)

	for i, e := range entries {
		require.Equal(t, int64(i+1), e.Version)
	}
}

// TestWALTruncate tests WAL truncation.
func TestWALTruncate(t *testing.T) {
	dir := t.TempDir()
	wal, err := OpenWAL(dir, 0)
	require.NoError(t, err)

	for v := int64(1); v <= 10; v++ {
		require.NoError(t, wal.Write(WALEntry{
			Version: v,
			ChangeSets: []NamedChangeSet{
				{Name: "s", Pairs: []KVPair{{Key: []byte("k"), Value: []byte{byte(v)}}}},
			},
		}))
	}
	require.NoError(t, wal.Close())

	// Reopen and truncate entries before version 5.
	wal2, err := OpenWAL(dir, 0)
	require.NoError(t, err)
	require.NoError(t, wal2.TruncateBefore(5))
	require.NoError(t, wal2.Close())

	// Read back — should have only entries >= version 5.
	wal3, err := OpenWAL(dir, 0)
	require.NoError(t, err)
	defer wal3.Close()

	entries, err := wal3.ReadAll()
	require.NoError(t, err)
	// TruncateBefore(5) should keep only entries with version >= 5.
	require.Len(t, entries, 6) // versions 5,6,7,8,9,10
	for _, e := range entries {
		require.GreaterOrEqual(t, e.Version, int64(5))
	}
}

// TestWALTruncateBeforePreservesPostSnapshotEntries verifies that TruncateBefore
// does NOT lose entries >= target version even when they share a segment with
// entries < target version. This was the root cause of a crash-loop bug where
// WAL entries between snapshot version and the segment boundary were lost.
func TestWALTruncateBeforePreservesPostSnapshotEntries(t *testing.T) {
	dir := t.TempDir()
	wal, err := OpenWAL(dir, 0)
	require.NoError(t, err)

	// Write 50 entries (versions 1-50) — all in one segment since segmentMaxEntries=10000.
	for v := int64(1); v <= 50; v++ {
		require.NoError(t, wal.Write(WALEntry{
			Version: v,
			ChangeSets: []NamedChangeSet{
				{Name: "bank", Pairs: []KVPair{{Key: []byte(fmt.Sprintf("k%d", v)), Value: []byte{byte(v)}}}},
			},
		}))
	}
	require.NoError(t, wal.Close())

	// Simulate snapshot at version 30: TruncateBefore(30) should keep entries 30-50.
	wal2, err := OpenWAL(dir, 0)
	require.NoError(t, err)
	require.NoError(t, wal2.TruncateBefore(30))
	require.NoError(t, wal2.Close())

	// Read back and verify entries 30-50 are preserved.
	wal3, err := OpenWAL(dir, 0)
	require.NoError(t, err)
	defer wal3.Close()

	entries, err := wal3.ReadAll()
	require.NoError(t, err)

	// Must have exactly 21 entries (versions 30-50).
	require.Len(t, entries, 21, "expected entries for versions 30-50")
	for i, e := range entries {
		expectedVersion := int64(30 + i)
		require.Equal(t, expectedVersion, e.Version, "entry %d has wrong version", i)
		require.Len(t, e.ChangeSets, 1)
		require.Equal(t, "bank", e.ChangeSets[0].Name)
	}
}

// TestWALSegmentRotation verifies that segments are rotated at segmentMaxEntries.
func TestWALSegmentRotation(t *testing.T) {
	dir := t.TempDir()
	wal, err := OpenWAL(dir, 0)
	require.NoError(t, err)

	// Write segmentMaxEntries + 5 entries to trigger rotation.
	total := segmentMaxEntries + 5
	for v := int64(1); v <= int64(total); v++ {
		require.NoError(t, wal.Write(WALEntry{
			Version:    v,
			ChangeSets: []NamedChangeSet{{Name: "s", Pairs: []KVPair{{Key: []byte("k"), Value: []byte{1}}}}},
		}))
	}
	require.NoError(t, wal.Close())

	// Should have 2 segments now.
	wal2, err := OpenWAL(dir, 0)
	require.NoError(t, err)
	defer wal2.Close()

	entries, err := wal2.ReadAll()
	require.NoError(t, err)
	require.Len(t, entries, total)

	// Check segment files.
	segs, err := wal2.listSegments()
	require.NoError(t, err)
	require.Len(t, segs, 2, "expected 2 segments after rotation")

	// First segment starts at version 1.
	require.Equal(t, int64(1), segmentVersion(segs[0]))
	// Second segment starts at version segmentMaxEntries+1.
	require.Equal(t, int64(segmentMaxEntries+1), segmentVersion(segs[1]))
}

// TestWALDeleteValues tests WAL with delete operations.
func TestWALDeleteValues(t *testing.T) {
	dir := t.TempDir()
	wal, err := OpenWAL(dir, 0)
	require.NoError(t, err)

	entry := WALEntry{
		Version: 1,
		ChangeSets: []NamedChangeSet{
			{
				Name: "bank",
				Pairs: []KVPair{
					{Key: []byte("balance/alice"), Value: []byte("100")},
					{Key: []byte("balance/bob"), Delete: true},
				},
			},
			{
				Name: "staking",
				Pairs: []KVPair{
					{Key: []byte("validator/abc"), Value: []byte("1000")},
				},
			},
		},
	}
	require.NoError(t, wal.Write(entry))
	require.NoError(t, wal.Close())

	wal2, err := OpenWAL(dir, 0)
	require.NoError(t, err)
	defer wal2.Close()

	entries, err := wal2.ReadAll()
	require.NoError(t, err)
	require.Len(t, entries, 1)

	e := entries[0]
	require.Equal(t, int64(1), e.Version)
	require.Len(t, e.ChangeSets, 2)

	// Check bank changeset.
	require.Equal(t, "bank", e.ChangeSets[0].Name)
	require.Len(t, e.ChangeSets[0].Pairs, 2)
	require.Equal(t, []byte("balance/alice"), e.ChangeSets[0].Pairs[0].Key)
	require.Equal(t, []byte("100"), e.ChangeSets[0].Pairs[0].Value)
	require.False(t, e.ChangeSets[0].Pairs[0].Delete)
	require.Equal(t, []byte("balance/bob"), e.ChangeSets[0].Pairs[1].Key)
	require.True(t, e.ChangeSets[0].Pairs[1].Delete)

	// Check staking changeset.
	require.Equal(t, "staking", e.ChangeSets[1].Name)
}

// --- MultiTree Tests ---

// TestMultiTreeSaveVersionParallel tests parallel SaveVersion across trees.
func TestMultiTreeSaveVersionParallel(t *testing.T) {
	// Create 6 trees (triggers parallel path with threshold > 4).
	storeNames := []string{"bank", "auth", "staking", "slashing", "distribution", "gov"}
	mt := NewEmptyMultiTree(storeNames)

	for _, name := range storeNames {
		tree := mt.TreeByName(name)
		require.NotNil(t, tree, "tree %s not found", name)
		tree.Set([]byte("key1"), []byte(name+"_value1"))
	}

	version, err := mt.SaveVersion(true)
	require.NoError(t, err)
	require.Equal(t, int64(1), version)

	// All trees should have version 1 and non-empty hashes.
	for _, nt := range mt.Trees() {
		require.Equal(t, int64(1), nt.Tree.Version())
		require.NotEmpty(t, nt.Tree.RootHash())
	}
}

// TestMultiTreeCopy tests copy-on-write for MultiTree.
func TestMultiTreeCopy(t *testing.T) {
	mt := NewEmptyMultiTree([]string{"bank", "auth"})
	mt.TreeByName("bank").Set([]byte("a"), []byte("1"))
	mt.TreeByName("auth").Set([]byte("b"), []byte("2"))
	mt.SaveVersion(true)

	snap := mt.Copy()

	// Mutate original.
	mt.TreeByName("bank").Set([]byte("c"), []byte("3"))

	// Snapshot should be independent.
	require.Nil(t, snap.TreeByName("bank").Get([]byte("c")))
	require.Equal(t, []byte("1"), snap.TreeByName("bank").Get([]byte("a")))
}

// --- DB Tests ---

// TestDBOpenCommitReopen tests the full DB lifecycle: open, commit, reopen.
func TestDBOpenCommitReopen(t *testing.T) {
	dir := t.TempDir()
	config := DBConfig{
		SnapshotInterval: 0, // disable auto snapshots for this test
		WALBufferSize:    0, // sync WAL
		WALDir:           "changelog",
		InitialStores:    []string{"bank", "auth"},
	}

	// First open — creates empty trees.
	db, err := OpenDB(dir, config)
	require.NoError(t, err)

	// Commit version 1.
	changeSets := []NamedChangeSet{
		{Name: "bank", Pairs: []KVPair{
			{Key: []byte("balance/alice"), Value: []byte("100")},
			{Key: []byte("balance/bob"), Value: []byte("200")},
		}},
		{Name: "auth", Pairs: []KVPair{
			{Key: []byte("account/alice"), Value: []byte("addr1")},
		}},
	}
	version, err := db.Commit(changeSets)
	require.NoError(t, err)
	require.Equal(t, int64(1), version)

	// Verify data.
	require.Equal(t, []byte("100"), db.MultiTree.TreeByName("bank").Get([]byte("balance/alice")))
	require.Equal(t, []byte("addr1"), db.MultiTree.TreeByName("auth").Get([]byte("account/alice")))

	// Write snapshot for later reopen.
	require.NoError(t, db.ForceSnapshot())
	require.NoError(t, db.Close())

	// Reopen — should load from snapshot.
	db2, err := OpenDB(dir, config)
	require.NoError(t, err)
	defer db2.Close()

	require.Equal(t, int64(1), db2.CommittedVersion())
	require.Equal(t, []byte("100"), db2.MultiTree.TreeByName("bank").Get([]byte("balance/alice")))
	require.Equal(t, []byte("addr1"), db2.MultiTree.TreeByName("auth").Get([]byte("account/alice")))
}

// TestDBWALReplay tests that WAL entries are replayed on reopen.
func TestDBWALReplay(t *testing.T) {
	dir := t.TempDir()
	config := DBConfig{
		SnapshotInterval: 0, // no auto snapshots
		WALBufferSize:    0,
		WALDir:           "changelog",
		InitialStores:    []string{"bank"},
	}

	// Open and commit several versions without snapshots.
	db, err := OpenDB(dir, config)
	require.NoError(t, err)

	for v := 1; v <= 5; v++ {
		_, err := db.Commit([]NamedChangeSet{
			{Name: "bank", Pairs: []KVPair{
				{Key: []byte("key"), Value: []byte{byte(v)}},
			}},
		})
		require.NoError(t, err)
	}
	require.NoError(t, db.Close())

	// Reopen — should replay WAL entries.
	db2, err := OpenDB(dir, config)
	require.NoError(t, err)
	defer db2.Close()

	require.Equal(t, int64(5), db2.CommittedVersion())
	require.Equal(t, []byte{5}, db2.MultiTree.TreeByName("bank").Get([]byte("key")))
}

// TestDBSnapshotThenWAL tests snapshot + WAL replay on reopen.
func TestDBSnapshotThenWAL(t *testing.T) {
	dir := t.TempDir()
	config := DBConfig{
		SnapshotInterval: 0,
		WALBufferSize:    0,
		WALDir:           "changelog",
		InitialStores:    []string{"store1"},
	}

	db, err := OpenDB(dir, config)
	require.NoError(t, err)

	// Commit versions 1-3.
	for v := 1; v <= 3; v++ {
		_, err := db.Commit([]NamedChangeSet{
			{Name: "store1", Pairs: []KVPair{
				{Key: []byte("ver"), Value: []byte{byte(v)}},
			}},
		})
		require.NoError(t, err)
	}

	// Force snapshot at version 3.
	require.NoError(t, db.ForceSnapshot())

	// Commit versions 4-6 (after snapshot).
	for v := 4; v <= 6; v++ {
		_, err := db.Commit([]NamedChangeSet{
			{Name: "store1", Pairs: []KVPair{
				{Key: []byte("ver"), Value: []byte{byte(v)}},
			}},
		})
		require.NoError(t, err)
	}
	require.NoError(t, db.Close())

	// Reopen — loads snapshot@3, replays WAL 4-6.
	db2, err := OpenDB(dir, config)
	require.NoError(t, err)
	defer db2.Close()

	require.Equal(t, int64(6), db2.CommittedVersion())
	require.Equal(t, []byte{6}, db2.MultiTree.TreeByName("store1").Get([]byte("ver")))
}

// TestDBCommitWithDeletesAndUpdates tests DB commit with mixed operations.
func TestDBCommitWithDeletesAndUpdates(t *testing.T) {
	dir := t.TempDir()
	config := DBConfig{
		SnapshotInterval: 0,
		WALBufferSize:    0,
		WALDir:           "changelog",
		InitialStores:    []string{"bank"},
	}

	db, err := OpenDB(dir, config)
	require.NoError(t, err)

	// Version 1: insert.
	_, err = db.Commit([]NamedChangeSet{
		{Name: "bank", Pairs: []KVPair{
			{Key: []byte("a"), Value: []byte("1")},
			{Key: []byte("b"), Value: []byte("2")},
			{Key: []byte("c"), Value: []byte("3")},
		}},
	})
	require.NoError(t, err)

	// Version 2: update + delete.
	_, err = db.Commit([]NamedChangeSet{
		{Name: "bank", Pairs: []KVPair{
			{Key: []byte("a"), Value: []byte("updated")},
			{Key: []byte("b"), Delete: true},
			{Key: []byte("d"), Value: []byte("4")},
		}},
	})
	require.NoError(t, err)

	tree := db.MultiTree.TreeByName("bank")
	require.Equal(t, []byte("updated"), tree.Get([]byte("a")))
	require.Nil(t, tree.Get([]byte("b")))
	require.Equal(t, []byte("3"), tree.Get([]byte("c")))
	require.Equal(t, []byte("4"), tree.Get([]byte("d")))

	require.NoError(t, db.Close())
}

// TestStoreChangeTracking tests that Store tracks changes for WAL.
func TestStoreChangeTracking(t *testing.T) {
	tree := New()
	store := NewStoreFromTree(tree, true)

	store.Set([]byte("a"), []byte("1"))
	store.Set([]byte("b"), []byte("2"))
	store.Delete([]byte("a"))

	changes := store.FlushPendingChanges()
	require.Len(t, changes, 3)
	require.Equal(t, []byte("a"), changes[0].Key)
	require.Equal(t, []byte("1"), changes[0].Value)
	require.False(t, changes[0].Delete)
	require.Equal(t, []byte("b"), changes[1].Key)
	require.True(t, changes[2].Delete)

	// After flush, pending should be empty.
	changes2 := store.FlushPendingChanges()
	require.Nil(t, changes2)
}

// TestDBHashConsistency verifies that DB commit produces the same hash as direct tree operations.
func TestDBHashConsistency(t *testing.T) {
	dir := t.TempDir()
	config := DBConfig{
		SnapshotInterval: 0,
		WALBufferSize:    0,
		WALDir:           "changelog",
		InitialStores:    []string{"bank"},
	}

	db, err := OpenDB(dir, config)
	require.NoError(t, err)
	defer db.Close()

	// Also build the same tree directly.
	directTree := New()

	cs := []NamedChangeSet{
		{Name: "bank", Pairs: []KVPair{
			{Key: []byte("alice"), Value: []byte("100")},
			{Key: []byte("bob"), Value: []byte("200")},
		}},
	}
	_, err = db.Commit(cs)
	require.NoError(t, err)

	directTree.Set([]byte("alice"), []byte("100"))
	directTree.Set([]byte("bob"), []byte("200"))
	directHash, _, _ := directTree.SaveVersion(true)

	dbHash := db.MultiTree.TreeByName("bank").RootHash()
	require.Equal(t, directHash, dbHash, "DB and direct tree hashes differ")
}

// --- WAL Marshal/Unmarshal Test ---

func TestWALEntryMarshalRoundTrip(t *testing.T) {
	entry := WALEntry{
		Version: 42,
		ChangeSets: []NamedChangeSet{
			{
				Name: "bank",
				Pairs: []KVPair{
					{Key: []byte("k1"), Value: []byte("v1")},
					{Key: []byte("k2"), Delete: true},
					{Key: []byte("k3"), Value: []byte("v3_long_value_data")},
				},
			},
			{
				Name: "auth",
				Pairs: []KVPair{
					{Key: []byte("account"), Value: []byte("data")},
				},
			},
		},
	}

	data := marshalWALEntry(entry)
	decoded, err := unmarshalWALEntry(data)
	require.NoError(t, err)

	require.Equal(t, entry.Version, decoded.Version)
	require.Len(t, decoded.ChangeSets, 2)

	require.Equal(t, "bank", decoded.ChangeSets[0].Name)
	require.Len(t, decoded.ChangeSets[0].Pairs, 3)
	require.Equal(t, []byte("k1"), decoded.ChangeSets[0].Pairs[0].Key)
	require.Equal(t, []byte("v1"), decoded.ChangeSets[0].Pairs[0].Value)
	require.False(t, decoded.ChangeSets[0].Pairs[0].Delete)
	require.True(t, decoded.ChangeSets[0].Pairs[1].Delete)
	require.Nil(t, decoded.ChangeSets[0].Pairs[1].Value) // deleted key has nil value

	require.Equal(t, "auth", decoded.ChangeSets[1].Name)
}

// TestSnapshotIterator verifies iteration over a snapshot-backed tree.
func TestSnapshotIterator(t *testing.T) {
	tree := New()
	tree.Set([]byte("a"), []byte("1"))
	tree.Set([]byte("b"), []byte("2"))
	tree.Set([]byte("c"), []byte("3"))
	tree.Set([]byte("d"), []byte("4"))
	tree.Set([]byte("e"), []byte("5"))
	tree.SaveVersion(true)

	dir := t.TempDir()
	snapshotDir := filepath.Join(dir, "snap-iter")
	err := tree.WriteSnapshot(context.Background(), snapshotDir)
	require.NoError(t, err)

	snapshot, err := OpenSnapshot(snapshotDir)
	require.NoError(t, err)
	defer snapshot.Close()

	loaded := NewFromSnapshot(snapshot)

	// Full ascending.
	iter := loaded.Iterator(nil, nil, true)
	var keys []string
	for ; iter.Valid(); iter.Next() {
		keys = append(keys, string(iter.Key()))
	}
	iter.Close()
	require.Equal(t, []string{"a", "b", "c", "d", "e"}, keys)

	// Range [b, d).
	iter = loaded.Iterator([]byte("b"), []byte("d"), true)
	keys = nil
	for ; iter.Valid(); iter.Next() {
		keys = append(keys, string(iter.Key()))
	}
	iter.Close()
	require.Equal(t, []string{"b", "c"}, keys)

	// Descending.
	iter = loaded.Iterator(nil, nil, false)
	keys = nil
	for ; iter.Valid(); iter.Next() {
		keys = append(keys, string(iter.Key()))
	}
	iter.Close()
	require.Equal(t, []string{"e", "d", "c", "b", "a"}, keys)
}

// TestDBMultipleStores tests DB with multiple stores and WAL replay.
func TestDBMultipleStores(t *testing.T) {
	dir := t.TempDir()
	stores := []string{"bank", "auth", "staking", "gov", "distribution"}
	config := DBConfig{
		SnapshotInterval: 0,
		WALBufferSize:    0,
		WALDir:           "changelog",
		InitialStores:    stores,
	}

	db, err := OpenDB(dir, config)
	require.NoError(t, err)

	// Commit with all stores.
	var changeSets []NamedChangeSet
	for _, name := range stores {
		changeSets = append(changeSets, NamedChangeSet{
			Name: name,
			Pairs: []KVPair{
				{Key: []byte("key"), Value: []byte(name + "_value")},
			},
		})
	}
	_, err = db.Commit(changeSets)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	// Reopen and verify.
	db2, err := OpenDB(dir, config)
	require.NoError(t, err)
	defer db2.Close()

	for _, name := range stores {
		tree := db2.MultiTree.TreeByName(name)
		require.NotNil(t, tree, "store %s missing", name)
		require.Equal(t, []byte(name+"_value"), tree.Get([]byte("key")), "store %s", name)
	}
}

// TestSnapshotMultiTree tests MultiTree snapshot write and load.
func TestSnapshotMultiTree(t *testing.T) {
	mt := NewEmptyMultiTree([]string{"a", "b", "c"})
	mt.TreeByName("a").Set([]byte("k1"), []byte("v1"))
	mt.TreeByName("b").Set([]byte("k2"), []byte("v2"))
	mt.TreeByName("c").Set([]byte("k3"), []byte("v3"))
	_, err := mt.SaveVersion(true)
	require.NoError(t, err)

	dir := t.TempDir()
	snapDir := filepath.Join(dir, "multitree-snap")

	err = mt.WriteSnapshot(context.Background(), snapDir)
	require.NoError(t, err)
	err = mt.WriteMultiTreeMetadata(snapDir)
	require.NoError(t, err)

	// Load.
	loaded, err := LoadMultiTree(context.Background(), snapDir)
	require.NoError(t, err)

	require.Equal(t, mt.Version(), loaded.Version())
	require.Equal(t, []byte("v1"), loaded.TreeByName("a").Get([]byte("k1")))
	require.Equal(t, []byte("v2"), loaded.TreeByName("b").Get([]byte("k2")))
	require.Equal(t, []byte("v3"), loaded.TreeByName("c").Get([]byte("k3")))

	// Close snapshot handles.
	for _, nt := range loaded.Trees() {
		nt.Tree.Close()
	}
}

// TestWALMarshalEmpty tests marshal/unmarshal of empty entry.
func TestWALMarshalEmpty(t *testing.T) {
	entry := WALEntry{Version: 1, ChangeSets: nil}
	data := marshalWALEntry(entry)
	decoded, err := unmarshalWALEntry(data)
	require.NoError(t, err)
	require.Equal(t, int64(1), decoded.Version)
	require.Empty(t, decoded.ChangeSets)
}

// TestWALMarshalLargeKey tests WAL with large keys/values.
func TestWALMarshalLargeKey(t *testing.T) {
	bigKey := make([]byte, 1024)
	bigVal := make([]byte, 4096)
	for i := range bigKey {
		bigKey[i] = byte(i % 256)
	}
	for i := range bigVal {
		bigVal[i] = byte((i * 7) % 256)
	}

	entry := WALEntry{
		Version: 999,
		ChangeSets: []NamedChangeSet{
			{
				Name:  "bigstore",
				Pairs: []KVPair{{Key: bigKey, Value: bigVal}},
			},
		},
	}
	data := marshalWALEntry(entry)
	decoded, err := unmarshalWALEntry(data)
	require.NoError(t, err)
	require.Equal(t, bigKey, decoded.ChangeSets[0].Pairs[0].Key)
	require.Equal(t, bigVal, decoded.ChangeSets[0].Pairs[0].Value)
}

// TestMultiTreeWriteSnapshotLoadCheckFiles verifies snapshot files exist.
func TestMultiTreeWriteSnapshotLoadCheckFiles(t *testing.T) {
	mt := NewEmptyMultiTree([]string{"store1"})
	mt.TreeByName("store1").Set([]byte("x"), []byte("y"))
	mt.SaveVersion(true)

	dir := t.TempDir()
	snapDir := filepath.Join(dir, "check-files")
	require.NoError(t, mt.WriteSnapshot(context.Background(), snapDir))
	require.NoError(t, mt.WriteMultiTreeMetadata(snapDir))

	// Check file existence.
	for _, name := range []string{"__metadata", "store1"} {
		path := filepath.Join(snapDir, name)
		_, err := os.Stat(path)
		require.NoError(t, err, "missing: %s", path)
	}
	for _, fname := range []string{FileNameMetadata, FileNameNodes, FileNameLeaves, FileNameKVs} {
		path := filepath.Join(snapDir, "store1", fname)
		_, err := os.Stat(path)
		require.NoError(t, err, "missing: %s", path)
	}
}

// --- WAL TruncateAfter Tests ---

func TestWALTruncateAfter(t *testing.T) {
	dir := t.TempDir()
	wal, err := OpenWAL(dir, 0)
	require.NoError(t, err)

	// Write 5 entries (versions 1-5).
	for i := int64(1); i <= 5; i++ {
		err := wal.Write(WALEntry{
			Version: i,
			ChangeSets: []NamedChangeSet{{
				Name:  "store",
				Pairs: []KVPair{{Key: []byte("k"), Value: []byte{byte(i)}}},
			}},
		})
		require.NoError(t, err)
	}

	// Truncate after version 3.
	err = wal.TruncateAfter(3)
	require.NoError(t, err)

	// Read back — should only have versions 1-3.
	entries, err := wal.ReadAll()
	require.NoError(t, err)
	require.Len(t, entries, 3)
	require.Equal(t, int64(1), entries[0].Version)
	require.Equal(t, int64(2), entries[1].Version)
	require.Equal(t, int64(3), entries[2].Version)

	require.NoError(t, wal.Close())
}

// --- DB Rollback Tests ---

func TestDBRollback(t *testing.T) {
	dir := t.TempDir()
	config := DefaultDBConfig()
	config.InitialStores = []string{"bank", "staking"}
	config.SnapshotInterval = 0 // no auto-snapshots

	db, err := OpenDB(dir, config)
	require.NoError(t, err)

	// Commit 5 versions with different values.
	for i := int64(1); i <= 5; i++ {
		_, err := db.Commit([]NamedChangeSet{
			{Name: "bank", Pairs: []KVPair{{Key: []byte("balance"), Value: []byte{byte(i * 10)}}}},
			{Name: "staking", Pairs: []KVPair{{Key: []byte("power"), Value: []byte{byte(i)}}}},
		})
		require.NoError(t, err)
	}
	require.Equal(t, int64(5), db.CommittedVersion())

	// Rollback to version 3.
	err = db.Rollback(3)
	require.NoError(t, err)
	require.Equal(t, int64(3), db.CommittedVersion())

	// Verify data matches version 3 state.
	bankTree := db.MultiTree.TreeByName("bank")
	require.NotNil(t, bankTree)
	val := bankTree.Get([]byte("balance"))
	require.Equal(t, []byte{30}, val) // 3 * 10

	stakingTree := db.MultiTree.TreeByName("staking")
	require.NotNil(t, stakingTree)
	val = stakingTree.Get([]byte("power"))
	require.Equal(t, []byte{3}, val)

	// Close and reopen — should still be at version 3.
	require.NoError(t, db.Close())

	db2, err := OpenDB(dir, config)
	require.NoError(t, err)
	defer db2.Close()

	require.Equal(t, int64(3), db2.CommittedVersion())

	bankTree = db2.MultiTree.TreeByName("bank")
	val = bankTree.Get([]byte("balance"))
	require.Equal(t, []byte{30}, val)
}

func TestDBRollbackNoOp(t *testing.T) {
	dir := t.TempDir()
	config := DefaultDBConfig()
	config.InitialStores = []string{"store1"}
	config.SnapshotInterval = 0

	db, err := OpenDB(dir, config)
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Commit([]NamedChangeSet{
		{Name: "store1", Pairs: []KVPair{{Key: []byte("k"), Value: []byte("v")}}},
	})
	require.NoError(t, err)

	// Rollback to current version — should be a no-op.
	err = db.Rollback(1)
	require.NoError(t, err)
	require.Equal(t, int64(1), db.CommittedVersion())

	// Rollback to future version — also no-op.
	err = db.Rollback(100)
	require.NoError(t, err)
	require.Equal(t, int64(1), db.CommittedVersion())
}
