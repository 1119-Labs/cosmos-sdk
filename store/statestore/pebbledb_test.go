package statestore

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPebbleDBBasicGetSet(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenPebbleDB(dir)
	require.NoError(t, err)
	defer db.Close()

	// Version 1.
	err = db.ApplyChangesetSync(1, []NamedChangeSet{
		{Name: "bank", Pairs: []KVPair{
			{Key: []byte("balance/alice"), Value: []byte("100")},
			{Key: []byte("balance/bob"), Value: []byte("200")},
		}},
	})
	require.NoError(t, err)

	// Get at version 1.
	val, err := db.Get("bank", 1, []byte("balance/alice"))
	require.NoError(t, err)
	require.Equal(t, []byte("100"), val)

	val, err = db.Get("bank", 1, []byte("balance/bob"))
	require.NoError(t, err)
	require.Equal(t, []byte("200"), val)

	// Nonexistent key.
	val, err = db.Get("bank", 1, []byte("balance/charlie"))
	require.NoError(t, err)
	require.Nil(t, val)

	// Nonexistent store.
	val, err = db.Get("auth", 1, []byte("balance/alice"))
	require.NoError(t, err)
	require.Nil(t, val)
}

func TestPebbleDBVersionedReads(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenPebbleDB(dir)
	require.NoError(t, err)
	defer db.Close()

	// Version 1.
	require.NoError(t, db.ApplyChangesetSync(1, []NamedChangeSet{
		{Name: "bank", Pairs: []KVPair{
			{Key: []byte("key"), Value: []byte("v1")},
		}},
	}))

	// Version 2: update.
	require.NoError(t, db.ApplyChangesetSync(2, []NamedChangeSet{
		{Name: "bank", Pairs: []KVPair{
			{Key: []byte("key"), Value: []byte("v2")},
		}},
	}))

	// Version 3: update again.
	require.NoError(t, db.ApplyChangesetSync(3, []NamedChangeSet{
		{Name: "bank", Pairs: []KVPair{
			{Key: []byte("key"), Value: []byte("v3")},
		}},
	}))

	// Time-travel queries.
	val, err := db.Get("bank", 1, []byte("key"))
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), val)

	val, err = db.Get("bank", 2, []byte("key"))
	require.NoError(t, err)
	require.Equal(t, []byte("v2"), val)

	val, err = db.Get("bank", 3, []byte("key"))
	require.NoError(t, err)
	require.Equal(t, []byte("v3"), val)

	// Query at version 0 (before any writes).
	val, err = db.Get("bank", 0, []byte("key"))
	require.NoError(t, err)
	require.Nil(t, val)
}

func TestPebbleDBDelete(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenPebbleDB(dir)
	require.NoError(t, err)
	defer db.Close()

	// Version 1: create.
	require.NoError(t, db.ApplyChangesetSync(1, []NamedChangeSet{
		{Name: "bank", Pairs: []KVPair{
			{Key: []byte("a"), Value: []byte("1")},
			{Key: []byte("b"), Value: []byte("2")},
		}},
	}))

	// Version 2: delete "a".
	require.NoError(t, db.ApplyChangesetSync(2, []NamedChangeSet{
		{Name: "bank", Pairs: []KVPair{
			{Key: []byte("a"), Delete: true},
		}},
	}))

	// At version 1: "a" exists.
	val, err := db.Get("bank", 1, []byte("a"))
	require.NoError(t, err)
	require.Equal(t, []byte("1"), val)

	// At version 2: "a" is deleted.
	val, err = db.Get("bank", 2, []byte("a"))
	require.NoError(t, err)
	require.Nil(t, val)

	// "b" still exists at version 2.
	val, err = db.Get("bank", 2, []byte("b"))
	require.NoError(t, err)
	require.Equal(t, []byte("2"), val)
}

func TestPebbleDBHas(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenPebbleDB(dir)
	require.NoError(t, err)
	defer db.Close()

	require.NoError(t, db.ApplyChangesetSync(1, []NamedChangeSet{
		{Name: "bank", Pairs: []KVPair{
			{Key: []byte("x"), Value: []byte("y")},
		}},
	}))

	has, err := db.Has("bank", 1, []byte("x"))
	require.NoError(t, err)
	require.True(t, has)

	has, err = db.Has("bank", 1, []byte("z"))
	require.NoError(t, err)
	require.False(t, has)
}

func TestPebbleDBLatestVersion(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenPebbleDB(dir)
	require.NoError(t, err)

	v, err := db.GetLatestVersion()
	require.NoError(t, err)
	require.Equal(t, int64(0), v)

	require.NoError(t, db.ApplyChangesetSync(5, []NamedChangeSet{
		{Name: "bank", Pairs: []KVPair{
			{Key: []byte("a"), Value: []byte("1")},
		}},
	}))

	v, err = db.GetLatestVersion()
	require.NoError(t, err)
	require.Equal(t, int64(5), v)

	require.NoError(t, db.Close())

	// Reopen and verify persisted version.
	db2, err := OpenPebbleDB(dir)
	require.NoError(t, err)
	defer db2.Close()

	v, err = db2.GetLatestVersion()
	require.NoError(t, err)
	require.Equal(t, int64(5), v)
}

func TestPebbleDBIterator(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenPebbleDB(dir)
	require.NoError(t, err)
	defer db.Close()

	require.NoError(t, db.ApplyChangesetSync(1, []NamedChangeSet{
		{Name: "bank", Pairs: []KVPair{
			{Key: []byte("a"), Value: []byte("1")},
			{Key: []byte("b"), Value: []byte("2")},
			{Key: []byte("c"), Value: []byte("3")},
			{Key: []byte("d"), Value: []byte("4")},
		}},
	}))

	// Full ascending.
	iter, err := db.Iterator("bank", 1, nil, nil)
	require.NoError(t, err)
	var keys, vals []string
	for iter.Valid() {
		keys = append(keys, string(iter.Key()))
		vals = append(vals, string(iter.Value()))
		iter.Next()
	}
	require.NoError(t, iter.Close())
	require.Equal(t, []string{"a", "b", "c", "d"}, keys)
	require.Equal(t, []string{"1", "2", "3", "4"}, vals)
}

func TestPebbleDBIteratorVersioned(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenPebbleDB(dir)
	require.NoError(t, err)
	defer db.Close()

	// Version 1: a=1, b=2.
	require.NoError(t, db.ApplyChangesetSync(1, []NamedChangeSet{
		{Name: "s", Pairs: []KVPair{
			{Key: []byte("a"), Value: []byte("1")},
			{Key: []byte("b"), Value: []byte("2")},
		}},
	}))

	// Version 2: a=updated, b=deleted, c=3.
	require.NoError(t, db.ApplyChangesetSync(2, []NamedChangeSet{
		{Name: "s", Pairs: []KVPair{
			{Key: []byte("a"), Value: []byte("updated")},
			{Key: []byte("b"), Delete: true},
			{Key: []byte("c"), Value: []byte("3")},
		}},
	}))

	// Iterate at version 1.
	iter, err := db.Iterator("s", 1, nil, nil)
	require.NoError(t, err)
	var keys []string
	for iter.Valid() {
		keys = append(keys, string(iter.Key()))
		iter.Next()
	}
	iter.Close()
	require.Equal(t, []string{"a", "b"}, keys)

	// Iterate at version 2.
	iter, err = db.Iterator("s", 2, nil, nil)
	require.NoError(t, err)
	keys = nil
	for iter.Valid() {
		keys = append(keys, string(iter.Key()))
		iter.Next()
	}
	iter.Close()
	require.Equal(t, []string{"a", "c"}, keys)
}

func TestPebbleDBMultiStore(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenPebbleDB(dir)
	require.NoError(t, err)
	defer db.Close()

	require.NoError(t, db.ApplyChangesetSync(1, []NamedChangeSet{
		{Name: "bank", Pairs: []KVPair{
			{Key: []byte("key1"), Value: []byte("bank_val")},
		}},
		{Name: "auth", Pairs: []KVPair{
			{Key: []byte("key1"), Value: []byte("auth_val")},
		}},
	}))

	// Same key name in different stores should return different values.
	val, err := db.Get("bank", 1, []byte("key1"))
	require.NoError(t, err)
	require.Equal(t, []byte("bank_val"), val)

	val, err = db.Get("auth", 1, []byte("key1"))
	require.NoError(t, err)
	require.Equal(t, []byte("auth_val"), val)
}

func TestPebbleDBAsync(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenPebbleDB(dir)
	require.NoError(t, err)

	// Apply changes asynchronously.
	for v := int64(1); v <= 10; v++ {
		require.NoError(t, db.ApplyChangesetAsync(v, []NamedChangeSet{
			{Name: "bank", Pairs: []KVPair{
				{Key: []byte("counter"), Value: []byte{byte(v)}},
			}},
		}))
	}

	// Close to flush async writes.
	require.NoError(t, db.Close())

	// Reopen and verify.
	db2, err := OpenPebbleDB(dir)
	require.NoError(t, err)
	defer db2.Close()

	val, err := db2.Get("bank", 10, []byte("counter"))
	require.NoError(t, err)
	require.Equal(t, []byte{10}, val)

	// Verify time-travel still works.
	val, err = db2.Get("bank", 5, []byte("counter"))
	require.NoError(t, err)
	require.Equal(t, []byte{5}, val)
}

func TestPebbleDBAsyncError(t *testing.T) {
	dir := t.TempDir()
	db, err := OpenPebbleDB(dir)
	require.NoError(t, err)
	defer db.Close()

	// Normal async operation should not error.
	err = db.ApplyChangesetAsync(1, []NamedChangeSet{
		{Name: "s", Pairs: []KVPair{{Key: []byte("k"), Value: []byte("v")}}},
	})
	require.NoError(t, err)

	// Wait briefly for async processing.
	time.Sleep(50 * time.Millisecond)

	// Subsequent call should also succeed.
	err = db.ApplyChangesetAsync(2, []NamedChangeSet{
		{Name: "s", Pairs: []KVPair{{Key: []byte("k"), Value: []byte("v2")}}},
	})
	require.NoError(t, err)
}
