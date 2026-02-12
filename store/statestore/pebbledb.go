package statestore

import (
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble"
)

// PebbleDB implements StateStore using PebbleDB with MVCC version-tagged keys.
//
// Key format: <storeKey>\x00<userKey>\x00<version-big-endian-8-bytes>
// The version is big-endian so that keys for the same user-key are sorted by version ascending.
// A tombstone is represented by a value of exactly 1 byte: [0x01].
type PebbleDB struct {
	db *pebble.DB

	latestVersion  atomic.Int64
	pendingVersion atomic.Int64

	// Async changeset application.
	asyncCh   chan asyncEntry
	asyncErr  atomic.Value // stores error
	asyncOnce sync.Once
	closeCh   chan struct{}
	closeWg   sync.WaitGroup
}

type asyncEntry struct {
	version    int64
	changesets []NamedChangeSet
}

var tombstoneValue = []byte{0x01}

const (
	metaLatestVersion = "__ss_latest_version"
	asyncBufferSize   = 256
)

// OpenPebbleDB opens or creates a PebbleDB state store at the given directory.
func OpenPebbleDB(dir string) (*PebbleDB, error) {
	opts := &pebble.Options{
		Comparer:           mvccComparer,
		FormatMajorVersion: pebble.FormatNewest,
	}

	db, err := pebble.Open(dir, opts)
	if err != nil {
		return nil, fmt.Errorf("open pebble: %w", err)
	}

	p := &PebbleDB{
		db:      db,
		asyncCh: make(chan asyncEntry, asyncBufferSize),
		closeCh: make(chan struct{}),
	}

	// Load latest version.
	val, closer, err := db.Get([]byte(metaLatestVersion))
	if err == nil {
		if len(val) == 8 {
			p.latestVersion.Store(int64(binary.BigEndian.Uint64(val)))
		}
		closer.Close()
	}

	// Start async worker.
	p.closeWg.Add(1)
	go p.asyncWorker()

	return p, nil
}

// --- StateStore interface ---

func (p *PebbleDB) Get(storeKey string, version int64, key []byte) ([]byte, error) {
	// Seek to the highest version <= requested version for this key.
	// Upper bound is version+1 (exclusive) so iter.Last() finds highest version <= target.
	iter, err := p.db.NewIter(&pebble.IterOptions{
		LowerBound: encodeMVCCKey(storeKey, key, 0),
		UpperBound: encodeMVCCKey(storeKey, key, uint64(version)+1),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	if !iter.Last() {
		return nil, nil // key doesn't exist at or before this version
	}

	val := iter.Value()
	if isTombstone(val) {
		return nil, nil
	}

	result := make([]byte, len(val))
	copy(result, val)
	return result, nil
}

func (p *PebbleDB) Has(storeKey string, version int64, key []byte) (bool, error) {
	v, err := p.Get(storeKey, version, key)
	if err != nil {
		return false, err
	}
	return v != nil, nil
}

func (p *PebbleDB) Iterator(storeKey string, version int64, start, end []byte) (Iterator, error) {
	return newMVCCIterator(p.db, storeKey, version, start, end, false)
}

func (p *PebbleDB) ReverseIterator(storeKey string, version int64, start, end []byte) (Iterator, error) {
	return newMVCCIterator(p.db, storeKey, version, start, end, true)
}

func (p *PebbleDB) ApplyChangesetSync(version int64, changesets []NamedChangeSet) error {
	batch := p.db.NewBatch()
	defer batch.Close()

	for _, cs := range changesets {
		for _, pair := range cs.Pairs {
			mvccKey := encodeMVCCKey(cs.Name, pair.Key, uint64(version))
			if pair.Delete {
				if err := batch.Set(mvccKey, tombstoneValue, pebble.Sync); err != nil {
					return err
				}
			} else {
				if err := batch.Set(mvccKey, pair.Value, pebble.Sync); err != nil {
					return err
				}
			}
		}
	}

	if err := batch.Commit(pebble.NoSync); err != nil {
		return err
	}

	// Update latest version.
	p.latestVersion.Store(version)
	return p.SetLatestVersion(version)
}

func (p *PebbleDB) ApplyChangesetAsync(version int64, changesets []NamedChangeSet) error {
	// Check for async errors.
	if errVal := p.asyncErr.Load(); errVal != nil {
		return errVal.(error)
	}

	// Make deep copies since caller may reuse slices.
	copied := make([]NamedChangeSet, len(changesets))
	for i, cs := range changesets {
		pairs := make([]KVPair, len(cs.Pairs))
		for j, p := range cs.Pairs {
			pairs[j] = KVPair{
				Key:    append([]byte(nil), p.Key...),
				Value:  append([]byte(nil), p.Value...),
				Delete: p.Delete,
			}
		}
		copied[i] = NamedChangeSet{Name: cs.Name, Pairs: pairs}
	}

	p.pendingVersion.Store(version)
	p.asyncCh <- asyncEntry{version: version, changesets: copied}
	return nil
}

func (p *PebbleDB) GetLatestVersion() (int64, error) {
	return p.latestVersion.Load(), nil
}

func (p *PebbleDB) SetLatestVersion(version int64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(version))
	return p.db.Set([]byte(metaLatestVersion), buf[:], pebble.NoSync)
}

func (p *PebbleDB) Prune(version int64) error {
	// Delete all entries with version < given version.
	// This is expensive; run in background.
	iter, err := p.db.NewIter(nil)
	if err != nil {
		return err
	}
	defer iter.Close()

	batch := p.db.NewBatch()
	defer batch.Close()

	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		_, v, ok := decodeMVCCKey(key)
		if !ok {
			continue
		}
		if int64(v) < version {
			if err := batch.Delete(key, nil); err != nil {
				return err
			}
			count++
			if count%10000 == 0 {
				if err := batch.Commit(pebble.NoSync); err != nil {
					return err
				}
				batch.Reset()
			}
		}
	}

	if count%10000 != 0 {
		return batch.Commit(pebble.NoSync)
	}
	return nil
}

func (p *PebbleDB) Close() error {
	close(p.closeCh)
	p.closeWg.Wait()
	return p.db.Close()
}

// --- Async worker ---

func (p *PebbleDB) asyncWorker() {
	defer p.closeWg.Done()

	for {
		select {
		case entry := <-p.asyncCh:
			if err := p.ApplyChangesetSync(entry.version, entry.changesets); err != nil {
				p.asyncErr.Store(err)
			}
		case <-p.closeCh:
			// Drain remaining entries.
			for {
				select {
				case entry := <-p.asyncCh:
					if err := p.ApplyChangesetSync(entry.version, entry.changesets); err != nil {
						p.asyncErr.Store(err)
					}
				default:
					return
				}
			}
		}
	}
}

// --- MVCC key encoding ---

// Key format: <storeKey>\x00<userKey>\x00<version-BE-8-bytes>
func encodeMVCCKey(storeKey string, userKey []byte, version uint64) []byte {
	buf := make([]byte, len(storeKey)+1+len(userKey)+1+8)
	n := copy(buf, storeKey)
	buf[n] = 0
	n++
	n += copy(buf[n:], userKey)
	buf[n] = 0
	n++
	binary.BigEndian.PutUint64(buf[n:], version)
	return buf
}

// decodeMVCCKey extracts (userKey, version, ok) from a MVCC key.
// Returns the raw userKey with store prefix stripped.
func decodeMVCCKey(key []byte) (userKey []byte, version uint64, ok bool) {
	if len(key) < 10 { // min: 1 store + \0 + 0 user + \0 + 8 version
		return nil, 0, false
	}
	version = binary.BigEndian.Uint64(key[len(key)-8:])
	// Find the second \0 separator (end of userKey).
	raw := key[:len(key)-8]
	if len(raw) == 0 || raw[len(raw)-1] != 0 {
		return nil, 0, false
	}
	return raw[:len(raw)-1], version, true
}

func isTombstone(val []byte) bool {
	return len(val) == 1 && val[0] == 0x01
}

// mvccComparer is a PebbleDB comparer that sorts by user key first, then version.
var mvccComparer = &pebble.Comparer{
	Compare: func(a, b []byte) int {
		aKey, aVer, aOK := decodeMVCCKey(a)
		bKey, bVer, bOK := decodeMVCCKey(b)

		if !aOK || !bOK {
			// Fall back to byte comparison for non-MVCC keys (metadata).
			return pebble.DefaultComparer.Compare(a, b)
		}

		if cmp := byteCompare(aKey, bKey); cmp != 0 {
			return cmp
		}
		// Same user key — sort by version ascending.
		if aVer < bVer {
			return -1
		}
		if aVer > bVer {
			return 1
		}
		return 0
	},
	Equal: func(a, b []byte) bool {
		return pebble.DefaultComparer.Equal(a, b)
	},
	AbbreviatedKey: func(key []byte) uint64 {
		return pebble.DefaultComparer.AbbreviatedKey(key)
	},
	FormatKey: pebble.DefaultComparer.FormatKey,
	Separator: func(dst, a, b []byte) []byte {
		return pebble.DefaultComparer.Separator(dst, a, b)
	},
	Successor: func(dst, a []byte) []byte {
		return pebble.DefaultComparer.Successor(dst, a)
	},
	// ImmediateSuccessor is not needed since we handle version ordering ourselves.
	Split: func(a []byte) int {
		return len(a) // No prefix compression — treat entire key as prefix.
	},
	Name: "mvcc-v1",
}

func byteCompare(a, b []byte) int {
	if len(a) < len(b) {
		for i := range a {
			if a[i] < b[i] {
				return -1
			}
			if a[i] > b[i] {
				return 1
			}
		}
		return -1
	}
	if len(a) > len(b) {
		for i := range b {
			if a[i] < b[i] {
				return -1
			}
			if a[i] > b[i] {
				return 1
			}
		}
		return 1
	}
	for i := range a {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	return 0
}

// --- MVCC Iterator ---

// mvccIterator wraps a PebbleDB iterator to provide MVCC-aware iteration.
// It deduplicates versions and returns only the latest version <= target version.
type mvccIterator struct {
	iter      *pebble.Iterator
	storeKey  string
	version   int64
	start     []byte
	end       []byte
	reverse   bool
	valid     bool
	err       error
	curKey    []byte
	curValue  []byte
}

func newMVCCIterator(db *pebble.DB, storeKey string, version int64, start, end []byte, reverse bool) (*mvccIterator, error) {
	lowerBound := encodeMVCCKey(storeKey, start, 0)
	var upperBound []byte
	if end != nil {
		upperBound = encodeMVCCKey(storeKey, end, 0)
	} else {
		// Upper bound: storeKey\x01 (next store prefix).
		upperBound = make([]byte, len(storeKey)+1)
		copy(upperBound, storeKey)
		upperBound[len(storeKey)] = 0x01
	}

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, err
	}

	it := &mvccIterator{
		iter:     iter,
		storeKey: storeKey,
		version:  version,
		start:    start,
		end:      end,
		reverse:  reverse,
	}

	if reverse {
		iter.Last()
	} else {
		iter.First()
	}
	it.advance()

	return it, nil
}

func (it *mvccIterator) advance() {
	it.valid = false
	it.curKey = nil
	it.curValue = nil

	prefix := it.storeKey + "\x00"

	for it.iter.Valid() {
		fullKey, ver, ok := decodeMVCCKey(it.iter.Key())
		if !ok {
			it.next()
			continue
		}

		// Extract user key from full key (strip store prefix).
		if len(fullKey) < len(prefix) {
			it.next()
			continue
		}

		if int64(ver) > it.version {
			// This version is newer than what we want — skip.
			it.next()
			continue
		}

		// Found a version <= target. Copy userKey and value since PebbleDB's
		// iter.Key()/Value() references are invalidated by Next()/Prev().
		rawUser := fullKey[len(prefix):]
		userKey := make([]byte, len(rawUser))
		copy(userKey, rawUser)
		bestVal := make([]byte, len(it.iter.Value()))
		copy(bestVal, it.iter.Value())

		if !it.reverse {
			// Forward: scan ahead for the same user key to find the highest version <= target.
			for {
				it.iter.Next()
				if !it.iter.Valid() {
					break
				}
				nextKey, nextVer, nextOK := decodeMVCCKey(it.iter.Key())
				if !nextOK || len(nextKey) < len(prefix) {
					break
				}
				nextUser := nextKey[len(prefix):]
				if !bytesEqual(nextUser, userKey) {
					break
				}
				if int64(nextVer) > it.version {
					break
				}
				// Better version found — copy value.
				bestVal = make([]byte, len(it.iter.Value()))
				copy(bestVal, it.iter.Value())
			}
			// Iterator is now positioned at the first entry past this user key's
			// valid versions (different user key, version > target, or invalid).
		} else {
			// Reverse: the first version <= target IS the highest (descending order).
			// Skip past remaining versions of this user key for the next advance() call.
			for {
				it.iter.Prev()
				if !it.iter.Valid() {
					break
				}
				nextKey, _, nextOK := decodeMVCCKey(it.iter.Key())
				if !nextOK || len(nextKey) < len(prefix) {
					break
				}
				nextUser := nextKey[len(prefix):]
				if !bytesEqual(nextUser, userKey) {
					break
				}
				// Same user key, lower version — skip.
			}
			// Iterator is now at a different user key or invalid.
		}

		if isTombstone(bestVal) {
			// Key was deleted — continue to next user key.
			continue
		}

		it.curKey = userKey
		it.curValue = bestVal
		it.valid = true
		return
	}
}

func (it *mvccIterator) next() {
	if it.reverse {
		it.iter.Prev()
	} else {
		it.iter.Next()
	}
}

func (it *mvccIterator) Valid() bool { return it.valid }

func (it *mvccIterator) Next() {
	it.advance()
}

func (it *mvccIterator) Key() []byte   { return it.curKey }
func (it *mvccIterator) Value() []byte { return it.curValue }
func (it *mvccIterator) Error() error  { return it.err }

func (it *mvccIterator) Close() error {
	return it.iter.Close()
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
