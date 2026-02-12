package statestore

import "io"

// StateStore is a versioned key-value store for historical state queries.
// It serves as the State Store (SS) layer in a dual-layer architecture where
// State Commitment (SC) handles consensus hashes via MemIAVL/IAVL, and
// SS handles historical state storage and queries.
type StateStore interface {
	// Get returns the value for a key at the given version.
	// Returns nil if the key doesn't exist or was deleted at that version.
	Get(storeKey string, version int64, key []byte) ([]byte, error)

	// Has returns whether a key exists at the given version.
	Has(storeKey string, version int64, key []byte) (bool, error)

	// Iterator returns an ascending iterator over [start, end) at the given version.
	Iterator(storeKey string, version int64, start, end []byte) (Iterator, error)

	// ReverseIterator returns a descending iterator over [start, end) at the given version.
	ReverseIterator(storeKey string, version int64, start, end []byte) (Iterator, error)

	// ApplyChangesetSync applies changesets synchronously.
	ApplyChangesetSync(version int64, changesets []NamedChangeSet) error

	// ApplyChangesetAsync queues changesets for background application.
	// Returns immediately; errors are reported on next call.
	ApplyChangesetAsync(version int64, changesets []NamedChangeSet) error

	// GetLatestVersion returns the latest version that has been applied.
	GetLatestVersion() (int64, error)

	// SetLatestVersion sets the latest version marker.
	SetLatestVersion(version int64) error

	// Prune removes all data for versions older than the given version.
	Prune(version int64) error

	io.Closer
}

// Iterator is a key-value iterator for the state store.
type Iterator interface {
	// Valid returns whether the iterator is positioned at a valid entry.
	Valid() bool

	// Next advances the iterator.
	Next()

	// Key returns the key at the current position.
	Key() []byte

	// Value returns the value at the current position.
	Value() []byte

	// Error returns any error that occurred during iteration.
	Error() error

	// Close releases the iterator resources.
	Close() error
}

// KVPair is a key-value mutation.
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
