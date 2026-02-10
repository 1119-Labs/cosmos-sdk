package memiavl

import (
	"io"

	"github.com/cosmos/iavl"

	"cosmossdk.io/store/cachekv"
	pruningtypes "cosmossdk.io/store/pruning/types"
	"cosmossdk.io/store/tracekv"
	"cosmossdk.io/store/types"
)

var (
	_ types.KVStore                 = (*Store)(nil)
	_ types.CommitStore             = (*Store)(nil)
	_ types.CommitKVStore           = (*Store)(nil)
	_ types.StoreWithInitialVersion = (*Store)(nil)
)

// Store wraps a MemIAVL Tree as a Cosmos SDK CommitKVStore.
type Store struct {
	tree         *Tree
	lastCommitID types.CommitID
}

// NewStore creates a new MemIAVL store at genesis version.
func NewStore() *Store {
	return &Store{tree: New()}
}

// NewStoreWithVersion creates a new MemIAVL store at the given version.
func NewStoreWithVersion(version uint64, initialVersion uint32) *Store {
	return &Store{tree: NewEmptyTree(version, initialVersion)}
}

// --- types.Store ---

func (s *Store) GetStoreType() types.StoreType { return types.StoreTypeIAVL }

func (s *Store) CacheWrap() types.CacheWrap { return cachekv.NewStore(s) }

func (s *Store) CacheWrapWithTrace(w io.Writer, tc types.TraceContext) types.CacheWrap {
	return cachekv.NewStore(tracekv.NewStore(s, w, tc))
}

// --- types.BasicKVStore ---

func (s *Store) Get(key []byte) []byte { return s.tree.Get(key) }
func (s *Store) Has(key []byte) bool   { return s.tree.Has(key) }

func (s *Store) Set(key, value []byte) {
	types.AssertValidKey(key)
	types.AssertValidValue(value)
	s.tree.Set(key, value)
}

func (s *Store) Delete(key []byte) { s.tree.Remove(key) }

// --- types.KVStore ---

func (s *Store) Iterator(start, end []byte) types.Iterator {
	return s.tree.Iterator(start, end, true)
}

func (s *Store) ReverseIterator(start, end []byte) types.Iterator {
	return s.tree.Iterator(start, end, false)
}

// --- types.Committer ---

func (s *Store) Commit() types.CommitID {
	hash, version, err := s.tree.SaveVersion(true)
	if err != nil {
		panic(err)
	}
	s.lastCommitID = types.CommitID{Version: version, Hash: hash}
	return s.lastCommitID
}

func (s *Store) LastCommitID() types.CommitID { return s.lastCommitID }

func (s *Store) WorkingHash() []byte { return s.tree.RootHash() }

func (s *Store) SetPruning(_ pruningtypes.PruningOptions) {}

func (s *Store) GetPruning() pruningtypes.PruningOptions {
	return pruningtypes.NewPruningOptions(pruningtypes.PruningNothing)
}

// --- types.StoreWithInitialVersion ---

func (s *Store) SetInitialVersion(version int64) {
	_ = s.tree.SetInitialVersion(version)
}

// --- BatchSettable (Block-STM integration) ---

// SetBatch applies multiple key-value pairs efficiently, holding the tree lock once.
// Pairs with Delete=true are removed; others are set.
func (s *Store) SetBatch(pairs []iavl.BatchPair) error {
	s.tree.mtx.Lock()
	defer s.tree.mtx.Unlock()
	for i := range pairs {
		if pairs[i].Delete {
			s.tree.remove(pairs[i].Key)
		} else {
			s.tree.set(pairs[i].Key, pairs[i].Value)
		}
	}
	return nil
}

// IsInMemory implements the InMemoryStore interface.
// MemIAVL keeps all state in memory, so block-level read caching is unnecessary.
func (s *Store) IsInMemory() bool { return true }

// Tree returns the underlying tree (for testing).
func (s *Store) Tree() *Tree { return s.tree }
