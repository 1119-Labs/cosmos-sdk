package memiavl

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	SnapshotPrefix = "snapshot-"
	SnapshotDirLen = len(SnapshotPrefix) + 20 // max uint64 decimal digits
	currentLink    = "current"
	tmpPrefix      = "tmp-"
)

// DBConfig holds configuration for the DB lifecycle.
type DBConfig struct {
	// SnapshotInterval is the number of blocks between snapshot rewrites.
	// 0 disables automatic snapshots.
	SnapshotInterval uint32

	// SnapshotKeepRecent is the number of old snapshots to keep.
	SnapshotKeepRecent uint32

	// SnapshotMinTimeInterval is the minimum wall-clock time between snapshots.
	SnapshotMinTimeInterval time.Duration

	// WALBufferSize is the async WAL write buffer size. 0 = sync writes.
	WALBufferSize int

	// WALDir is the subdirectory for WAL files. Default: "changelog".
	WALDir string

	// InitialStores is the list of store names to create on first open.
	InitialStores []string

	// ReadOnly prevents mutations.
	ReadOnly bool
}

// DefaultDBConfig returns a config with sensible defaults.
func DefaultDBConfig() DBConfig {
	return DBConfig{
		SnapshotInterval:        100,
		SnapshotKeepRecent:      1,
		SnapshotMinTimeInterval: 30 * time.Second,
		WALBufferSize:           0, // sync writes — must reach disk before Commit returns
		WALDir:                  "changelog",
	}
}

// DB manages the lifecycle of a MultiTree with snapshot persistence and WAL.
type DB struct {
	*MultiTree

	dir    string
	config DBConfig

	// WAL for crash recovery.
	wal *WAL

	// Background snapshot rewrite state.
	snapshotRewriteChan       chan snapshotResult
	snapshotRewriteCancelFunc context.CancelFunc
	lastSnapshotTime          time.Time
	snapshotVersion           int64 // version of the current snapshot on disk

	mtx    sync.Mutex
	closed bool
}

type snapshotResult struct {
	newMT *MultiTree
	err   error
}

// OpenDB opens or creates a DB at the given directory.
// On first open (no current snapshot), creates empty trees from config.InitialStores.
// On subsequent opens, loads the latest snapshot and replays WAL entries.
func OpenDB(dir string, config DBConfig) (*DB, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	db := &DB{
		dir:              dir,
		config:           config,
		lastSnapshotTime: time.Now(),
	}

	// Try to load the latest snapshot.
	currentPath := filepath.Join(dir, currentLink)
	snapshotName, err := os.Readlink(currentPath)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("read current link: %w", err)
		}
		// No snapshot — create from scratch.
		if len(config.InitialStores) == 0 {
			return nil, fmt.Errorf("no snapshot found and no initial stores configured at %s", dir)
		}
		db.MultiTree = NewEmptyMultiTree(config.InitialStores)
	} else {
		snapshotDir := filepath.Join(dir, snapshotName)
		mt, err := LoadMultiTree(context.Background(), snapshotDir)
		if err != nil {
			return nil, fmt.Errorf("load snapshot %s: %w", snapshotDir, err)
		}
		db.MultiTree = mt
		db.snapshotVersion = mt.Version()
	}

	// Open WAL and replay.
	walDir := filepath.Join(dir, config.WALDir)
	wal, err := OpenWAL(walDir, config.WALBufferSize)
	if err != nil {
		return nil, fmt.Errorf("open WAL: %w", err)
	}
	db.wal = wal

	// Replay WAL entries after the snapshot version.
	if err := db.replayWAL(); err != nil {
		_ = wal.Close()
		return nil, fmt.Errorf("replay WAL: %w", err)
	}

	return db, nil
}

// replayWAL replays all WAL entries with versions > snapshot version.
func (db *DB) replayWAL() error {
	entries, err := db.wal.ReadAll()
	if err != nil {
		return err
	}

	snapshotVersion := db.snapshotVersion
	for _, entry := range entries {
		if entry.Version <= snapshotVersion {
			continue
		}
		if err := db.MultiTree.ApplyChangeSets(entry.ChangeSets); err != nil {
			return fmt.Errorf("apply changeset at version %d: %w", entry.Version, err)
		}
		// Compute hashes during replay to ensure the tree state is identical
		// to what was committed originally. Without this, the hash computed later
		// in loadVersionMemIAVL may differ from the original commit hash.
		if _, err := db.MultiTree.SaveVersion(true); err != nil {
			return fmt.Errorf("save version %d: %w", entry.Version, err)
		}
	}
	return nil
}

// Commit applies changesets, writes to WAL, bumps version, and triggers
// background snapshot rewrite if applicable.
func (db *DB) Commit(changeSets []NamedChangeSet) (int64, error) {
	db.mtx.Lock()
	defer db.mtx.Unlock()

	if db.closed {
		return 0, errors.New("db is closed")
	}

	// Apply changesets.
	if err := db.MultiTree.ApplyChangeSets(changeSets); err != nil {
		return 0, fmt.Errorf("apply changesets: %w", err)
	}

	// SaveVersion — increment version and compute hashes.
	version, err := db.MultiTree.SaveVersion(true)
	if err != nil {
		return 0, fmt.Errorf("save version: %w", err)
	}

	// Write to WAL AFTER SaveVersion succeeds.
	// On crash between SaveVersion and WAL write: the version is lost,
	// but state is consistent (no partial writes).
	walEntry := WALEntry{
		Version:    version,
		ChangeSets: changeSets,
	}
	if err := db.wal.Write(walEntry); err != nil {
		return 0, fmt.Errorf("write WAL: %w", err)
	}

	// Check for async snapshot rewrite completion.
	if err := db.checkBackgroundSnapshotRewrite(); err != nil {
		return 0, fmt.Errorf("check snapshot rewrite: %w", err)
	}

	// Trigger new snapshot rewrite if applicable.
	if err := db.rewriteIfApplicable(version); err != nil {
		return 0, fmt.Errorf("rewrite snapshot: %w", err)
	}

	return version, nil
}

// CommitWithoutApply finalizes a version when changesets were already applied
// directly to the trees (e.g., via Store.Set/Delete).
// It performs SaveVersion + WAL write + snapshot lifecycle without ApplyChangeSets.
func (db *DB) CommitWithoutApply(changeSets []NamedChangeSet) (int64, error) {
	db.mtx.Lock()
	defer db.mtx.Unlock()

	if db.closed {
		return 0, errors.New("db is closed")
	}

	// SaveVersion — increment version and compute hashes.
	version, err := db.MultiTree.SaveVersion(true)
	if err != nil {
		return 0, fmt.Errorf("save version: %w", err)
	}

	// Write to WAL AFTER SaveVersion succeeds.
	walEntry := WALEntry{
		Version:    version,
		ChangeSets: changeSets,
	}
	if err := db.wal.Write(walEntry); err != nil {
		return 0, fmt.Errorf("write WAL: %w", err)
	}

	// Check for async snapshot rewrite completion.
	if err := db.checkBackgroundSnapshotRewrite(); err != nil {
		return 0, fmt.Errorf("check snapshot rewrite: %w", err)
	}

	// Trigger new snapshot rewrite if applicable.
	if err := db.rewriteIfApplicable(version); err != nil {
		return 0, fmt.Errorf("rewrite snapshot: %w", err)
	}

	return version, nil
}

// CommittedVersion returns the latest committed version.
func (db *DB) CommittedVersion() int64 {
	db.mtx.Lock()
	defer db.mtx.Unlock()
	return db.MultiTree.Version()
}

// rewriteIfApplicable triggers a background snapshot rewrite if conditions are met.
func (db *DB) rewriteIfApplicable(version int64) error {
	if db.config.SnapshotInterval == 0 {
		return nil
	}
	if db.snapshotRewriteChan != nil {
		return nil // already in progress
	}

	blocksSinceSnapshot := version - db.snapshotVersion
	if blocksSinceSnapshot < int64(db.config.SnapshotInterval) {
		return nil
	}
	if version%int64(db.config.SnapshotInterval) != 0 {
		return nil
	}
	if db.config.SnapshotMinTimeInterval > 0 && time.Since(db.lastSnapshotTime) < db.config.SnapshotMinTimeInterval {
		return nil
	}

	return db.rewriteSnapshotBackground()
}

// rewriteSnapshotBackground clones the current tree and writes a snapshot in a goroutine.
func (db *DB) rewriteSnapshotBackground() error {
	cloned := db.MultiTree.Copy()
	version := cloned.Version()
	resultCh := make(chan snapshotResult, 1)
	ctx, cancel := context.WithCancel(context.Background())

	db.snapshotRewriteChan = resultCh
	db.snapshotRewriteCancelFunc = cancel

	go func() {
		defer cancel()

		snapshotName := fmt.Sprintf("%s%020d", SnapshotPrefix, version)
		tmpDir := filepath.Join(db.dir, tmpPrefix+snapshotName)
		finalDir := filepath.Join(db.dir, snapshotName)

		// Write snapshot to temp directory.
		if err := cloned.WriteSnapshot(ctx, tmpDir); err != nil {
			resultCh <- snapshotResult{err: fmt.Errorf("write snapshot: %w", err)}
			return
		}

		// Write multi-tree metadata.
		if err := cloned.WriteMultiTreeMetadata(tmpDir); err != nil {
			resultCh <- snapshotResult{err: fmt.Errorf("write metadata: %w", err)}
			return
		}

		// Atomic rename tmp -> final.
		if err := os.Rename(tmpDir, finalDir); err != nil {
			resultCh <- snapshotResult{err: fmt.Errorf("rename snapshot: %w", err)}
			return
		}

		// Load the new snapshot.
		newMT, err := LoadMultiTree(ctx, finalDir)
		if err != nil {
			resultCh <- snapshotResult{err: fmt.Errorf("reload snapshot: %w", err)}
			return
		}

		resultCh <- snapshotResult{newMT: newMT}
	}()

	return nil
}

// checkBackgroundSnapshotRewrite checks if a background snapshot rewrite has completed.
func (db *DB) checkBackgroundSnapshotRewrite() error {
	if db.snapshotRewriteChan == nil {
		return nil
	}

	select {
	case result := <-db.snapshotRewriteChan:
		db.snapshotRewriteChan = nil
		db.snapshotRewriteCancelFunc = nil

		if result.err != nil {
			return result.err
		}

		// The new snapshot was written and loaded from disk. We do NOT switch
		// db.MultiTree to the new snapshot-loaded tree because the Store objects
		// in rootmulti hold direct references to the current trees. Replacing
		// db.MultiTree would cause Store.Set to write to the old trees while
		// CommitWithoutApply operates on the new trees, producing wrong hashes.
		//
		// Instead, we only:
		// 1. Update the "current" symlink so restart loads the latest snapshot
		// 2. Truncate WAL entries before the snapshot version
		// 3. Update snapshotVersion so the next snapshot triggers correctly
		// The live tree continues to grow in memory; the snapshot is for restart.
		newMT := result.newMT
		snapshotVersion := newMT.Version()

		// Close the snapshot-loaded trees — we don't need them for live execution.
		for _, nt := range newMT.Trees() {
			if nt.Tree != nil {
				_ = nt.Tree.Close()
			}
		}

		db.snapshotVersion = snapshotVersion
		db.lastSnapshotTime = time.Now()

		// Update the "current" symlink.
		snapshotName := fmt.Sprintf("%s%020d", SnapshotPrefix, snapshotVersion)
		if err := updateCurrentLink(db.dir, snapshotName); err != nil {
			return fmt.Errorf("update current link: %w", err)
		}

		// Truncate WAL entries before the snapshot version.
		if err := db.wal.TruncateBefore(snapshotVersion); err != nil {
			return fmt.Errorf("truncate WAL: %w", err)
		}

		// Prune old snapshots in background.
		if db.config.SnapshotKeepRecent > 0 {
			go db.pruneSnapshots(snapshotVersion)
		}

	default:
		// Not done yet.
	}

	return nil
}

// Close closes the DB, releasing all resources.
func (db *DB) Close() error {
	db.mtx.Lock()
	defer db.mtx.Unlock()

	if db.closed {
		return nil
	}
	db.closed = true

	// Cancel any in-progress snapshot rewrite.
	if db.snapshotRewriteCancelFunc != nil {
		db.snapshotRewriteCancelFunc()
	}

	var errs []error
	if db.wal != nil {
		errs = append(errs, db.wal.Close())
	}
	// Close all tree snapshots.
	for _, t := range db.MultiTree.Trees() {
		if t.Tree != nil {
			errs = append(errs, t.Tree.Close())
		}
	}
	return errors.Join(errs...)
}

// updateCurrentLink atomically updates the "current" symlink.
func updateCurrentLink(dir, snapshotName string) error {
	tmpLink := filepath.Join(dir, currentLink+".tmp")
	_ = os.Remove(tmpLink)
	if err := os.Symlink(snapshotName, tmpLink); err != nil {
		return err
	}
	return os.Rename(tmpLink, filepath.Join(dir, currentLink))
}

// pruneSnapshots removes old snapshots, keeping the most recent ones.
func (db *DB) pruneSnapshots(currentVersion int64) {
	entries, err := os.ReadDir(db.dir)
	if err != nil {
		return
	}

	type snapshotEntry struct {
		name    string
		version int64
	}

	var snapshots []snapshotEntry
	for _, e := range entries {
		if !e.IsDir() || !strings.HasPrefix(e.Name(), SnapshotPrefix) {
			continue
		}
		vStr := strings.TrimPrefix(e.Name(), SnapshotPrefix)
		v, err := strconv.ParseInt(strings.TrimLeft(vStr, "0"), 10, 64)
		if err != nil {
			continue
		}
		if v == currentVersion {
			continue // never prune the current snapshot
		}
		snapshots = append(snapshots, snapshotEntry{name: e.Name(), version: v})
	}

	// Keep the most recent ones.
	keep := int(db.config.SnapshotKeepRecent)
	if len(snapshots) <= keep {
		return
	}

	// Remove oldest snapshots.
	for i := 0; i < len(snapshots)-keep; i++ {
		_ = os.RemoveAll(filepath.Join(db.dir, snapshots[i].name))
	}
}

// Rollback rolls back the DB state to the given target version.
// It re-loads from the latest snapshot (if snapshotVersion <= target), replays
// WAL entries up to targetVersion, and truncates all WAL entries after target.
// This is used to recover from partial commits where MemIAVL advanced beyond
// the version that CometBFT finalized.
func (db *DB) Rollback(targetVersion int64) error {
	db.mtx.Lock()
	defer db.mtx.Unlock()

	if db.closed {
		return errors.New("db is closed")
	}

	currentVersion := db.MultiTree.Version()
	if targetVersion >= currentVersion {
		return nil // nothing to do
	}

	if targetVersion < db.snapshotVersion {
		return fmt.Errorf("cannot rollback to version %d: below snapshot version %d", targetVersion, db.snapshotVersion)
	}

	// Cancel any in-progress snapshot rewrite — it may reference stale state.
	if db.snapshotRewriteCancelFunc != nil {
		db.snapshotRewriteCancelFunc()
		db.snapshotRewriteChan = nil
		db.snapshotRewriteCancelFunc = nil
	}

	// Re-load from the current snapshot, or create empty trees if no snapshot exists.
	var newMT *MultiTree
	currentPath := filepath.Join(db.dir, currentLink)
	snapshotName, err := os.Readlink(currentPath)
	if err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("read current link for rollback: %w", err)
		}
		// No snapshot — rebuild from initial stores.
		newMT = NewEmptyMultiTree(db.config.InitialStores)
	} else {
		snapshotDir := filepath.Join(db.dir, snapshotName)
		mt, loadErr := LoadMultiTree(context.Background(), snapshotDir)
		if loadErr != nil {
			return fmt.Errorf("reload snapshot for rollback: %w", loadErr)
		}
		newMT = mt
	}

	// Replay WAL entries up to targetVersion only.
	walEntries, err := db.wal.ReadAll()
	if err != nil {
		return fmt.Errorf("read WAL for rollback: %w", err)
	}

	for _, entry := range walEntries {
		if entry.Version <= db.snapshotVersion {
			continue // already in snapshot
		}
		if entry.Version > targetVersion {
			break // stop replaying — entries are version-ordered
		}
		if err := newMT.ApplyChangeSets(entry.ChangeSets); err != nil {
			return fmt.Errorf("rollback apply changeset %d: %w", entry.Version, err)
		}
		if _, err := newMT.SaveVersion(true); err != nil {
			return fmt.Errorf("rollback save version %d: %w", entry.Version, err)
		}
	}

	// Truncate WAL entries after target version.
	if err := db.wal.TruncateAfter(targetVersion); err != nil {
		return fmt.Errorf("truncate WAL after rollback: %w", err)
	}

	// Switch to the rolled-back tree.
	db.MultiTree = newMT
	return nil
}

// ForceSnapshot triggers an immediate snapshot write (synchronous).
// Useful for testing and graceful shutdown.
func (db *DB) ForceSnapshot() error {
	db.mtx.Lock()
	defer db.mtx.Unlock()

	if db.closed {
		return errors.New("db is closed")
	}

	version := db.MultiTree.Version()
	snapshotName := fmt.Sprintf("%s%020d", SnapshotPrefix, version)
	snapshotDir := filepath.Join(db.dir, snapshotName)

	if err := db.MultiTree.WriteSnapshot(context.Background(), snapshotDir); err != nil {
		return err
	}
	if err := db.MultiTree.WriteMultiTreeMetadata(snapshotDir); err != nil {
		return err
	}
	if err := updateCurrentLink(db.dir, snapshotName); err != nil {
		return err
	}

	db.snapshotVersion = version
	db.lastSnapshotTime = time.Now()

	if err := db.wal.TruncateBefore(version); err != nil {
		return err
	}

	return nil
}
