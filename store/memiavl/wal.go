package memiavl

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// WAL is a simple append-only Write-Ahead Log for MemIAVL changesets.
// Each entry is a versioned set of named changesets that can be replayed
// to reconstruct tree state from a snapshot.
//
// File format:
//   - Each segment file is named by its first version number (zero-padded to 20 digits)
//   - Entries are length-prefixed: uint32(len) + marshaled WALEntry
//   - Entries within a segment are strictly version-ordered
//
// Async mode (bufferSize > 0):
//   - Writes are buffered in a channel and flushed by a background goroutine
//   - Error from the background goroutine is propagated on the next Write call
type WAL struct {
	dir        string
	bufferSize int

	// Current write segment.
	currentFile    *os.File
	currentVersion int64 // first version in current segment

	// Async support.
	writeCh  chan WALEntry
	errCh    chan error
	closeCh  chan struct{}
	closeWg  sync.WaitGroup
	closeErr error

	mu     sync.Mutex
	closed bool
}

// WALEntry is a single WAL record containing changesets for one version.
type WALEntry struct {
	Version    int64
	ChangeSets []NamedChangeSet
}

// segmentMaxEntries is the max entries per segment file before rotation.
const segmentMaxEntries = 10000

// OpenWAL opens or creates a WAL at the given directory.
// bufferSize > 0 enables async writes.
func OpenWAL(dir string, bufferSize int) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	w := &WAL{
		dir:        dir,
		bufferSize: bufferSize,
	}

	if bufferSize > 0 {
		w.writeCh = make(chan WALEntry, bufferSize)
		w.errCh = make(chan error, 1)
		w.closeCh = make(chan struct{})
		w.closeWg.Add(1)
		go w.asyncWriteLoop()
	}

	return w, nil
}

// Write appends a WAL entry. In async mode, queues the write.
func (w *WAL) Write(entry WALEntry) error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return errors.New("WAL is closed")
	}
	w.mu.Unlock()

	if w.bufferSize > 0 {
		// Check for async errors.
		select {
		case err := <-w.errCh:
			return fmt.Errorf("async WAL error: %w", err)
		default:
		}
		w.writeCh <- entry
		return nil
	}

	return w.writeSync(entry)
}

// writeSync writes an entry synchronously.
func (w *WAL) writeSync(entry WALEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.currentFile == nil {
		if err := w.openNewSegment(entry.Version); err != nil {
			return err
		}
	}

	data := marshalWALEntry(entry)
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(data)))
	if _, err := w.currentFile.Write(lenBuf[:]); err != nil {
		return err
	}
	if _, err := w.currentFile.Write(data); err != nil {
		return err
	}
	return nil
}

// asyncWriteLoop runs in a background goroutine for async writes.
func (w *WAL) asyncWriteLoop() {
	defer w.closeWg.Done()

	for {
		select {
		case entry := <-w.writeCh:
			if err := w.writeSync(entry); err != nil {
				select {
				case w.errCh <- err:
				default:
				}
			}
		case <-w.closeCh:
			// Drain remaining entries.
			for {
				select {
				case entry := <-w.writeCh:
					if err := w.writeSync(entry); err != nil {
						w.closeErr = err
					}
				default:
					return
				}
			}
		}
	}
}

// ReadAll reads all entries from all segment files.
func (w *WAL) ReadAll() ([]WALEntry, error) {
	segments, err := w.listSegments()
	if err != nil {
		return nil, err
	}

	var entries []WALEntry
	for _, seg := range segments {
		segEntries, err := w.readSegment(seg)
		if err != nil {
			return nil, fmt.Errorf("read segment %s: %w", seg, err)
		}
		entries = append(entries, segEntries...)
	}
	return entries, nil
}

// TruncateBefore removes all WAL segments whose first version is before the given version.
func (w *WAL) TruncateBefore(version int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	segments, err := w.listSegments()
	if err != nil {
		return err
	}

	for _, seg := range segments {
		segVersion := segmentVersion(seg)
		if segVersion < version {
			// If this is the current segment, close the file handle first.
			if segVersion == w.currentVersion && w.currentFile != nil {
				_ = w.currentFile.Sync()
				_ = w.currentFile.Close()
				w.currentFile = nil
			}
			// This entire segment is before the version — safe to remove.
			if err := os.Remove(filepath.Join(w.dir, seg)); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
	}
	return nil
}

// Close flushes pending writes and closes the WAL.
func (w *WAL) Close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	w.mu.Unlock()

	if w.bufferSize > 0 {
		close(w.closeCh)
		w.closeWg.Wait()
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	var errs []error
	if w.closeErr != nil {
		errs = append(errs, w.closeErr)
	}
	if w.currentFile != nil {
		if err := w.currentFile.Sync(); err != nil {
			errs = append(errs, err)
		}
		if err := w.currentFile.Close(); err != nil {
			errs = append(errs, err)
		}
		w.currentFile = nil
	}
	return errors.Join(errs...)
}

// --- Internal helpers ---

func (w *WAL) openNewSegment(version int64) error {
	name := fmt.Sprintf("%020d.wal", version)
	f, err := os.OpenFile(
		filepath.Join(w.dir, name),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND,
		0o644,
	)
	if err != nil {
		return err
	}
	w.currentFile = f
	w.currentVersion = version
	return nil
}

func (w *WAL) listSegments() ([]string, error) {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return nil, err
	}

	var segments []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".wal") {
			continue
		}
		segments = append(segments, e.Name())
	}
	sort.Strings(segments) // version order (zero-padded names sort correctly)
	return segments, nil
}

func segmentVersion(name string) int64 {
	trimmed := strings.TrimSuffix(name, ".wal")
	trimmed = strings.TrimLeft(trimmed, "0")
	if trimmed == "" {
		return 0
	}
	v, _ := strconv.ParseInt(trimmed, 10, 64)
	return v
}

func (w *WAL) readSegment(name string) ([]WALEntry, error) {
	f, err := os.Open(filepath.Join(w.dir, name))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var entries []WALEntry
	for {
		var lenBuf [4]byte
		if _, err := io.ReadFull(f, lenBuf[:]); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		dataLen := binary.LittleEndian.Uint32(lenBuf[:])
		data := make([]byte, dataLen)
		if _, err := io.ReadFull(f, data); err != nil {
			return nil, err
		}
		entry, err := unmarshalWALEntry(data)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// --- Marshaling ---
// Simple binary format: version(8) + numChangeSets(4) + for each: nameLen(4) + name + numPairs(4) + for each: delete(1) + keyLen(4) + key + valueLen(4) + value

func marshalWALEntry(e WALEntry) []byte {
	// Estimate size.
	size := 8 + 4 // version + numChangeSets
	for _, cs := range e.ChangeSets {
		size += 4 + len(cs.Name) + 4 // nameLen + name + numPairs
		for _, p := range cs.Pairs {
			size += 1 + 4 + len(p.Key) + 4 + len(p.Value) // delete + keyLen + key + valueLen + value
		}
	}

	buf := make([]byte, size)
	offset := 0

	binary.LittleEndian.PutUint64(buf[offset:], uint64(e.Version))
	offset += 8
	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(e.ChangeSets)))
	offset += 4

	for _, cs := range e.ChangeSets {
		binary.LittleEndian.PutUint32(buf[offset:], uint32(len(cs.Name)))
		offset += 4
		offset += copy(buf[offset:], cs.Name)

		binary.LittleEndian.PutUint32(buf[offset:], uint32(len(cs.Pairs)))
		offset += 4

		for _, p := range cs.Pairs {
			if p.Delete {
				buf[offset] = 1
			} else {
				buf[offset] = 0
			}
			offset++
			binary.LittleEndian.PutUint32(buf[offset:], uint32(len(p.Key)))
			offset += 4
			offset += copy(buf[offset:], p.Key)
			binary.LittleEndian.PutUint32(buf[offset:], uint32(len(p.Value)))
			offset += 4
			offset += copy(buf[offset:], p.Value)
		}
	}

	return buf[:offset]
}

func unmarshalWALEntry(data []byte) (WALEntry, error) {
	if len(data) < 12 {
		return WALEntry{}, errors.New("WAL entry too short")
	}

	offset := 0
	version := int64(binary.LittleEndian.Uint64(data[offset:]))
	offset += 8
	numCS := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	changeSets := make([]NamedChangeSet, 0, numCS)
	for i := uint32(0); i < numCS; i++ {
		if offset+4 > len(data) {
			return WALEntry{}, errors.New("WAL entry truncated at changeset name length")
		}
		nameLen := int(binary.LittleEndian.Uint32(data[offset:]))
		offset += 4
		if offset+nameLen > len(data) {
			return WALEntry{}, errors.New("WAL entry truncated at changeset name")
		}
		name := string(data[offset : offset+nameLen])
		offset += nameLen

		if offset+4 > len(data) {
			return WALEntry{}, errors.New("WAL entry truncated at pair count")
		}
		numPairs := binary.LittleEndian.Uint32(data[offset:])
		offset += 4

		pairs := make([]KVPair, 0, numPairs)
		for j := uint32(0); j < numPairs; j++ {
			if offset+1 > len(data) {
				return WALEntry{}, errors.New("WAL entry truncated at delete flag")
			}
			del := data[offset] == 1
			offset++

			if offset+4 > len(data) {
				return WALEntry{}, errors.New("WAL entry truncated at key length")
			}
			keyLen := int(binary.LittleEndian.Uint32(data[offset:]))
			offset += 4
			if offset+keyLen > len(data) {
				return WALEntry{}, errors.New("WAL entry truncated at key")
			}
			key := make([]byte, keyLen)
			copy(key, data[offset:offset+keyLen])
			offset += keyLen

			if offset+4 > len(data) {
				return WALEntry{}, errors.New("WAL entry truncated at value length")
			}
			valLen := int(binary.LittleEndian.Uint32(data[offset:]))
			offset += 4
			var value []byte
			if valLen > 0 {
				if offset+valLen > len(data) {
					return WALEntry{}, errors.New("WAL entry truncated at value")
				}
				value = make([]byte, valLen)
				copy(value, data[offset:offset+valLen])
				offset += valLen
			}

			pairs = append(pairs, KVPair{Key: key, Value: value, Delete: del})
		}

		changeSets = append(changeSets, NamedChangeSet{Name: name, Pairs: pairs})
	}

	return WALEntry{Version: version, ChangeSets: changeSets}, nil
}
