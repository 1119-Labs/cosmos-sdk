package memiavl

import (
	"errors"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// MmapFile manages the lifecycle of a memory-mapped file.
type MmapFile struct {
	file *os.File
	data []byte
}

// NewMmap opens the file at path and creates a read-only memory mapping.
// Applies MADV_SEQUENTIAL + MADV_WILLNEED for optimal initial loading.
func NewMmap(path string) (*MmapFile, error) {
	file, err := os.Open(filepath.Clean(path))
	if err != nil {
		return nil, err
	}

	fi, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	size := fi.Size()
	if size == 0 {
		// Empty file — return a valid MmapFile with nil data.
		return &MmapFile{file: file}, nil
	}

	data, err := unix.Mmap(int(file.Fd()), 0, int(size), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	m := &MmapFile{file: file, data: data}
	m.PrepareForSequentialRead()
	return m, nil
}

// PrepareForSequentialRead sets madvise hints for sequential access with readahead.
func (m *MmapFile) PrepareForSequentialRead() {
	if len(m.data) > 0 {
		_ = unix.Madvise(m.data, unix.MADV_SEQUENTIAL)
		_ = unix.Madvise(m.data, unix.MADV_WILLNEED)
	}
}

// PrepareForRandomRead switches madvise to random access mode (disables readahead).
func (m *MmapFile) PrepareForRandomRead() {
	if len(m.data) > 0 {
		_ = unix.Madvise(m.data, unix.MADV_RANDOM)
	}
}

// Data returns the mmap-ed buffer.
func (m *MmapFile) Data() []byte {
	return m.data
}

// Close unmaps the memory and closes the underlying file.
func (m *MmapFile) Close() error {
	var errs []error
	if len(m.data) > 0 {
		if err := unix.Munmap(m.data); err != nil {
			errs = append(errs, err)
		}
		m.data = nil
	}
	if m.file != nil {
		if err := m.file.Close(); err != nil {
			errs = append(errs, err)
		}
		m.file = nil
	}
	return errors.Join(errs...)
}
