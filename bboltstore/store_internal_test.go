package bboltstore

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/golang/snappy"
	bolt "go.etcd.io/bbolt"
)

// These tests live in the internal test package because they poke at
// the bbolt buckets directly — they assert backend-specific invariants
// (snappy compression on disk, meta-bucket version counter, colon
// validation) that aren't part of the public DocumentStore contract.

func freshStore(t *testing.T) *Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "store.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// TestSnappyOnDisk verifies bytes stored under docs:{tenant} are
// snappy-encoded, not the raw input.
func TestSnappyOnDisk(t *testing.T) {
	t.Parallel()
	s := freshStore(t)
	ctx := context.Background()
	doc := bytes.Repeat([]byte(`{"k":"v"}`), 200)
	if err := s.Put(ctx, "tenant-a", "doc-1", doc); err != nil {
		t.Fatalf("Put: %v", err)
	}
	var raw []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(docsBucketPrefix + "tenant-a"))
		if b == nil {
			t.Fatal("docs bucket missing")
		}
		raw = append([]byte(nil), b.Get([]byte("doc-1"))...)
		return nil
	})
	if err != nil {
		t.Fatalf("View: %v", err)
	}
	if bytes.Equal(raw, doc) {
		t.Fatal("on-disk bytes equal raw input — snappy compression missing")
	}
	if len(raw) >= len(doc) {
		t.Fatalf("snappy output (%d) not smaller than input (%d)", len(raw), len(doc))
	}
	decoded, err := snappy.Decode(nil, raw)
	if err != nil {
		t.Fatalf("snappy decode: %v", err)
	}
	if !bytes.Equal(decoded, doc) {
		t.Fatal("decoded on-disk bytes differ from input")
	}
}

// TestPutBumpsVersion verifies the meta bucket tracks document
// versioning. Other backends may use a different metadata layout, so
// this is bbolt-specific.
func TestPutBumpsVersion(t *testing.T) {
	t.Parallel()
	s := freshStore(t)
	ctx := context.Background()
	if err := s.Put(ctx, "tenant-a", "doc-1", []byte(`{"v":1}`)); err != nil {
		t.Fatalf("Put v1: %v", err)
	}
	if err := s.Put(ctx, "tenant-a", "doc-1", []byte(`{"v":2}`)); err != nil {
		t.Fatalf("Put v2: %v", err)
	}
	var m docMeta
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(metaBucketPrefix + "tenant-a"))
		if b == nil {
			t.Fatal("meta bucket missing")
		}
		raw := b.Get([]byte("doc-1"))
		if raw == nil {
			t.Fatal("meta record missing")
		}
		return json.Unmarshal(raw, &m)
	})
	if err != nil {
		t.Fatalf("View: %v", err)
	}
	if m.Version != 2 {
		t.Fatalf("version = %d, want 2", m.Version)
	}
	if m.UpdatedAt < m.CreatedAt {
		t.Fatalf("UpdatedAt (%d) should be >= CreatedAt (%d) after rewrite", m.UpdatedAt, m.CreatedAt)
	}
}

// TestColonInEntityRejected is bbolt-specific: the colon separates the
// bucket prefix from the tenant name in the on-disk layout.
func TestColonInEntityRejected(t *testing.T) {
	t.Parallel()
	s := freshStore(t)
	err := s.Put(context.Background(), "a:b", "doc-1", []byte("{}"))
	if err == nil {
		t.Fatal("Put with colon in entityID: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("error %q does not mention reserved character", err.Error())
	}
}
