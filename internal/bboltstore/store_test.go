package bboltstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	talondb "github.com/opentalon/talon-db"

	"github.com/golang/snappy"
	bolt "go.etcd.io/bbolt"
)

func newStore(t *testing.T) *Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "store.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestPutGetRoundtrip(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	ctx := context.Background()

	doc := []byte(`{"hello":"world","n":42}`)
	if err := s.Put(ctx, "tenant-a", "doc-1", doc); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := s.Get(ctx, "tenant-a", "doc-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, doc) {
		t.Fatalf("Get returned %q, want %q", got, doc)
	}
}

func TestPutSnappyCompressed(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	ctx := context.Background()

	doc := bytes.Repeat([]byte(`{"k":"v"}`), 200)
	if err := s.Put(ctx, "tenant-a", "doc-1", doc); err != nil {
		t.Fatalf("Put: %v", err)
	}

	var rawOnDisk []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(docsBucketPrefix + "tenant-a"))
		if b == nil {
			t.Fatal("docs bucket missing")
		}
		v := b.Get([]byte("doc-1"))
		rawOnDisk = append([]byte(nil), v...)
		return nil
	})
	if err != nil {
		t.Fatalf("View: %v", err)
	}
	if bytes.Equal(rawOnDisk, doc) {
		t.Fatal("on-disk bytes equal raw input — snappy compression missing")
	}
	if len(rawOnDisk) >= len(doc) {
		t.Fatalf("snappy output (%d) not smaller than input (%d)", len(rawOnDisk), len(doc))
	}
	decoded, err := snappy.Decode(nil, rawOnDisk)
	if err != nil {
		t.Fatalf("snappy decode: %v", err)
	}
	if !bytes.Equal(decoded, doc) {
		t.Fatal("decoded on-disk bytes differ from input")
	}
}

func TestGetMissingReturnsErrNotFound(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	ctx := context.Background()

	tests := []struct {
		name           string
		setupEntity    string
		setupDocID     string
		queryEntity    string
		queryDocID     string
	}{
		{"empty store", "", "", "tenant-a", "doc-1"},
		{"wrong entity", "tenant-a", "doc-1", "tenant-b", "doc-1"},
		{"wrong doc id", "tenant-a", "doc-1", "tenant-a", "doc-2"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.setupEntity != "" {
				if err := s.Put(ctx, tc.setupEntity, tc.setupDocID, []byte("{}")); err != nil {
					t.Fatalf("Put setup: %v", err)
				}
			}
			_, err := s.Get(ctx, tc.queryEntity, tc.queryDocID)
			if !errors.Is(err, talondb.ErrNotFound) {
				t.Fatalf("Get: got %v, want ErrNotFound", err)
			}
		})
	}
}

func TestDeleteIdempotent(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	ctx := context.Background()

	if err := s.Delete(ctx, "tenant-a", "doc-1"); err != nil {
		t.Fatalf("Delete on empty store: %v", err)
	}

	if err := s.Put(ctx, "tenant-a", "doc-1", []byte("{}")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Delete(ctx, "tenant-a", "doc-1"); err != nil {
		t.Fatalf("Delete first time: %v", err)
	}
	if err := s.Delete(ctx, "tenant-a", "doc-1"); err != nil {
		t.Fatalf("Delete second time: %v", err)
	}
	if _, err := s.Get(ctx, "tenant-a", "doc-1"); !errors.Is(err, talondb.ErrNotFound) {
		t.Fatalf("Get after delete: got %v, want ErrNotFound", err)
	}
}

func TestBatchPutAtomic(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	ctx := context.Background()

	docs := map[string][]byte{
		"a": []byte(`{"v":"a"}`),
		"b": []byte(`{"v":"b"}`),
		"c": []byte(`{"v":"c"}`),
	}
	if err := s.BatchPut(ctx, "tenant-a", docs); err != nil {
		t.Fatalf("BatchPut: %v", err)
	}
	for k, want := range docs {
		got, err := s.Get(ctx, "tenant-a", k)
		if err != nil {
			t.Fatalf("Get %q: %v", k, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("Get %q = %q, want %q", k, got, want)
		}
	}
}

func TestBatchPutRollbackOnCancelledContext(t *testing.T) {
	t.Parallel()
	s := newStore(t)

	if err := s.Put(context.Background(), "tenant-a", "existing", []byte(`{"keep":true}`)); err != nil {
		t.Fatalf("Put existing: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	docs := map[string][]byte{"x": []byte("{}"), "y": []byte("{}")}
	err := s.BatchPut(ctx, "tenant-a", docs)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("BatchPut: got %v, want context.Canceled", err)
	}

	for _, k := range []string{"x", "y"} {
		if _, err := s.Get(context.Background(), "tenant-a", k); !errors.Is(err, talondb.ErrNotFound) {
			t.Fatalf("Get %q after rollback: got %v, want ErrNotFound", k, err)
		}
	}
	if _, err := s.Get(context.Background(), "tenant-a", "existing"); err != nil {
		t.Fatalf("Get existing after rollback: %v", err)
	}
}

func TestBatchPutRejectsInvalidDocID(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	docs := map[string][]byte{"a": []byte("{}"), "": []byte("{}")}
	if err := s.BatchPut(context.Background(), "tenant-a", docs); err == nil {
		t.Fatal("BatchPut: expected error for empty docID")
	}
	if _, err := s.Get(context.Background(), "tenant-a", "a"); !errors.Is(err, talondb.ErrNotFound) {
		t.Fatalf("'a' should not have been written: %v", err)
	}
}

func TestScanVisitsAllAndStops(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	ctx := context.Background()

	docs := map[string][]byte{
		"a": []byte(`{"v":1}`),
		"b": []byte(`{"v":2}`),
		"c": []byte(`{"v":3}`),
		"d": []byte(`{"v":4}`),
	}
	if err := s.BatchPut(ctx, "tenant-a", docs); err != nil {
		t.Fatalf("BatchPut: %v", err)
	}

	var visited []string
	if err := s.Scan(ctx, "tenant-a", func(id string, doc []byte) bool {
		visited = append(visited, id)
		return true
	}); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	sort.Strings(visited)
	want := []string{"a", "b", "c", "d"}
	if !equalStrings(visited, want) {
		t.Fatalf("Scan visited %v, want %v", visited, want)
	}

	var stopped []string
	if err := s.Scan(ctx, "tenant-a", func(id string, doc []byte) bool {
		stopped = append(stopped, id)
		return len(stopped) < 2
	}); err != nil {
		t.Fatalf("Scan early-exit: %v", err)
	}
	if len(stopped) != 2 {
		t.Fatalf("Scan should have stopped after 2, visited %d: %v", len(stopped), stopped)
	}
}

func TestScanEmptyEntity(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	visited := 0
	err := s.Scan(context.Background(), "nobody", func(string, []byte) bool {
		visited++
		return true
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if visited != 0 {
		t.Fatalf("Scan over empty entity visited %d docs", visited)
	}
}

func TestTenantIsolation(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	ctx := context.Background()

	if err := s.Put(ctx, "tenant-a", "doc-1", []byte(`"a"`)); err != nil {
		t.Fatalf("Put tenant-a: %v", err)
	}
	if err := s.Put(ctx, "tenant-b", "doc-1", []byte(`"b"`)); err != nil {
		t.Fatalf("Put tenant-b: %v", err)
	}

	got, err := s.Get(ctx, "tenant-a", "doc-1")
	if err != nil {
		t.Fatalf("Get tenant-a: %v", err)
	}
	if string(got) != `"a"` {
		t.Fatalf("tenant-a saw %q, want %q", got, `"a"`)
	}

	var seenInA []string
	_ = s.Scan(ctx, "tenant-a", func(id string, _ []byte) bool {
		seenInA = append(seenInA, id)
		return true
	})
	if len(seenInA) != 1 || seenInA[0] != "doc-1" {
		t.Fatalf("tenant-a scan: %v, want [doc-1]", seenInA)
	}

	if err := s.Delete(ctx, "tenant-a", "doc-1"); err != nil {
		t.Fatalf("Delete tenant-a: %v", err)
	}
	if _, err := s.Get(ctx, "tenant-b", "doc-1"); err != nil {
		t.Fatalf("tenant-b's doc was affected by tenant-a delete: %v", err)
	}
}

func TestConcurrentPutAcrossEntities(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	ctx := context.Background()

	const tenants = 8
	const docsPerTenant = 25

	var wg sync.WaitGroup
	for i := 0; i < tenants; i++ {
		wg.Add(1)
		go func(tenant int) {
			defer wg.Done()
			entity := "tenant-" + string(rune('a'+tenant))
			for j := 0; j < docsPerTenant; j++ {
				docID := "doc-" + string(rune('a'+j%26))
				_ = s.Put(ctx, entity, docID, []byte(`{}`))
			}
		}(i)
	}
	wg.Wait()
}

func TestValidationErrors(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	ctx := context.Background()

	tests := []struct {
		name      string
		entityID  string
		docID     string
		wantSub   string
	}{
		{"empty entity", "", "doc-1", "empty"},
		{"colon in entity", "a:b", "doc-1", "reserved"},
		{"empty doc id", "tenant-a", "", "docID"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := s.Put(ctx, tc.entityID, tc.docID, []byte("{}"))
			if err == nil {
				t.Fatal("Put: expected error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Fatalf("Put error %q does not contain %q", err.Error(), tc.wantSub)
			}
		})
	}
}

func TestPutBumpsVersion(t *testing.T) {
	t.Parallel()
	s := newStore(t)
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

func equalStrings(a, b []string) bool {
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
