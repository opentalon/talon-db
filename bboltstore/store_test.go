package bboltstore_test

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	talondb "github.com/opentalon/talon-db"
	"github.com/opentalon/talon-db/bboltstore"
	"github.com/opentalon/talon-db/talondbtest"
)

// newStore returns a fresh, isolated *bboltstore.Store backed by a
// temp-dir bbolt file. Closure is registered with t.Cleanup so tests
// can run in parallel without leaking handles.
func newStore(t *testing.T) *bboltstore.Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "store.db")
	s, err := bboltstore.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// TestConformance runs the backend-agnostic DocumentStore contract
// against the bbolt implementation.
func TestConformance(t *testing.T) {
	talondbtest.Suite(t, func(t *testing.T) talondb.DocumentStore {
		return newStore(t)
	})
}

// TestConcurrentPutAcrossEntities exercises bbolt's single-writer
// model: many goroutines targeting different entities must not
// deadlock. -race in CI surfaces torn writes if per-entity bucket
// creation were racy.
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
