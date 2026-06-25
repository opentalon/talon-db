package bboltstore

import (
	"path/filepath"
	"sync"
	"testing"

	bolt "go.etcd.io/bbolt"
)

func openTxStore(t *testing.T) *Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "idmap.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestIDMapAssignReturnsStableID(t *testing.T) {
	t.Parallel()
	s := openTxStore(t)
	var first, second uint32
	if err := s.db.Update(func(tx *bolt.Tx) error {
		var err error
		first, err = idmapAssign(tx, "tenant-a", "doc-1")
		return err
	}); err != nil {
		t.Fatalf("first assign: %v", err)
	}
	if err := s.db.Update(func(tx *bolt.Tx) error {
		var err error
		second, err = idmapAssign(tx, "tenant-a", "doc-1")
		return err
	}); err != nil {
		t.Fatalf("second assign: %v", err)
	}
	if first != second {
		t.Fatalf("assign returned %d then %d for same docID, want stable", first, second)
	}
	if first == 0 {
		t.Fatal("assign returned 0; counter must start at 1")
	}
}

func TestIDMapAssignsAreMonotonic(t *testing.T) {
	t.Parallel()
	s := openTxStore(t)
	ids := make([]uint32, 0, 10)
	if err := s.db.Update(func(tx *bolt.Tx) error {
		for i := 0; i < 10; i++ {
			id, err := idmapAssign(tx, "tenant-a", "doc-"+string(rune('a'+i)))
			if err != nil {
				return err
			}
			ids = append(ids, id)
		}
		return nil
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	for i := 1; i < len(ids); i++ {
		if ids[i] <= ids[i-1] {
			t.Fatalf("non-monotonic at index %d: %v", i, ids)
		}
	}
	if ids[0] != 1 {
		t.Fatalf("first id = %d, want 1", ids[0])
	}
}

func TestIDMapPerEntityIsolation(t *testing.T) {
	t.Parallel()
	s := openTxStore(t)
	var aID, bID uint32
	if err := s.db.Update(func(tx *bolt.Tx) error {
		var err error
		aID, err = idmapAssign(tx, "tenant-a", "doc-1")
		if err != nil {
			return err
		}
		bID, err = idmapAssign(tx, "tenant-b", "doc-1")
		return err
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if aID != 1 || bID != 1 {
		t.Fatalf("per-entity counters should both start at 1; got a=%d b=%d", aID, bID)
	}
}

func TestIDMapLookupAndReverse(t *testing.T) {
	t.Parallel()
	s := openTxStore(t)
	var assigned uint32
	if err := s.db.Update(func(tx *bolt.Tx) error {
		var err error
		assigned, err = idmapAssign(tx, "tenant-a", "doc-xyz")
		return err
	}); err != nil {
		t.Fatalf("assign: %v", err)
	}

	var (
		looked uint32
		found  bool
		doc    string
	)
	if err := s.db.View(func(tx *bolt.Tx) error {
		var err error
		looked, found, err = idmapLookup(tx, "tenant-a", "doc-xyz")
		if err != nil {
			return err
		}
		doc, _, err = idmapReverse(tx, "tenant-a", assigned)
		return err
	}); err != nil {
		t.Fatalf("View: %v", err)
	}
	if !found || looked != assigned {
		t.Fatalf("Lookup: got (%d, %v), want (%d, true)", looked, found, assigned)
	}
	if doc != "doc-xyz" {
		t.Fatalf("Reverse: got %q, want doc-xyz", doc)
	}
}

func TestIDMapLookupAbsent(t *testing.T) {
	t.Parallel()
	s := openTxStore(t)
	var found bool
	if err := s.db.View(func(tx *bolt.Tx) error {
		_, f, err := idmapLookup(tx, "tenant-empty", "doc-?")
		found = f
		return err
	}); err != nil {
		t.Fatalf("View: %v", err)
	}
	if found {
		t.Fatal("Lookup on empty store reported found=true")
	}
}

func TestIDMapConcurrentAcrossEntities(t *testing.T) {
	t.Parallel()
	s := openTxStore(t)

	const tenants = 8
	const ids = 20
	var wg sync.WaitGroup
	for i := 0; i < tenants; i++ {
		wg.Add(1)
		go func(tenant int) {
			defer wg.Done()
			entity := "tenant-" + string(rune('a'+tenant))
			_ = s.db.Update(func(tx *bolt.Tx) error {
				for j := 0; j < ids; j++ {
					if _, err := idmapAssign(tx, entity, "doc-"+string(rune('a'+j))); err != nil {
						return err
					}
				}
				return nil
			})
		}(i)
	}
	wg.Wait()

	for i := 0; i < tenants; i++ {
		entity := "tenant-" + string(rune('a'+i))
		var last uint32
		if err := s.db.View(func(tx *bolt.Tx) error {
			_, _, err := idmapLookup(tx, entity, "doc-a")
			if err != nil {
				return err
			}
			// Sanity: every docID gets a non-zero ID.
			for j := 0; j < ids; j++ {
				id, found, _ := idmapLookup(tx, entity, "doc-"+string(rune('a'+j)))
				if !found {
					t.Errorf("%s doc-%c not assigned", entity, 'a'+j)
				}
				if id > last {
					last = id
				}
			}
			return nil
		}); err != nil {
			t.Fatalf("View: %v", err)
		}
		if last != uint32(ids) {
			t.Errorf("%s last id = %d, want %d", entity, last, ids)
		}
	}
}
