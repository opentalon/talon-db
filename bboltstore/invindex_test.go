package bboltstore

import (
	"testing"

	bolt "go.etcd.io/bbolt"
)

func TestInvIndexAddLookup(t *testing.T) {
	t.Parallel()
	s := openTxStore(t)
	if err := s.db.Update(func(tx *bolt.Tx) error {
		if err := invIndexAdd(tx, "tenant-a", "hund", 1); err != nil {
			return err
		}
		if err := invIndexAdd(tx, "tenant-a", "hund", 2); err != nil {
			return err
		}
		return invIndexAdd(tx, "tenant-a", "katze", 3)
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}

	if err := s.db.View(func(tx *bolt.Tx) error {
		bm, err := invIndexLookup(tx, "tenant-a", "hund")
		if err != nil {
			return err
		}
		got := bm.ToArray()
		if len(got) != 2 || got[0] != 1 || got[1] != 2 {
			t.Fatalf("Lookup hund: got %v, want [1 2]", got)
		}

		bm, err = invIndexLookup(tx, "tenant-a", "missing")
		if err != nil {
			return err
		}
		if !bm.IsEmpty() {
			t.Fatalf("Lookup missing: got %v, want empty", bm.ToArray())
		}
		return nil
	}); err != nil {
		t.Fatalf("View: %v", err)
	}
}

func TestInvIndexRemove(t *testing.T) {
	t.Parallel()
	s := openTxStore(t)
	if err := s.db.Update(func(tx *bolt.Tx) error {
		_ = invIndexAdd(tx, "tenant-a", "hund", 1)
		_ = invIndexAdd(tx, "tenant-a", "hund", 2)
		return invIndexRemove(tx, "tenant-a", "hund", 1)
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		bm, err := invIndexLookup(tx, "tenant-a", "hund")
		if err != nil {
			return err
		}
		got := bm.ToArray()
		if len(got) != 1 || got[0] != 2 {
			t.Fatalf("after Remove: got %v, want [2]", got)
		}
		return nil
	}); err != nil {
		t.Fatalf("View: %v", err)
	}
}

func TestInvIndexRemoveLastBitDeletesTerm(t *testing.T) {
	t.Parallel()
	s := openTxStore(t)
	if err := s.db.Update(func(tx *bolt.Tx) error {
		_ = invIndexAdd(tx, "tenant-a", "lonely", 5)
		return invIndexRemove(tx, "tenant-a", "lonely", 5)
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(invIndexBucketName("tenant-a"))
		if b == nil {
			return nil
		}
		if v := b.Get([]byte("lonely")); v != nil {
			t.Fatalf("term key should be gone after last bit removed, got %d bytes", len(v))
		}
		return nil
	}); err != nil {
		t.Fatalf("View: %v", err)
	}
}

func TestInvIndexLookupPrefix(t *testing.T) {
	t.Parallel()
	s := openTxStore(t)
	if err := s.db.Update(func(tx *bolt.Tx) error {
		_ = invIndexAdd(tx, "tenant-a", "tier:hund", 1)
		_ = invIndexAdd(tx, "tenant-a", "tier:katze", 2)
		_ = invIndexAdd(tx, "tenant-a", "tier:vogel", 3)
		return invIndexAdd(tx, "tenant-a", "color:braun", 4)
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		bm, err := invIndexLookupPrefix(tx, "tenant-a", "tier:")
		if err != nil {
			return err
		}
		got := bm.ToArray()
		if len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 3 {
			t.Fatalf("Prefix tier: got %v, want [1 2 3]", got)
		}
		bm, err = invIndexLookupPrefix(tx, "tenant-a", "")
		if err != nil {
			return err
		}
		if bm.GetCardinality() != 4 {
			t.Fatalf("empty prefix: got %d, want 4", bm.GetCardinality())
		}
		return nil
	}); err != nil {
		t.Fatalf("View: %v", err)
	}
}

func TestInvIndexPerEntity(t *testing.T) {
	t.Parallel()
	s := openTxStore(t)
	if err := s.db.Update(func(tx *bolt.Tx) error {
		_ = invIndexAdd(tx, "tenant-a", "shared", 1)
		return invIndexAdd(tx, "tenant-b", "shared", 99)
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		bmA, _ := invIndexLookup(tx, "tenant-a", "shared")
		bmB, _ := invIndexLookup(tx, "tenant-b", "shared")
		if bmA.GetCardinality() != 1 || !bmA.Contains(1) {
			t.Fatalf("tenant-a: got %v", bmA.ToArray())
		}
		if bmB.GetCardinality() != 1 || !bmB.Contains(99) {
			t.Fatalf("tenant-b: got %v", bmB.ToArray())
		}
		return nil
	}); err != nil {
		t.Fatalf("View: %v", err)
	}
}
