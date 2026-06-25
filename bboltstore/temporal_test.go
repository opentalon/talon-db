package bboltstore

import (
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func TestTemporalAddAndRead(t *testing.T) {
	t.Parallel()
	s := openTxStore(t)
	if err := s.db.Update(func(tx *bolt.Tx) error {
		_ = temporalAdd(tx, "tenant-a", "item-1", "doc-1", "inspect", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano())
		_ = temporalAdd(tx, "tenant-a", "item-1", "doc-2", "repair", time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC).UnixNano())
		return temporalAdd(tx, "tenant-a", "item-1", "doc-3", "inspect", time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC).UnixNano())
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		entries, err := temporalRead(tx, "tenant-a", "item-1", nil)
		if err != nil {
			return err
		}
		if len(entries) != 3 {
			t.Fatalf("read all: got %d, want 3", len(entries))
		}
		for i := 1; i < len(entries); i++ {
			if entries[i].At <= entries[i-1].At {
				t.Fatalf("not sorted at %d", i)
			}
		}

		entries, err = temporalRead(tx, "tenant-a", "item-1", []string{"inspect"})
		if err != nil {
			return err
		}
		if len(entries) != 2 {
			t.Fatalf("filtered: got %d, want 2 (doc-1, doc-3)", len(entries))
		}
		return nil
	}); err != nil {
		t.Fatalf("View: %v", err)
	}
}

func TestTemporalReadEmpty(t *testing.T) {
	t.Parallel()
	s := openTxStore(t)
	if err := s.db.View(func(tx *bolt.Tx) error {
		entries, err := temporalRead(tx, "tenant-a", "unknown", nil)
		if err != nil {
			return err
		}
		if len(entries) != 0 {
			t.Fatalf("empty: got %d entries", len(entries))
		}
		return nil
	}); err != nil {
		t.Fatalf("View: %v", err)
	}
}

func TestTemporalRemove(t *testing.T) {
	t.Parallel()
	s := openTxStore(t)
	at := time.Now().UnixNano()
	if err := s.db.Update(func(tx *bolt.Tx) error {
		_ = temporalAdd(tx, "tenant-a", "item-1", "doc-1", "inspect", at)
		_ = temporalAdd(tx, "tenant-a", "item-1", "doc-2", "inspect", at+1)
		return temporalRemove(tx, "tenant-a", "item-1", "doc-1")
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		entries, _ := temporalRead(tx, "tenant-a", "item-1", nil)
		if len(entries) != 1 || entries[0].DocID != "doc-2" {
			t.Fatalf("after remove: %v", entries)
		}
		return nil
	}); err != nil {
		t.Fatalf("View: %v", err)
	}
}

func TestTemporalAddReplacesSameDocID(t *testing.T) {
	t.Parallel()
	s := openTxStore(t)
	if err := s.db.Update(func(tx *bolt.Tx) error {
		_ = temporalAdd(tx, "tenant-a", "item-1", "doc-1", "inspect", 100)
		return temporalAdd(tx, "tenant-a", "item-1", "doc-1", "repair", 200)
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		entries, _ := temporalRead(tx, "tenant-a", "item-1", nil)
		if len(entries) != 1 {
			t.Fatalf("after replace: got %d entries, want 1", len(entries))
		}
		if entries[0].At != 200 || entries[0].Type != "repair" {
			t.Fatalf("replace lost: %v", entries[0])
		}
		return nil
	}); err != nil {
		t.Fatalf("View: %v", err)
	}
}

func TestTemporalFieldsParsesNumericAt(t *testing.T) {
	t.Parallel()
	item, recType, at, ok := temporalFields([]byte(`{"item_id":"i1","type":"inspect","at":1234567890}`))
	if !ok || item != "i1" || recType != "inspect" || at != 1234567890 {
		t.Fatalf("parse: ok=%v item=%q type=%q at=%d", ok, item, recType, at)
	}
}

func TestTemporalFieldsParsesRFC3339At(t *testing.T) {
	t.Parallel()
	doc := []byte(`{"item_id":"i1","type":"inspect","at":"2026-06-25T13:00:00Z"}`)
	_, _, at, ok := temporalFields(doc)
	if !ok {
		t.Fatal("RFC3339: not ok")
	}
	want := time.Date(2026, 6, 25, 13, 0, 0, 0, time.UTC).UnixNano()
	if at != want {
		t.Fatalf("at = %d, want %d", at, want)
	}
}

func TestTemporalFieldsRejectsMissing(t *testing.T) {
	t.Parallel()
	cases := [][]byte{
		[]byte(`{}`),
		[]byte(`{"item_id":"i"}`),
		[]byte(`{"item_id":"i","type":"t"}`),
		[]byte(`{"item_id":"i","at":1}`),
		[]byte(`{"item_id":42,"type":"t","at":1}`), // non-string item_id
	}
	for _, c := range cases {
		if _, _, _, ok := temporalFields(c); ok {
			t.Errorf("should reject %q", c)
		}
	}
}
