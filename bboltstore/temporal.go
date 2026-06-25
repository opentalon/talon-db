package bboltstore

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	bolt "go.etcd.io/bbolt"
)

// Temporal index: per-(entity, itemID) sorted event log.
//
// Storage layout:
//
//	temp:{entity}:{itemID} — value: JSON-encoded []temporalEntry sorted by At ascending
//
// A document contributes a temporal entry when it has all three of
// these top-level fields:
//   - "item_id" (string) — bucket key suffix
//   - "type"    (string) — record type for filtering
//   - "at"      (number or RFC3339 string) — event timestamp
//
// Documents that lack any of these are silently ignored by the
// temporal index. This matches the design rule from #27: each index
// extracts only the fields it understands.

const temporalPrefix = "temp:"

func temporalBucketName(entityID, itemID string) []byte {
	return []byte(temporalPrefix + entityID + ":" + itemID)
}

type temporalEntry struct {
	At    int64  `json:"at"` // unix nanos
	Type  string `json:"type"`
	DocID string `json:"doc_id"`
}

// temporalFields extracts (itemID, type, at) from a document if all
// three are present. Returns ok=false otherwise.
func temporalFields(doc []byte) (itemID, recordType string, at int64, ok bool) {
	if len(doc) == 0 {
		return "", "", 0, false
	}
	var m map[string]any
	if err := json.Unmarshal(doc, &m); err != nil {
		return "", "", 0, false
	}
	itemRaw, hasItem := m["item_id"]
	typeRaw, hasType := m["type"]
	atRaw, hasAt := m["at"]
	if !hasItem || !hasType || !hasAt {
		return "", "", 0, false
	}
	itemStr, ok1 := itemRaw.(string)
	typeStr, ok2 := typeRaw.(string)
	if !ok1 || !ok2 {
		return "", "", 0, false
	}
	switch v := atRaw.(type) {
	case float64:
		return itemStr, typeStr, int64(v), true
	case string:
		t, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			t, err = time.Parse(time.RFC3339, v)
		}
		if err != nil {
			return "", "", 0, false
		}
		return itemStr, typeStr, t.UnixNano(), true
	default:
		return "", "", 0, false
	}
}

// temporalAdd inserts an entry into the sorted log for (entity, itemID).
func temporalAdd(tx *bolt.Tx, entityID, itemID, docID, recordType string, at int64) error {
	entries, err := loadTemporal(tx, entityID, itemID)
	if err != nil {
		return err
	}
	// Replace any existing entry with same docID first to support
	// idempotent re-puts.
	entries = filterOutDocID(entries, docID)
	entries = append(entries, temporalEntry{At: at, Type: recordType, DocID: docID})
	sort.Slice(entries, func(i, j int) bool { return entries[i].At < entries[j].At })
	return storeTemporal(tx, entityID, itemID, entries)
}

// temporalRemove deletes the entry matching docID from (entity, itemID).
func temporalRemove(tx *bolt.Tx, entityID, itemID, docID string) error {
	entries, err := loadTemporal(tx, entityID, itemID)
	if err != nil {
		return err
	}
	entries = filterOutDocID(entries, docID)
	if len(entries) == 0 {
		b := tx.Bucket([]byte(temporalPrefix))
		_ = b
		return deleteTemporalKey(tx, entityID, itemID)
	}
	return storeTemporal(tx, entityID, itemID, entries)
}

// temporalRead returns the event log for (entity, itemID), filtered to
// `types` if non-empty, sorted by At ascending.
func temporalRead(tx *bolt.Tx, entityID, itemID string, types []string) ([]temporalEntry, error) {
	entries, err := loadTemporal(tx, entityID, itemID)
	if err != nil {
		return nil, err
	}
	if len(types) == 0 {
		return entries, nil
	}
	allowed := make(map[string]struct{}, len(types))
	for _, t := range types {
		allowed[t] = struct{}{}
	}
	out := make([]temporalEntry, 0, len(entries))
	for _, e := range entries {
		if _, ok := allowed[e.Type]; ok {
			out = append(out, e)
		}
	}
	return out, nil
}

func loadTemporal(tx *bolt.Tx, entityID, itemID string) ([]temporalEntry, error) {
	b := tx.Bucket(temporalBucketName(entityID, itemID))
	if b == nil {
		return nil, nil
	}
	raw := b.Get([]byte("log"))
	if raw == nil {
		return nil, nil
	}
	var entries []temporalEntry
	if err := json.Unmarshal(raw, &entries); err != nil {
		return nil, fmt.Errorf("temporal: decode log: %w", err)
	}
	return entries, nil
}

func storeTemporal(tx *bolt.Tx, entityID, itemID string, entries []temporalEntry) error {
	b, err := tx.CreateBucketIfNotExists(temporalBucketName(entityID, itemID))
	if err != nil {
		return err
	}
	raw, err := json.Marshal(entries)
	if err != nil {
		return fmt.Errorf("temporal: encode log: %w", err)
	}
	return b.Put([]byte("log"), raw)
}

func deleteTemporalKey(tx *bolt.Tx, entityID, itemID string) error {
	name := temporalBucketName(entityID, itemID)
	if b := tx.Bucket(name); b == nil {
		return nil
	}
	return tx.DeleteBucket(name)
}

func filterOutDocID(entries []temporalEntry, docID string) []temporalEntry {
	out := entries[:0]
	for _, e := range entries {
		if e.DocID != docID {
			out = append(out, e)
		}
	}
	return out
}
