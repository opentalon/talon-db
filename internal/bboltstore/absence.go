package bboltstore

import (
	"encoding/json"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// Absence index: last-occurrence timestamp per (entity, itemID, recordType).
//
// Storage layout:
//
//	abs:{entity}:{itemID}:{recordType} — value: JSON absenceEntry
//
// "Has item X seen a type-Y record in the last N days?" becomes a
// single bbolt Get. Maintains the owning docID alongside the
// timestamp so a Delete that touches the current-max doc can trigger
// a recompute from the temporal index.

const absencePrefix = "abs:"

func absenceKey(entityID, itemID, recordType string) []byte {
	return []byte(absencePrefix + entityID + ":" + itemID + ":" + recordType)
}

// Single bucket per entity holds every (itemID, recordType) pair as
// individual keys; this keeps the dataset small enough that one bucket
// won't blow up bbolt's page management.
const absenceBucketPrefix = "absbucket:"

func absenceBucketName(entityID string) []byte {
	return []byte(absenceBucketPrefix + entityID)
}

type absenceEntry struct {
	At    int64  `json:"at"`
	DocID string `json:"doc_id"`
}

func absenceCellKey(itemID, recordType string) []byte {
	return []byte(itemID + "|" + recordType)
}

// absenceRecord upserts the (itemID, recordType) → at mapping if `at`
// is newer than what's currently stored. The doc_id is recorded too so
// later Deletes can detect when they need to recompute the max.
func absenceRecord(tx *bolt.Tx, entityID, itemID, recordType, docID string, at int64) error {
	b, err := tx.CreateBucketIfNotExists(absenceBucketName(entityID))
	if err != nil {
		return err
	}
	key := absenceCellKey(itemID, recordType)
	if raw := b.Get(key); raw != nil {
		var cur absenceEntry
		if err := json.Unmarshal(raw, &cur); err != nil {
			return fmt.Errorf("absence: decode: %w", err)
		}
		if at <= cur.At && cur.DocID != docID {
			// Older event; the current-max wins.
			return nil
		}
	}
	out, err := json.Marshal(absenceEntry{At: at, DocID: docID})
	if err != nil {
		return err
	}
	return b.Put(key, out)
}

// absenceRetract handles the Delete case. If this docID owns the
// current-max for (itemID, recordType), recompute the max from the
// temporal index. Otherwise no-op.
func absenceRetract(tx *bolt.Tx, entityID, itemID, recordType, docID string) error {
	b := tx.Bucket(absenceBucketName(entityID))
	if b == nil {
		return nil
	}
	key := absenceCellKey(itemID, recordType)
	raw := b.Get(key)
	if raw == nil {
		return nil
	}
	var cur absenceEntry
	if err := json.Unmarshal(raw, &cur); err != nil {
		return fmt.Errorf("absence: decode: %w", err)
	}
	if cur.DocID != docID {
		return nil
	}
	// Walk the temporal index to find the new max for this type.
	entries, err := temporalRead(tx, entityID, itemID, []string{recordType})
	if err != nil {
		return err
	}
	// Filter out the doc being deleted.
	var newMax absenceEntry
	for _, e := range entries {
		if e.DocID == docID {
			continue
		}
		if e.At > newMax.At {
			newMax = absenceEntry{At: e.At, DocID: e.DocID}
		}
	}
	if newMax.At == 0 {
		return b.Delete(key)
	}
	out, err := json.Marshal(newMax)
	if err != nil {
		return err
	}
	return b.Put(key, out)
}

func absenceLookup(tx *bolt.Tx, entityID, itemID, recordType string) (int64, bool, error) {
	b := tx.Bucket(absenceBucketName(entityID))
	if b == nil {
		return 0, false, nil
	}
	raw := b.Get(absenceCellKey(itemID, recordType))
	if raw == nil {
		return 0, false, nil
	}
	var e absenceEntry
	if err := json.Unmarshal(raw, &e); err != nil {
		return 0, false, fmt.Errorf("absence: decode: %w", err)
	}
	return e.At, true, nil
}
