package bboltstore

import (
	"encoding/json"
	"fmt"

	roaring "github.com/RoaringBitmap/roaring/v2"
	bolt "go.etcd.io/bbolt"
)

// Closure table: pre-computed category ancestor / descendant tables.
//
// Storage layout:
//
//	clo:asc:{entity}  — key: categoryDocID
//	                    value: JSON []string [parent_id, grandparent_id, ..., root_id]
//	clo:desc:{entity} — key: rootCategoryDocID
//	                    value: serialized roaring bitmap of descendant internal docIDs
//
// A document participates in the closure table when it has a
// top-level "parent" field (string). The category's own identifier is
// its docID; the value of "parent" is another docID. Cycles are
// guarded by a hard depth cap (closureMaxDepth) so a malformed corpus
// can never lock up a Put forever.

const (
	closureAscPrefix  = "clo:asc:"
	closureDescPrefix = "clo:desc:"
	closureMaxDepth   = 256
)

func closureAscBucket(entityID string) []byte  { return []byte(closureAscPrefix + entityID) }
func closureDescBucket(entityID string) []byte { return []byte(closureDescPrefix + entityID) }

// closureParent returns the doc's parent ID (the value of the "parent"
// top-level field) if present and string-typed. ok=false otherwise.
func closureParent(doc []byte) (parentID string, ok bool) {
	if len(doc) == 0 {
		return "", false
	}
	var m map[string]any
	if err := json.Unmarshal(doc, &m); err != nil {
		return "", false
	}
	raw, has := m["parent"]
	if !has {
		return "", false
	}
	s, isStr := raw.(string)
	if !isStr || s == "" {
		return "", false
	}
	return s, true
}

// closureBuildAncestors returns the parent chain for `parentID` rooted
// at the closure table (i.e. [parentID, asc[parentID]...]).
func closureBuildAncestors(tx *bolt.Tx, entityID, parentID string) ([]string, error) {
	out := []string{parentID}
	b := tx.Bucket(closureAscBucket(entityID))
	if b == nil {
		return out, nil
	}
	raw := b.Get([]byte(parentID))
	if raw == nil {
		return out, nil
	}
	var existing []string
	if err := json.Unmarshal(raw, &existing); err != nil {
		return nil, fmt.Errorf("closure: decode asc: %w", err)
	}
	out = append(out, existing...)
	if len(out) > closureMaxDepth {
		return nil, fmt.Errorf("closure: depth exceeds %d for %q", closureMaxDepth, parentID)
	}
	return out, nil
}

// closureAdd registers (docID, parentID) by writing asc[docID] and
// extending every ancestor's desc bitmap.
func closureAdd(tx *bolt.Tx, entityID, docID, parentID string, internalID uint32) error {
	chain, err := closureBuildAncestors(tx, entityID, parentID)
	if err != nil {
		return err
	}
	for _, anc := range chain {
		if anc == docID {
			return fmt.Errorf("closure: cycle detected at %q", docID)
		}
	}
	ascB, err := tx.CreateBucketIfNotExists(closureAscBucket(entityID))
	if err != nil {
		return err
	}
	raw, err := json.Marshal(chain)
	if err != nil {
		return err
	}
	if err := ascB.Put([]byte(docID), raw); err != nil {
		return err
	}
	descB, err := tx.CreateBucketIfNotExists(closureDescBucket(entityID))
	if err != nil {
		return err
	}
	for _, anc := range chain {
		bm, err := readBitmap(descB, []byte(anc))
		if err != nil {
			return err
		}
		bm.Add(internalID)
		if err := writeBitmap(descB, []byte(anc), bm); err != nil {
			return err
		}
	}
	return nil
}

// closureRemove undoes closureAdd: removes asc[docID] and clears the
// internalID from every ancestor's desc bitmap.
func closureRemove(tx *bolt.Tx, entityID, docID string, internalID uint32) error {
	ascB := tx.Bucket(closureAscBucket(entityID))
	if ascB == nil {
		return nil
	}
	raw := ascB.Get([]byte(docID))
	if raw == nil {
		return nil
	}
	var chain []string
	if err := json.Unmarshal(raw, &chain); err != nil {
		return fmt.Errorf("closure: decode asc: %w", err)
	}
	if err := ascB.Delete([]byte(docID)); err != nil {
		return err
	}
	descB := tx.Bucket(closureDescBucket(entityID))
	if descB == nil {
		return nil
	}
	for _, anc := range chain {
		bm, err := readBitmap(descB, []byte(anc))
		if err != nil {
			return err
		}
		bm.Remove(internalID)
		if bm.IsEmpty() {
			if err := descB.Delete([]byte(anc)); err != nil {
				return err
			}
			continue
		}
		if err := writeBitmap(descB, []byte(anc), bm); err != nil {
			return err
		}
	}
	return nil
}

func closureReadAncestors(tx *bolt.Tx, entityID, docID string) ([]string, error) {
	b := tx.Bucket(closureAscBucket(entityID))
	if b == nil {
		return nil, nil
	}
	raw := b.Get([]byte(docID))
	if raw == nil {
		return nil, nil
	}
	var chain []string
	if err := json.Unmarshal(raw, &chain); err != nil {
		return nil, fmt.Errorf("closure: decode asc: %w", err)
	}
	return chain, nil
}

func closureReadDescendants(tx *bolt.Tx, entityID, rootID string) (*roaring.Bitmap, error) {
	b := tx.Bucket(closureDescBucket(entityID))
	if b == nil {
		return roaring.New(), nil
	}
	return readBitmap(b, []byte(rootID))
}

func readBitmap(b *bolt.Bucket, key []byte) (*roaring.Bitmap, error) {
	bm := roaring.New()
	raw := b.Get(key)
	if raw == nil {
		return bm, nil
	}
	if _, err := bm.FromBuffer(append([]byte(nil), raw...)); err != nil {
		return nil, err
	}
	return bm, nil
}

