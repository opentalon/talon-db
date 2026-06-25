package bboltstore

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"

	roaring "github.com/RoaringBitmap/roaring/v2"
	bolt "go.etcd.io/bbolt"
)

// Group-by index: pre-aggregated counters per (itemID, attr, value).
//
// Storage layout:
//
//	gby:{entity} — key: itemID|attr|value
//	              value: JSON groupByValue (count + timestamps + bitmap bytes)
//
// A document contributes to a group when it has a top-level "item_id"
// (string) plus one or more top-level scalar fields. Each top-level
// scalar field becomes a group-by tuple. Non-scalar fields (objects,
// arrays) are skipped — they belong to the inverted index.

const groupByPrefix = "gby:"

func groupByBucketName(entityID string) []byte { return []byte(groupByPrefix + entityID) }

type groupByValue struct {
	Count     int64  `json:"count"`
	FirstSeen int64  `json:"first_seen"`
	LastSeen  int64  `json:"last_seen"`
	Bitmap    []byte `json:"bitmap"`
}

// groupByFields returns the (itemID, scalarFields) pair for the doc.
// scalarFields maps attr → stringified value for every top-level
// string/number/bool field other than item_id itself. Returns
// ok=false if the doc has no item_id.
func groupByFields(doc []byte) (itemID string, scalars map[string]string, ok bool) {
	if len(doc) == 0 {
		return "", nil, false
	}
	var m map[string]any
	if err := json.Unmarshal(doc, &m); err != nil {
		return "", nil, false
	}
	itemRaw, hasItem := m["item_id"]
	if !hasItem {
		return "", nil, false
	}
	itemStr, isStr := itemRaw.(string)
	if !isStr {
		return "", nil, false
	}
	scalars = make(map[string]string, len(m))
	for k, v := range m {
		if k == "item_id" {
			continue
		}
		switch x := v.(type) {
		case string:
			scalars[k] = x
		case bool:
			if x {
				scalars[k] = "true"
			} else {
				scalars[k] = "false"
			}
		case float64:
			scalars[k] = strconv.FormatFloat(x, 'g', -1, 64)
		case json.Number:
			scalars[k] = string(x)
		}
	}
	return itemStr, scalars, true
}

func groupByKey(itemID, attr, value string) []byte {
	return []byte(itemID + "|" + attr + "|" + value)
}

// groupByAdd increments the counter, extends the bitmap with
// internalID, and merges timestamps for one (itemID, attr, value).
func groupByAdd(tx *bolt.Tx, entityID, itemID, attr, value string, internalID uint32, at int64) error {
	b, err := tx.CreateBucketIfNotExists(groupByBucketName(entityID))
	if err != nil {
		return err
	}
	key := groupByKey(itemID, attr, value)
	var v groupByValue
	if raw := b.Get(key); raw != nil {
		if err := json.Unmarshal(raw, &v); err != nil {
			return fmt.Errorf("groupby: decode: %w", err)
		}
	}
	bm := roaring.New()
	if len(v.Bitmap) > 0 {
		if _, err := bm.FromBuffer(append([]byte(nil), v.Bitmap...)); err != nil {
			return fmt.Errorf("groupby: decode bitmap: %w", err)
		}
	}
	if bm.CheckedAdd(internalID) {
		v.Count++
	}
	if v.FirstSeen == 0 || at < v.FirstSeen {
		v.FirstSeen = at
	}
	if at > v.LastSeen {
		v.LastSeen = at
	}
	var buf bytes.Buffer
	if _, err := bm.WriteTo(&buf); err != nil {
		return err
	}
	v.Bitmap = buf.Bytes()
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return b.Put(key, raw)
}

// groupByRemove undoes one group's contribution.
func groupByRemove(tx *bolt.Tx, entityID, itemID, attr, value string, internalID uint32) error {
	b := tx.Bucket(groupByBucketName(entityID))
	if b == nil {
		return nil
	}
	key := groupByKey(itemID, attr, value)
	raw := b.Get(key)
	if raw == nil {
		return nil
	}
	var v groupByValue
	if err := json.Unmarshal(raw, &v); err != nil {
		return fmt.Errorf("groupby: decode: %w", err)
	}
	bm := roaring.New()
	if len(v.Bitmap) > 0 {
		if _, err := bm.FromBuffer(append([]byte(nil), v.Bitmap...)); err != nil {
			return fmt.Errorf("groupby: decode bitmap: %w", err)
		}
	}
	if !bm.CheckedRemove(internalID) {
		return nil
	}
	v.Count--
	if v.Count <= 0 || bm.IsEmpty() {
		return b.Delete(key)
	}
	var buf bytes.Buffer
	if _, err := bm.WriteTo(&buf); err != nil {
		return err
	}
	v.Bitmap = buf.Bytes()
	out, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return b.Put(key, out)
}

// groupByRead returns the stored value, or zero if missing.
func groupByRead(tx *bolt.Tx, entityID, itemID, attr, value string) (groupByValue, *roaring.Bitmap, error) {
	b := tx.Bucket(groupByBucketName(entityID))
	if b == nil {
		return groupByValue{}, roaring.New(), nil
	}
	raw := b.Get(groupByKey(itemID, attr, value))
	if raw == nil {
		return groupByValue{}, roaring.New(), nil
	}
	var v groupByValue
	if err := json.Unmarshal(raw, &v); err != nil {
		return groupByValue{}, nil, fmt.Errorf("groupby: decode: %w", err)
	}
	bm := roaring.New()
	if len(v.Bitmap) > 0 {
		if _, err := bm.FromBuffer(append([]byte(nil), v.Bitmap...)); err != nil {
			return groupByValue{}, nil, fmt.Errorf("groupby: decode bitmap: %w", err)
		}
	}
	return v, bm, nil
}
