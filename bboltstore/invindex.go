package bboltstore

import (
	"bytes"
	"fmt"

	roaring "github.com/RoaringBitmap/roaring/v2"
	bolt "go.etcd.io/bbolt"
)

// Inverted index: term → roaring bitmap of internal uint32 docIDs.
//
// Storage layout:
//
//	inv:{entity} — key: term bytes, value: serialized roaring bitmap
//
// Prefix queries use bbolt's native sorted-key iteration. A future
// vellum FST term dictionary could accelerate large-scale prefix
// lookups; the read path is structured so it can be swapped in
// without changing callers.

const invIndexPrefix = "inv:"

func invIndexBucketName(entityID string) []byte {
	return []byte(invIndexPrefix + entityID)
}

// invIndexAdd sets `internalID` in the bitmap for `term`, creating the
// bitmap if absent. Must run inside a writable transaction.
func invIndexAdd(tx *bolt.Tx, entityID, term string, internalID uint32) error {
	b, err := tx.CreateBucketIfNotExists(invIndexBucketName(entityID))
	if err != nil {
		return err
	}
	bm := roaring.New()
	if raw := b.Get([]byte(term)); raw != nil {
		if _, err := bm.FromBuffer(append([]byte(nil), raw...)); err != nil {
			return fmt.Errorf("invindex: decode bitmap for %q: %w", term, err)
		}
	}
	bm.Add(internalID)
	return writeBitmap(b, []byte(term), bm)
}

// invIndexRemove clears `internalID` from the bitmap for `term`. The
// term entry is deleted entirely if the bitmap becomes empty.
func invIndexRemove(tx *bolt.Tx, entityID, term string, internalID uint32) error {
	b := tx.Bucket(invIndexBucketName(entityID))
	if b == nil {
		return nil
	}
	raw := b.Get([]byte(term))
	if raw == nil {
		return nil
	}
	bm := roaring.New()
	if _, err := bm.FromBuffer(append([]byte(nil), raw...)); err != nil {
		return fmt.Errorf("invindex: decode bitmap for %q: %w", term, err)
	}
	bm.Remove(internalID)
	if bm.IsEmpty() {
		return b.Delete([]byte(term))
	}
	return writeBitmap(b, []byte(term), bm)
}

// invIndexLookup returns the bitmap for `term`, or an empty bitmap if
// the term has never been indexed. Caller owns the returned bitmap.
func invIndexLookup(tx *bolt.Tx, entityID, term string) (*roaring.Bitmap, error) {
	b := tx.Bucket(invIndexBucketName(entityID))
	if b == nil {
		return roaring.New(), nil
	}
	raw := b.Get([]byte(term))
	if raw == nil {
		return roaring.New(), nil
	}
	bm := roaring.New()
	if _, err := bm.FromBuffer(append([]byte(nil), raw...)); err != nil {
		return nil, fmt.Errorf("invindex: decode bitmap for %q: %w", term, err)
	}
	return bm, nil
}

// invIndexLookupPrefix returns the union of bitmaps for every term
// starting with `prefix`. An empty prefix matches everything.
func invIndexLookupPrefix(tx *bolt.Tx, entityID, prefix string) (*roaring.Bitmap, error) {
	b := tx.Bucket(invIndexBucketName(entityID))
	if b == nil {
		return roaring.New(), nil
	}
	result := roaring.New()
	cursor := b.Cursor()
	p := []byte(prefix)
	for k, v := cursor.Seek(p); k != nil && bytes.HasPrefix(k, p); k, v = cursor.Next() {
		bm := roaring.New()
		if _, err := bm.FromBuffer(append([]byte(nil), v...)); err != nil {
			return nil, fmt.Errorf("invindex: decode bitmap for %q: %w", k, err)
		}
		result.Or(bm)
	}
	return result, nil
}

func writeBitmap(b *bolt.Bucket, key []byte, bm *roaring.Bitmap) error {
	bm.RunOptimize()
	var buf bytes.Buffer
	if _, err := bm.WriteTo(&buf); err != nil {
		return fmt.Errorf("invindex: encode bitmap: %w", err)
	}
	return b.Put(key, buf.Bytes())
}
