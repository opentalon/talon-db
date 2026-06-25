package bboltstore

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"

	roaring "github.com/RoaringBitmap/roaring/v2"
	bolt "go.etcd.io/bbolt"
)

// Numeric range index: per-attribute sorted (value, docID) keys.
//
// Storage layout:
//
//	num:{entity}:{attr} — key: 8-byte sortable float64 + 4-byte uint32 docID
//	                     value: empty
//
// The 8-byte float prefix uses the standard IEEE-754 → sortable
// transform (flip sign bit for positives, flip all bits for negatives)
// so byte-wise comparison matches numeric ordering. Adding the docID
// suffix lets multiple documents share the same value without
// colliding.

const numIndexPrefix = "num:"

func numIndexBucketName(entityID, attr string) []byte {
	return []byte(numIndexPrefix + entityID + ":" + attr)
}

// numEncode produces the 12-byte key for (value, internalID).
func numEncode(value float64, internalID uint32) []byte {
	var buf [12]byte
	bits := math.Float64bits(value)
	if bits>>63 == 1 {
		bits = ^bits
	} else {
		bits ^= 1 << 63
	}
	binary.BigEndian.PutUint64(buf[0:8], bits)
	binary.BigEndian.PutUint32(buf[8:12], internalID)
	return buf[:]
}

// numEncodeBound produces an 8-byte key for use as a cursor bound. The
// `upper` flag controls how docID-collision behaves: lower bound uses
// the smallest possible suffix, upper bound the largest.
func numEncodeBound(value float64, upper bool) []byte {
	var buf [8]byte
	bits := math.Float64bits(value)
	if bits>>63 == 1 {
		bits = ^bits
	} else {
		bits ^= 1 << 63
	}
	binary.BigEndian.PutUint64(buf[:], bits)
	if upper {
		return append(buf[:], 0xff, 0xff, 0xff, 0xff)
	}
	return append(buf[:], 0x00, 0x00, 0x00, 0x00)
}

// numIndexAdd inserts (value, internalID) into the attr's sorted set.
func numIndexAdd(tx *bolt.Tx, entityID, attr string, value float64, internalID uint32) error {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return nil
	}
	b, err := tx.CreateBucketIfNotExists(numIndexBucketName(entityID, attr))
	if err != nil {
		return err
	}
	return b.Put(numEncode(value, internalID), nil)
}

// numIndexRemove deletes (value, internalID) from the attr's sorted
// set. Missing entries are not an error.
func numIndexRemove(tx *bolt.Tx, entityID, attr string, value float64, internalID uint32) error {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return nil
	}
	b := tx.Bucket(numIndexBucketName(entityID, attr))
	if b == nil {
		return nil
	}
	return b.Delete(numEncode(value, internalID))
}

// numIndexRange returns a bitmap of internalIDs whose attr-value falls
// in the (min, max) range adjusted by opts. NaN/Inf bounds return an
// error so callers don't paper over invalid queries.
func numIndexRange(tx *bolt.Tx, entityID, attr string, min, max float64, minExclusive, maxExclusive bool) (*roaring.Bitmap, error) {
	if math.IsNaN(min) || math.IsNaN(max) || math.IsInf(min, 0) || math.IsInf(max, 0) {
		return nil, fmt.Errorf("numidx: invalid bound (NaN/Inf)")
	}
	if min > max {
		return roaring.New(), nil
	}
	result := roaring.New()
	b := tx.Bucket(numIndexBucketName(entityID, attr))
	if b == nil {
		return result, nil
	}
	cursor := b.Cursor()
	lo := numEncodeBound(min, false)
	hi := numEncodeBound(max, true)
	for k, _ := cursor.Seek(lo); k != nil && bytes.Compare(k, hi) <= 0; k, _ = cursor.Next() {
		if len(k) != 12 {
			return nil, fmt.Errorf("numidx: malformed key (len %d)", len(k))
		}
		entryValue := decodeFloat(k[:8])
		if minExclusive && entryValue == min {
			continue
		}
		if maxExclusive && entryValue == max {
			continue
		}
		internalID := binary.BigEndian.Uint32(k[8:12])
		result.Add(internalID)
	}
	return result, nil
}

func decodeFloat(b []byte) float64 {
	bits := binary.BigEndian.Uint64(b)
	if bits>>63 == 1 {
		bits ^= 1 << 63
	} else {
		bits = ^bits
	}
	return math.Float64frombits(bits)
}
