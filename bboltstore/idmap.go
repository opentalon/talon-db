package bboltstore

import (
	"encoding/binary"
	"errors"
	"fmt"

	bolt "go.etcd.io/bbolt"
)

// idmap maintains a bidirectional mapping between caller-facing string
// docIDs and the dense uint32 identifiers the inverted / numeric /
// group-by indexes use as roaring bitmap members. The mapping is
// per-entity (per tenant), append-only (assignments are never reused
// — even after Delete — so historical bitmaps stay correct), and lives
// in three sibling buckets:
//
//	idmap:{entity}:fwd   — string docID  → 4-byte big-endian uint32
//	idmap:{entity}:rev   — 4-byte uint32 → string docID
//	idmap:{entity}:meta  — single key "next" holding the monotonic uint32
//
// All idmap operations run inside an existing bbolt transaction (read
// for Lookup, write for Assign) so they compose naturally with the
// document Put / Delete tx.

const (
	idmapFwdPrefix  = "idmap:"
	idmapFwdSuffix  = ":fwd"
	idmapRevSuffix  = ":rev"
	idmapMetaSuffix = ":meta"
	idmapMetaNext   = "next"
)

func idmapFwdName(entityID string) []byte {
	return []byte(idmapFwdPrefix + entityID + idmapFwdSuffix)
}
func idmapRevName(entityID string) []byte {
	return []byte(idmapFwdPrefix + entityID + idmapRevSuffix)
}
func idmapMetaName(entityID string) []byte {
	return []byte(idmapFwdPrefix + entityID + idmapMetaSuffix)
}

// idmapAssign returns the internal uint32 for (entityID, docID),
// assigning a fresh one if none exists. Must be called inside a
// writable transaction.
func idmapAssign(tx *bolt.Tx, entityID, docID string) (uint32, error) {
	fwd, err := tx.CreateBucketIfNotExists(idmapFwdName(entityID))
	if err != nil {
		return 0, err
	}
	if existing := fwd.Get([]byte(docID)); existing != nil {
		if len(existing) != 4 {
			return 0, fmt.Errorf("idmap: corrupt fwd entry for %q (len %d)", docID, len(existing))
		}
		return binary.BigEndian.Uint32(existing), nil
	}
	rev, err := tx.CreateBucketIfNotExists(idmapRevName(entityID))
	if err != nil {
		return 0, err
	}
	meta, err := tx.CreateBucketIfNotExists(idmapMetaName(entityID))
	if err != nil {
		return 0, err
	}
	var next uint32 = 1
	if raw := meta.Get([]byte(idmapMetaNext)); raw != nil {
		if len(raw) != 4 {
			return 0, fmt.Errorf("idmap: corrupt meta entry (len %d)", len(raw))
		}
		next = binary.BigEndian.Uint32(raw)
	}
	if next == 0 {
		return 0, errors.New("idmap: counter exhausted (uint32 wraparound)")
	}

	var keyBuf [4]byte
	binary.BigEndian.PutUint32(keyBuf[:], next)
	if err := fwd.Put([]byte(docID), append([]byte(nil), keyBuf[:]...)); err != nil {
		return 0, err
	}
	if err := rev.Put(append([]byte(nil), keyBuf[:]...), []byte(docID)); err != nil {
		return 0, err
	}
	var nextBuf [4]byte
	binary.BigEndian.PutUint32(nextBuf[:], next+1)
	if err := meta.Put([]byte(idmapMetaNext), append([]byte(nil), nextBuf[:]...)); err != nil {
		return 0, err
	}
	return next, nil
}

// idmapLookup returns the internal uint32 for (entityID, docID) if it
// exists. The bool is false when no mapping has been assigned. Safe to
// call inside read-only transactions.
func idmapLookup(tx *bolt.Tx, entityID, docID string) (uint32, bool, error) {
	fwd := tx.Bucket(idmapFwdName(entityID))
	if fwd == nil {
		return 0, false, nil
	}
	raw := fwd.Get([]byte(docID))
	if raw == nil {
		return 0, false, nil
	}
	if len(raw) != 4 {
		return 0, false, fmt.Errorf("idmap: corrupt fwd entry for %q (len %d)", docID, len(raw))
	}
	return binary.BigEndian.Uint32(raw), true, nil
}

// idmapReverse returns the string docID for a given internal uint32.
// The bool is false when no such mapping exists (e.g. the uint32 was
// never assigned — which should not happen in normal operation, but
// keeping the bool surfaces corruption rather than masking it).
func idmapReverse(tx *bolt.Tx, entityID string, internalID uint32) (string, bool, error) {
	rev := tx.Bucket(idmapRevName(entityID))
	if rev == nil {
		return "", false, nil
	}
	var keyBuf [4]byte
	binary.BigEndian.PutUint32(keyBuf[:], internalID)
	raw := rev.Get(keyBuf[:])
	if raw == nil {
		return "", false, nil
	}
	return string(raw), true, nil
}
