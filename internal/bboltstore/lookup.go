package bboltstore

import (
	"context"
	"fmt"
	"time"

	talondb "github.com/opentalon/talon-db"

	roaring "github.com/RoaringBitmap/roaring/v2"
	bolt "go.etcd.io/bbolt"
)

// Lookup implements talondb.IndexedStore — see indexed.go for the
// contract.
func (s *Store) Lookup(ctx context.Context, entityID, term string) (talondb.DocIDSet, error) {
	if err := validateEntityID(entityID); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var bm *roaring.Bitmap
	if err := s.db.View(func(tx *bolt.Tx) error {
		var err error
		bm, err = invIndexLookup(tx, entityID, term)
		return err
	}); err != nil {
		return nil, err
	}
	return s.materializeDocIDSet(entityID, bm)
}

// GroupCount implements talondb.IndexedStore.
func (s *Store) GroupCount(ctx context.Context, entityID, itemID, attr, value string) (talondb.GroupBucket, error) {
	if err := validateEntityID(entityID); err != nil {
		return talondb.GroupBucket{}, err
	}
	if err := ctx.Err(); err != nil {
		return talondb.GroupBucket{}, err
	}
	var (
		v  groupByValue
		bm *roaring.Bitmap
	)
	if err := s.db.View(func(tx *bolt.Tx) error {
		var err error
		v, bm, err = groupByRead(tx, entityID, itemID, attr, value)
		return err
	}); err != nil {
		return talondb.GroupBucket{}, err
	}
	if v.Count == 0 {
		return talondb.GroupBucket{}, nil
	}
	docIDs, err := s.materializeDocIDSet(entityID, bm)
	if err != nil {
		return talondb.GroupBucket{}, err
	}
	return talondb.GroupBucket{
		Count:  int(v.Count),
		First:  time.Unix(0, v.FirstSeen),
		Last:   time.Unix(0, v.LastSeen),
		DocIDs: docIDs,
	}, nil
}

// WindowQuery implements talondb.IndexedStore. The `window` parameter
// is currently advisory: every matching event is returned in time
// order; callers apply windowing on the result. Kept in the signature
// for forward compatibility with a future server-side window filter.
func (s *Store) WindowQuery(ctx context.Context, entityID, itemID string, types []string, window time.Duration) ([]talondb.TemporalEvent, error) {
	if err := validateEntityID(entityID); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	_ = window
	var entries []temporalEntry
	if err := s.db.View(func(tx *bolt.Tx) error {
		var err error
		entries, err = temporalRead(tx, entityID, itemID, types)
		return err
	}); err != nil {
		return nil, err
	}
	out := make([]talondb.TemporalEvent, len(entries))
	for i, e := range entries {
		out[i] = talondb.TemporalEvent{DocID: e.DocID, Type: e.Type, At: time.Unix(0, e.At)}
	}
	return out, nil
}

// LookupNumericRange implements talondb.IndexedStore.
func (s *Store) LookupNumericRange(ctx context.Context, entityID, attr string, min, max float64, opts talondb.RangeOpts) (talondb.DocIDSet, error) {
	if err := validateEntityID(entityID); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var bm *roaring.Bitmap
	if err := s.db.View(func(tx *bolt.Tx) error {
		var err error
		bm, err = numIndexRange(tx, entityID, attr, min, max, opts.MinExclusive, opts.MaxExclusive)
		return err
	}); err != nil {
		return nil, err
	}
	return s.materializeDocIDSet(entityID, bm)
}

// LookupPrefix implements talondb.IndexedStore.
func (s *Store) LookupPrefix(ctx context.Context, entityID, prefix string) (talondb.DocIDSet, error) {
	if err := validateEntityID(entityID); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var bm *roaring.Bitmap
	if err := s.db.View(func(tx *bolt.Tx) error {
		var err error
		bm, err = invIndexLookupPrefix(tx, entityID, prefix)
		return err
	}); err != nil {
		return nil, err
	}
	return s.materializeDocIDSet(entityID, bm)
}

// materializeDocIDSet resolves every internalID in `bm` back to its
// string docID via the idmap reverse bucket and returns a frozen
// snapshot. The set is detached from any open transaction.
func (s *Store) materializeDocIDSet(entityID string, bm *roaring.Bitmap) (talondb.DocIDSet, error) {
	if bm == nil || bm.IsEmpty() {
		return talondb.EmptyDocIDSet(), nil
	}
	ids := make([]string, 0, bm.GetCardinality())
	if err := s.db.View(func(tx *bolt.Tx) error {
		iter := bm.Iterator()
		for iter.HasNext() {
			internalID := iter.Next()
			docID, ok, err := idmapReverse(tx, entityID, internalID)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("bboltstore: orphan internalID %d in bitmap", internalID)
			}
			ids = append(ids, docID)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return &stringDocIDSet{ids: ids}, nil
}

// stringDocIDSet is a frozen, sorted-by-construction DocIDSet over a
// pre-materialized slice. Safe for concurrent reads.
type stringDocIDSet struct {
	ids []string
}

func (s *stringDocIDSet) Len() int { return len(s.ids) }

func (s *stringDocIDSet) Contains(docID string) bool {
	// Linear scan is fine for the sizes we expect in slice 1; if
	// callers need O(log n) we can swap in sort.Search later.
	for _, id := range s.ids {
		if id == docID {
			return true
		}
	}
	return false
}

func (s *stringDocIDSet) ForEach(fn func(docID string) bool) {
	for _, id := range s.ids {
		if !fn(id) {
			return
		}
	}
}

// AsSortedSlice is a backend convenience for callers that need the
// underlying slice (e.g. for joining with another set). The slice
// MUST NOT be mutated.
func (s *stringDocIDSet) AsSortedSlice() []string {
	return s.ids
}

var _ interface {
	Lookup(context.Context, string, string) (talondb.DocIDSet, error)
	LookupPrefix(context.Context, string, string) (talondb.DocIDSet, error)
	LookupNumericRange(context.Context, string, string, float64, float64, talondb.RangeOpts) (talondb.DocIDSet, error)
	WindowQuery(context.Context, string, string, []string, time.Duration) ([]talondb.TemporalEvent, error)
	GroupCount(context.Context, string, string, string, string) (talondb.GroupBucket, error)
} = (*Store)(nil)
