package bboltstore

import (
	"fmt"

	"github.com/opentalon/talon-db/internal/index"
	bolt "go.etcd.io/bbolt"
)

// indexDocOnPut maintains every secondary index for the (entityID,
// docID) being written. `oldDoc` is the prior contents (nil for a
// brand-new document) and `newDoc` is the contents about to be
// committed. Must run inside the same writable transaction as the
// docs/meta bucket writes — atomicity depends on it.
//
// Each per-index hook computes its own delta from oldDoc / newDoc; the
// dispatcher only assigns the internal uint32 and forwards.
func indexDocOnPut(tx *bolt.Tx, entityID, docID string, oldDoc, newDoc []byte) error {
	internalID, err := idmapAssign(tx, entityID, docID)
	if err != nil {
		return fmt.Errorf("indexhook: idmap assign: %w", err)
	}

	oldTerms, err := extractTerms(oldDoc)
	if err != nil {
		return fmt.Errorf("indexhook: extract old: %w", err)
	}
	newTerms, err := extractTerms(newDoc)
	if err != nil {
		return fmt.Errorf("indexhook: extract new: %w", err)
	}

	if err := updateInvertedIndex(tx, entityID, internalID, oldTerms.Terms, newTerms.Terms); err != nil {
		return fmt.Errorf("indexhook: inverted: %w", err)
	}

	return nil
}

// indexDocOnDelete clears every secondary-index entry for the
// document being removed. Runs in the same write tx as the doc/meta
// bucket deletes.
func indexDocOnDelete(tx *bolt.Tx, entityID, docID string, oldDoc []byte) error {
	internalID, found, err := idmapLookup(tx, entityID, docID)
	if err != nil {
		return fmt.Errorf("indexhook: idmap lookup: %w", err)
	}
	if !found {
		return nil
	}
	terms, err := extractTerms(oldDoc)
	if err != nil {
		return fmt.Errorf("indexhook: extract: %w", err)
	}
	if err := updateInvertedIndex(tx, entityID, internalID, terms.Terms, nil); err != nil {
		return fmt.Errorf("indexhook: inverted: %w", err)
	}
	return nil
}

// extractTerms is a thin wrapper that swallows nil/empty inputs and
// non-JSON content. The DocumentStore contract treats document bytes
// as opaque; if a caller stores raw bytes that aren't JSON, they get
// document storage but no secondary-index entries — never an error.
func extractTerms(doc []byte) (index.Result, error) {
	if len(doc) == 0 {
		return index.Result{}, nil
	}
	r, err := index.Extract(doc)
	if err != nil {
		return index.Result{}, nil
	}
	return r, nil
}

// updateInvertedIndex computes the delta between oldTerms and
// newTerms and adjusts each affected term's bitmap. Both slices must
// be sorted (the extractor guarantees this).
func updateInvertedIndex(tx *bolt.Tx, entityID string, internalID uint32, oldTerms, newTerms []string) error {
	removed, added := diffSortedTerms(oldTerms, newTerms)
	for _, term := range removed {
		if err := invIndexRemove(tx, entityID, term, internalID); err != nil {
			return err
		}
	}
	for _, term := range added {
		if err := invIndexAdd(tx, entityID, term, internalID); err != nil {
			return err
		}
	}
	return nil
}

// diffSortedTerms returns (removed, added) between two sorted,
// de-duplicated slices.
func diffSortedTerms(old, new []string) (removed, added []string) {
	i, j := 0, 0
	for i < len(old) && j < len(new) {
		switch {
		case old[i] < new[j]:
			removed = append(removed, old[i])
			i++
		case old[i] > new[j]:
			added = append(added, new[j])
			j++
		default:
			i++
			j++
		}
	}
	removed = append(removed, old[i:]...)
	added = append(added, new[j:]...)
	return removed, added
}
