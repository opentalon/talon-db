package bboltstore_test

import (
	"testing"

	talondb "github.com/opentalon/talon-db"
	"github.com/opentalon/talon-db/talondbtest"
)

// TestIndexedConformance wires the bbolt-backed Store into the
// backend-agnostic IndexedSuite. Specs are documented in
// talondbtest/indexed.go.
func TestIndexedConformance(t *testing.T) {
	talondbtest.IndexedSuite(t, func(t *testing.T) talondb.IndexedStore {
		return newStore(t)
	})
}
