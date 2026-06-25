package bboltstore

import (
	"math"
	"testing"

	bolt "go.etcd.io/bbolt"
)

func TestNumIndexEncodeIsSortable(t *testing.T) {
	t.Parallel()
	values := []float64{-1e9, -100.5, -1, -0.0001, 0, 0.0001, 1, 100.5, 1e9}
	encoded := make([][]byte, len(values))
	for i, v := range values {
		encoded[i] = numEncode(v, 0)
	}
	for i := 1; i < len(values); i++ {
		if string(encoded[i-1]) > string(encoded[i]) {
			t.Fatalf("encoding not sortable at index %d (%g vs %g)", i, values[i-1], values[i])
		}
	}
}

func TestNumIndexRoundtrip(t *testing.T) {
	t.Parallel()
	values := []float64{-1e6, -1.5, 0, 1.5, 1e6, math.Pi, -math.E}
	for _, v := range values {
		key := numEncode(v, 0)
		got := decodeFloat(key[:8])
		if got != v {
			t.Fatalf("roundtrip %g → %g", v, got)
		}
	}
}

func TestNumIndexRange(t *testing.T) {
	t.Parallel()
	s := openTxStore(t)
	entries := []struct {
		value      float64
		internalID uint32
	}{
		{10.0, 1},
		{20.0, 2},
		{30.0, 3},
		{40.0, 4},
		{50.0, 5},
	}
	if err := s.db.Update(func(tx *bolt.Tx) error {
		for _, e := range entries {
			if err := numIndexAdd(tx, "tenant-a", "km", e.value, e.internalID); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}

	cases := []struct {
		name             string
		min, max         float64
		minExc, maxExc   bool
		want             []uint32
	}{
		{"closed full", 0, 100, false, false, []uint32{1, 2, 3, 4, 5}},
		{"closed mid", 20, 40, false, false, []uint32{2, 3, 4}},
		{"open min", 20, 40, true, false, []uint32{3, 4}},
		{"open max", 20, 40, false, true, []uint32{2, 3}},
		{"open both", 20, 40, true, true, []uint32{3}},
		{"empty range", 100, 200, false, false, nil},
		{"point match", 30, 30, false, false, []uint32{3}},
		{"reverse bounds", 50, 10, false, false, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got []uint32
			if err := s.db.View(func(tx *bolt.Tx) error {
				bm, err := numIndexRange(tx, "tenant-a", "km", tc.min, tc.max, tc.minExc, tc.maxExc)
				if err != nil {
					return err
				}
				got = bm.ToArray()
				return nil
			}); err != nil {
				t.Fatalf("View: %v", err)
			}
			if !equalUint32(got, tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestNumIndexNaNInfBounds(t *testing.T) {
	t.Parallel()
	s := openTxStore(t)
	if err := s.db.View(func(tx *bolt.Tx) error {
		if _, err := numIndexRange(tx, "tenant-a", "x", math.NaN(), 10, false, false); err == nil {
			t.Fatal("NaN min: expected error")
		}
		if _, err := numIndexRange(tx, "tenant-a", "x", 0, math.Inf(1), false, false); err == nil {
			t.Fatal("Inf max: expected error")
		}
		return nil
	}); err != nil {
		t.Fatalf("View: %v", err)
	}
}

func TestNumIndexNaNInfValuesSilentlySkipped(t *testing.T) {
	t.Parallel()
	s := openTxStore(t)
	err := s.db.Update(func(tx *bolt.Tx) error {
		if err := numIndexAdd(tx, "tenant-a", "x", math.NaN(), 1); err != nil {
			return err
		}
		return numIndexAdd(tx, "tenant-a", "x", math.Inf(1), 2)
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if err := s.db.View(func(tx *bolt.Tx) error {
		bm, err := numIndexRange(tx, "tenant-a", "x", -1e9, 1e9, false, false)
		if err != nil {
			return err
		}
		if !bm.IsEmpty() {
			t.Fatalf("NaN/Inf should not have been indexed, got %v", bm.ToArray())
		}
		return nil
	}); err != nil {
		t.Fatalf("View: %v", err)
	}
}

func equalUint32(a, b []uint32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
