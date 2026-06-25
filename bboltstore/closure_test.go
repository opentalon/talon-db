package bboltstore_test

import (
	"context"
	"testing"
)

func TestClosureAncestorsAndDescendants(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	ctx := context.Background()
	// Three-level tree: Vehicles → Trucks → BigRigs
	_ = s.Put(ctx, "tenant-a", "Vehicles", []byte(`{}`))
	_ = s.Put(ctx, "tenant-a", "Trucks", []byte(`{"parent":"Vehicles"}`))
	_ = s.Put(ctx, "tenant-a", "BigRigs", []byte(`{"parent":"Trucks"}`))
	_ = s.Put(ctx, "tenant-a", "Sedans", []byte(`{"parent":"Vehicles"}`))

	chain, err := s.Ancestors(ctx, "tenant-a", "BigRigs")
	if err != nil {
		t.Fatalf("Ancestors: %v", err)
	}
	want := []string{"Trucks", "Vehicles"}
	if !equalStringSlices(chain, want) {
		t.Fatalf("Ancestors(BigRigs) = %v, want %v", chain, want)
	}

	d, err := s.Descendants(ctx, "tenant-a", "Vehicles")
	if err != nil {
		t.Fatalf("Descendants: %v", err)
	}
	if !sameDocs(d, []string{"Trucks", "BigRigs", "Sedans"}) {
		t.Fatalf("Descendants(Vehicles) = %v, want [BigRigs Sedans Trucks]", flatten(d))
	}
}

func TestClosureReparenting(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	ctx := context.Background()
	_ = s.Put(ctx, "tenant-a", "A", []byte(`{}`))
	_ = s.Put(ctx, "tenant-a", "B", []byte(`{}`))
	_ = s.Put(ctx, "tenant-a", "C", []byte(`{"parent":"A"}`))

	d, _ := s.Descendants(ctx, "tenant-a", "A")
	if !sameDocs(d, []string{"C"}) {
		t.Fatalf("initial A descendants = %v, want [C]", flatten(d))
	}

	// Re-parent C to B.
	_ = s.Put(ctx, "tenant-a", "C", []byte(`{"parent":"B"}`))

	d, _ = s.Descendants(ctx, "tenant-a", "A")
	if d.Len() != 0 {
		t.Fatalf("after reparent, A should have no descendants, got %v", flatten(d))
	}
	d, _ = s.Descendants(ctx, "tenant-a", "B")
	if !sameDocs(d, []string{"C"}) {
		t.Fatalf("after reparent, B descendants = %v, want [C]", flatten(d))
	}
}

func TestClosureDeleteRemovesFromAncestors(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	ctx := context.Background()
	_ = s.Put(ctx, "tenant-a", "A", []byte(`{}`))
	_ = s.Put(ctx, "tenant-a", "B", []byte(`{"parent":"A"}`))
	_ = s.Delete(ctx, "tenant-a", "B")

	d, _ := s.Descendants(ctx, "tenant-a", "A")
	if d.Len() != 0 {
		t.Fatalf("A should have no descendants after B delete, got %v", flatten(d))
	}
}

func equalStringSlices(a, b []string) bool {
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
