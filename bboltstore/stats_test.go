package bboltstore_test

import (
	"context"
	"fmt"
	"math"
	"testing"
)

func TestStatsSimpleMean(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	ctx := context.Background()
	values := []float64{10, 20, 30, 40, 50}
	for i, v := range values {
		doc := []byte(fmt.Sprintf(`{"km":%g}`, v))
		if err := s.Put(ctx, "tenant-a", fmt.Sprintf("v%d", i), doc); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	got, err := s.Stats(ctx, "tenant-a", "km")
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if got.Count != 5 {
		t.Fatalf("Count = %d, want 5", got.Count)
	}
	if math.Abs(got.Mean-30.0) > 1e-9 {
		t.Fatalf("Mean = %g, want 30", got.Mean)
	}
	if got.Min != 10 || got.Max != 50 {
		t.Fatalf("Min/Max = %g/%g, want 10/50", got.Min, got.Max)
	}
	if math.Abs(got.M2-1000) > 1e-9 {
		t.Fatalf("M2 = %g, want 1000 (sum of squared deviations for {10,20,30,40,50})", got.M2)
	}
}

func TestStatsRecomputeAfterDelete(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	ctx := context.Background()
	for i := 1; i <= 5; i++ {
		_ = s.Put(ctx, "tenant-a", fmt.Sprintf("v%d", i), []byte(fmt.Sprintf(`{"km":%d}`, i*10)))
	}
	_ = s.Delete(ctx, "tenant-a", "v3")
	got, err := s.Stats(ctx, "tenant-a", "km")
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if got.Count != 4 {
		t.Fatalf("Count after delete = %d, want 4", got.Count)
	}
	wantMean := (10. + 20. + 40. + 50.) / 4.
	if math.Abs(got.Mean-wantMean) > 1e-9 {
		t.Fatalf("Mean after delete = %g, want %g", got.Mean, wantMean)
	}
}

func TestStatsRecomputeAfterUpdate(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	ctx := context.Background()
	_ = s.Put(ctx, "tenant-a", "v1", []byte(`{"km":100}`))
	_ = s.Put(ctx, "tenant-a", "v1", []byte(`{"km":200}`))
	got, err := s.Stats(ctx, "tenant-a", "km")
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if got.Count != 1 {
		t.Fatalf("Count after update = %d, want 1", got.Count)
	}
	if math.Abs(got.Mean-200) > 1e-9 {
		t.Fatalf("Mean after update = %g, want 200", got.Mean)
	}
}

func TestStatsEmpty(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	got, err := s.Stats(context.Background(), "tenant-a", "missing")
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if got.Count != 0 {
		t.Fatalf("empty: Count = %d", got.Count)
	}
}
