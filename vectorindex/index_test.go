package vectorindex_test

import (
	"errors"
	"math/rand"
	"testing"

	"github.com/opentalon/talon-db/vectorindex"
)

func TestInsertSearchRoundtrip(t *testing.T) {
	t.Parallel()
	idx := vectorindex.New()
	for i, v := range [][]float32{
		{1, 0, 0},
		{0, 1, 0},
		{0, 0, 1},
	} {
		if err := idx.Insert("t", "embed3", string(rune('a'+i)), v, vectorindex.Cosine); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}
	res, err := idx.Search("t", "embed3", []float32{1, 0, 0}, 2)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(res) != 2 {
		t.Fatalf("got %d results, want 2", len(res))
	}
	if res[0].ID != "a" {
		t.Errorf("nearest = %q, want %q", res[0].ID, "a")
	}
}

// TestMixedDimensionScopes drives the headline motivation for the
// per-scope layout: three scopes under the same tenant carry vectors
// from three different embedding models (3, 8, 16 dimensions) and a
// query in the right dimension lands a hit in the right scope without
// any cross-contamination.
func TestMixedDimensionScopes(t *testing.T) {
	t.Parallel()
	idx := vectorindex.New()

	// Scope "small" — 3-dim toy vectors.
	for i, v := range [][]float32{
		{1, 0, 0},
		{0, 1, 0},
		{0, 0, 1},
	} {
		if err := idx.Insert("t", "small", "s"+itoa(i), v, vectorindex.Cosine); err != nil {
			t.Fatalf("small[%d]: %v", i, err)
		}
	}

	// Scope "medium" — 8-dim, two well-separated clusters.
	clusterA := []float32{1, 1, 1, 1, 0, 0, 0, 0}
	clusterB := []float32{0, 0, 0, 0, 1, 1, 1, 1}
	for i, v := range [][]float32{clusterA, clusterB} {
		if err := idx.Insert("t", "medium", "m"+itoa(i), v, vectorindex.Cosine); err != nil {
			t.Fatalf("medium[%d]: %v", i, err)
		}
	}

	// Scope "large" — 16-dim; cardinality 4.
	for i := 0; i < 4; i++ {
		v := make([]float32, 16)
		v[i] = 1
		if err := idx.Insert("t", "large", "l"+itoa(i), v, vectorindex.Cosine); err != nil {
			t.Fatalf("large[%d]: %v", i, err)
		}
	}

	// Each scope reports its own dimension.
	for _, c := range []struct {
		scope string
		want  int
		count int
	}{
		{"small", 3, 3},
		{"medium", 8, 2},
		{"large", 16, 4},
	} {
		if d, _ := idx.Dim("t", c.scope); d != c.want {
			t.Errorf("Dim(%q) = %d, want %d", c.scope, d, c.want)
		}
		if n := idx.Len("t", c.scope); n != c.count {
			t.Errorf("Len(%q) = %d, want %d", c.scope, n, c.count)
		}
	}

	// Search in each scope's own dimension lands on the right cluster.
	res, err := idx.Search("t", "small", []float32{1, 0, 0}, 1)
	if err != nil || len(res) != 1 || res[0].ID != "s0" {
		t.Errorf("small search: %v %v", err, res)
	}
	res, err = idx.Search("t", "medium", clusterB, 1)
	if err != nil || len(res) != 1 || res[0].ID != "m1" {
		t.Errorf("medium search: %v %v", err, res)
	}
	q := make([]float32, 16)
	q[2] = 1
	res, err = idx.Search("t", "large", q, 1)
	if err != nil || len(res) != 1 || res[0].ID != "l2" {
		t.Errorf("large search: %v %v", err, res)
	}

	// Cross-dimension queries are rejected. Sending an 8-dim query into
	// the 3-dim "small" scope must surface ErrDimensionMismatch; the
	// vectors must never silently coerce or look up nonsense neighbours.
	if _, err := idx.Search("t", "small", clusterA, 1); !errors.Is(err, vectorindex.ErrDimensionMismatch) {
		t.Errorf("cross-dim search: want ErrDimensionMismatch, got %v", err)
	}
	if _, err := idx.Search("t", "medium", []float32{1, 0, 0}, 1); !errors.Is(err, vectorindex.ErrDimensionMismatch) {
		t.Errorf("cross-dim search (reverse): want ErrDimensionMismatch, got %v", err)
	}
}

func TestPerScopeDimensionsAreIndependent(t *testing.T) {
	// Two scopes under the same entity, distinct dims. The vector
	// surface must not bleed one into the other.
	t.Parallel()
	idx := vectorindex.New()
	if err := idx.Insert("t", "model_a", "1", []float32{1, 0, 0}, vectorindex.Cosine); err != nil {
		t.Fatalf("Insert A: %v", err)
	}
	if err := idx.Insert("t", "model_b", "1", make([]float32, 384), vectorindex.Cosine); err != nil {
		t.Fatalf("Insert B: %v", err)
	}
	if d, _ := idx.Dim("t", "model_a"); d != 3 {
		t.Errorf("model_a dim = %d, want 3", d)
	}
	if d, _ := idx.Dim("t", "model_b"); d != 384 {
		t.Errorf("model_b dim = %d, want 384", d)
	}
}

func TestDimensionLockOnFirstInsert(t *testing.T) {
	t.Parallel()
	idx := vectorindex.New()
	if err := idx.Insert("t", "s", "1", []float32{1, 2, 3}, vectorindex.Cosine); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	err := idx.Insert("t", "s", "2", []float32{1, 2}, vectorindex.Cosine)
	if !errors.Is(err, vectorindex.ErrDimensionMismatch) {
		t.Errorf("want ErrDimensionMismatch, got %v", err)
	}
}

func TestSearchOnUnknownScope(t *testing.T) {
	t.Parallel()
	idx := vectorindex.New()
	_, err := idx.Search("t", "never_written", []float32{1, 2, 3}, 5)
	if !errors.Is(err, vectorindex.ErrScopeNotFound) {
		t.Errorf("want ErrScopeNotFound, got %v", err)
	}
}

func TestSearchClampsKToCardinality(t *testing.T) {
	t.Parallel()
	idx := vectorindex.New()
	for i := 0; i < 3; i++ {
		_ = idx.Insert("t", "s", string(rune('a'+i)), []float32{float32(i), 1, 1}, vectorindex.Cosine)
	}
	res, err := idx.Search("t", "s", []float32{0, 1, 1}, 50)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(res) != 3 {
		t.Errorf("got %d results, want 3 (clamped)", len(res))
	}
}

func TestEntityIsolation(t *testing.T) {
	// Tenant a and tenant b write into the same scope name with
	// incompatible dimensions; they MUST NOT interfere.
	t.Parallel()
	idx := vectorindex.New()
	if err := idx.Insert("a", "s", "1", []float32{1, 0}, vectorindex.Cosine); err != nil {
		t.Fatalf("a: %v", err)
	}
	if err := idx.Insert("b", "s", "1", []float32{1, 0, 0, 0}, vectorindex.Cosine); err != nil {
		t.Fatalf("b: %v", err)
	}
	if d, _ := idx.Dim("a", "s"); d != 2 {
		t.Errorf("a dim = %d, want 2", d)
	}
	if d, _ := idx.Dim("b", "s"); d != 4 {
		t.Errorf("b dim = %d, want 4", d)
	}
}

func TestRejectsEmptyVector(t *testing.T) {
	t.Parallel()
	idx := vectorindex.New()
	if err := idx.Insert("t", "s", "1", nil, vectorindex.Cosine); !errors.Is(err, vectorindex.ErrEmptyVector) {
		t.Errorf("Insert: want ErrEmptyVector, got %v", err)
	}
	_ = idx.Insert("t", "s", "1", []float32{1, 2}, vectorindex.Cosine)
	if _, err := idx.Search("t", "s", nil, 1); !errors.Is(err, vectorindex.ErrEmptyVector) {
		t.Errorf("Search: want ErrEmptyVector, got %v", err)
	}
}

func TestReplaceById(t *testing.T) {
	// HNSW's Add replaces existing nodes. Inserting under the same id
	// twice should leave Len at 1 and Search should reflect the
	// latest vector.
	t.Parallel()
	idx := vectorindex.New()
	_ = idx.Insert("t", "s", "v", []float32{1, 0}, vectorindex.Cosine)
	_ = idx.Insert("t", "s", "v", []float32{0, 1}, vectorindex.Cosine)
	if got := idx.Len("t", "s"); got != 1 {
		t.Fatalf("len = %d, want 1", got)
	}
	res, _ := idx.Search("t", "s", []float32{0, 1}, 1)
	if len(res) != 1 || res[0].ID != "v" {
		t.Fatalf("nearest = %v", res)
	}
	// Cosine distance to itself is ~0.
	if res[0].Distance > 0.01 {
		t.Errorf("expected near-zero distance after replace, got %v", res[0].Distance)
	}
}

func TestRecallSanityOnSyntheticClusters(t *testing.T) {
	// Embed 200 points in 2 well-separated clusters (8-dim). For a
	// query near cluster A's centre, recall@10 of cluster-A members
	// must be ≥ 0.9. This is the smallest "real" recall check we can
	// run without downloading SIFT-1M; PR 2 will add the SIFT
	// benchmark.
	t.Parallel()
	const dim = 8
	rng := rand.New(rand.NewSource(42))
	idx := vectorindex.New()

	centreA := []float32{1, 1, 1, 1, 0, 0, 0, 0}
	centreB := []float32{0, 0, 0, 0, 1, 1, 1, 1}
	clusterA := map[string]bool{}
	for i := 0; i < 100; i++ {
		v := jitter(rng, centreA, dim, 0.1)
		id := "a" + itoa(i)
		clusterA[id] = true
		_ = idx.Insert("t", "s", id, v, vectorindex.Cosine)
	}
	for i := 0; i < 100; i++ {
		v := jitter(rng, centreB, dim, 0.1)
		id := "b" + itoa(i)
		_ = idx.Insert("t", "s", id, v, vectorindex.Cosine)
	}
	res, err := idx.Search("t", "s", centreA, 10)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(res) != 10 {
		t.Fatalf("want 10 results, got %d", len(res))
	}
	hit := 0
	for _, r := range res {
		if clusterA[r.ID] {
			hit++
		}
	}
	// 9 of 10 nearest to centreA should be from cluster A.
	if hit < 9 {
		t.Errorf("recall@10 = %d/10, want ≥ 9", hit)
	}
}

func jitter(rng *rand.Rand, centre []float32, dim int, scale float32) []float32 {
	out := make([]float32, dim)
	for i := range out {
		out[i] = centre[i] + (rng.Float32()-0.5)*2*scale
	}
	return out
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [12]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
