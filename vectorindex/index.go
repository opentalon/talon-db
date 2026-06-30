// Package vectorindex is the in-memory HNSW layer behind talon-db's
// vector RPCs. One process-wide Index owns a map of (entity, scope) →
// HNSW graph. Dimension is locked on first insert into a scope —
// vectors only compare within the same model's embedding space, so
// silently coercing dimensions would break the recall contract.
//
// Persistence is delegated to the caller: this package never touches
// disk. The talon-db bboltstore wraps an Index alongside a raw-vector
// bucket so a process restart can rebuild graphs by replaying the
// stored vectors.
package vectorindex

import (
	"errors"
	"fmt"
	"sync"

	"github.com/coder/hnsw"
)

// Errors returned by Index.
var (
	// ErrDimensionMismatch is returned when an Insert provides a vector
	// whose length differs from the scope's locked dimension, or when a
	// Search query vector doesn't match. Dimension is locked on the
	// first insert into a scope and never changes.
	ErrDimensionMismatch = errors.New("vectorindex: dimension mismatch")

	// ErrScopeNotFound is returned when Search targets a (entity, scope)
	// that has never had an Insert.
	ErrScopeNotFound = errors.New("vectorindex: scope not found")

	// ErrEmptyVector rejects zero-length vectors. HNSW can't compute
	// distance on them and the lock-on-first-insert rule needs at least
	// one component to set the dimension.
	ErrEmptyVector = errors.New("vectorindex: empty vector")
)

// Result is one neighbour returned by Search, ordered nearest-first.
type Result struct {
	ID       string
	Distance float32
}

// Metric selects the HNSW distance function. Default is Cosine.
type Metric int

const (
	// Cosine is 1 - (a·b / (|a| |b|)). Matches the default of most
	// embedding models.
	Cosine Metric = iota
	// Euclidean is sqrt(Σ (a_i - b_i)²).
	Euclidean
)

func (m Metric) distanceFunc() hnsw.DistanceFunc {
	switch m {
	case Euclidean:
		return hnsw.EuclideanDistance
	default:
		return hnsw.CosineDistance
	}
}

// Index is the process-wide vector store. The zero value is unusable —
// callers go through New.
type Index struct {
	mu     sync.RWMutex
	scopes map[scopeKey]*scopeState
}

type scopeKey struct {
	entity string
	scope  string
}

type scopeState struct {
	graph  *hnsw.Graph[string]
	dim    int
	metric Metric
}

// New returns an empty Index.
func New() *Index {
	return &Index{scopes: map[scopeKey]*scopeState{}}
}

// Insert adds (or replaces) a vector under (entity, scope, id). On the
// first call for a (entity, scope) pair the dimension and metric are
// locked; later calls into the same scope must match the dimension or
// receive ErrDimensionMismatch. The metric argument is ignored on
// subsequent calls — passing a different metric is silently accepted
// because the scope's existing graph keeps its original distance
// function.
func (i *Index) Insert(entity, scope, id string, vec []float32, metric Metric) error {
	if err := validateID(entity, scope, id); err != nil {
		return err
	}
	if len(vec) == 0 {
		return ErrEmptyVector
	}
	i.mu.Lock()
	defer i.mu.Unlock()

	key := scopeKey{entity, scope}
	st, ok := i.scopes[key]
	if !ok {
		st = &scopeState{graph: newScopeGraph(metric), dim: len(vec), metric: metric}
		i.scopes[key] = st
	} else if len(vec) != st.dim {
		return fmt.Errorf("%w: scope %q/%q expects dim %d, got %d",
			ErrDimensionMismatch, entity, scope, st.dim, len(vec))
	}
	// v0.6.1's Add panics when the key already exists (its post-add
	// length invariant fails because Add-with-replace doesn't change
	// Len). The workaround is Delete-then-Add — but Delete on the only
	// node leaves the graph in a degenerate state (layers exist with
	// nil entries) that crashes assertDims on the next Add. So when
	// replacing the sole node, rebuild the graph from scratch instead.
	if _, ok := st.graph.Lookup(id); ok {
		if st.graph.Len() == 1 {
			st.graph = newScopeGraph(st.metric)
		} else {
			st.graph.Delete(id)
		}
	}
	st.graph.Add(hnsw.MakeNode(id, append([]float32(nil), vec...)))
	return nil
}

// Search returns the k nearest neighbours of query under (entity,
// scope), ordered closest first. Returns ErrScopeNotFound if the scope
// has never been written to, and ErrDimensionMismatch if the query
// length doesn't match the scope's locked dimension.
//
// k is clamped to the scope's current cardinality — a Search with k
// larger than the population returns every neighbour without error.
func (i *Index) Search(entity, scope string, query []float32, k int) ([]Result, error) {
	if len(query) == 0 {
		return nil, ErrEmptyVector
	}
	if k <= 0 {
		return nil, nil
	}
	i.mu.RLock()
	defer i.mu.RUnlock()

	st, ok := i.scopes[scopeKey{entity, scope}]
	if !ok {
		return nil, ErrScopeNotFound
	}
	if len(query) != st.dim {
		return nil, fmt.Errorf("%w: scope %q/%q expects dim %d, got %d",
			ErrDimensionMismatch, entity, scope, st.dim, len(query))
	}
	// v0.6.1 only exposes Search → []Node, no per-result distance. We
	// recompute distance against the query using the scope's configured
	// metric so callers see a real score in the response.
	hits := st.graph.Search(query, k)
	distFn := st.metric.distanceFunc()
	out := make([]Result, 0, len(hits))
	for _, n := range hits {
		out = append(out, Result{ID: n.Key, Distance: distFn(query, n.Value)})
	}
	return out, nil
}

// Dim returns the locked dimension for (entity, scope). Reports 0 +
// false when the scope is unknown.
func (i *Index) Dim(entity, scope string) (int, bool) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	st, ok := i.scopes[scopeKey{entity, scope}]
	if !ok {
		return 0, false
	}
	return st.dim, true
}

// Len returns the number of vectors stored under (entity, scope). 0
// when the scope is unknown.
func (i *Index) Len(entity, scope string) int {
	i.mu.RLock()
	defer i.mu.RUnlock()
	st, ok := i.scopes[scopeKey{entity, scope}]
	if !ok {
		return 0
	}
	return st.graph.Len()
}

func newScopeGraph(metric Metric) *hnsw.Graph[string] {
	g := hnsw.NewGraph[string]()
	g.Distance = metric.distanceFunc()
	return g
}

func validateID(entity, scope, id string) error {
	if entity == "" {
		return errors.New("vectorindex: empty entity")
	}
	if scope == "" {
		return errors.New("vectorindex: empty scope")
	}
	if id == "" {
		return errors.New("vectorindex: empty id")
	}
	return nil
}
