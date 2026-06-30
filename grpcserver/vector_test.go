package grpcserver_test

import (
	"context"
	"testing"

	"github.com/opentalon/talon-db/proto/talondbpb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGRPCVectorInsertSearchRoundtrip(t *testing.T) {
	t.Parallel()
	client, cleanup := dial(t)
	defer cleanup()
	ctx := context.Background()

	for i, v := range [][]float32{
		{1, 0, 0},
		{0, 1, 0},
		{0, 0, 1},
	} {
		if _, err := client.VectorInsert(ctx, &talondbpb.VectorInsertRequest{
			EntityId: "tenant-a",
			Scope:    "embed3",
			Id:       string(rune('a' + i)),
			Vector:   v,
			Metric:   talondbpb.VectorMetric_VECTOR_METRIC_COSINE,
		}); err != nil {
			t.Fatalf("VectorInsert %d: %v", i, err)
		}
	}
	resp, err := client.VectorSearch(ctx, &talondbpb.VectorSearchRequest{
		EntityId: "tenant-a",
		Scope:    "embed3",
		Vector:   []float32{1, 0, 0},
		K:        2,
	})
	if err != nil {
		t.Fatalf("VectorSearch: %v", err)
	}
	if len(resp.GetHits()) != 2 {
		t.Fatalf("got %d hits, want 2", len(resp.GetHits()))
	}
	if resp.GetHits()[0].GetId() != "a" {
		t.Errorf("nearest = %q, want %q", resp.GetHits()[0].GetId(), "a")
	}
}

func TestGRPCVectorScopeIsolation(t *testing.T) {
	// Three distinct scopes with three different dimensions under the
	// same tenant. Each scope's own dim-matched query should land hits
	// only in that scope; a cross-dim query must come back as
	// InvalidArgument.
	t.Parallel()
	client, cleanup := dial(t)
	defer cleanup()
	ctx := context.Background()

	type vec struct {
		scope string
		id    string
		v     []float32
	}
	for _, w := range []vec{
		{"small", "s1", []float32{1, 0, 0}},
		{"small", "s2", []float32{0, 1, 0}},
		{"medium", "m1", []float32{1, 1, 1, 1, 0, 0, 0, 0}},
		{"medium", "m2", []float32{0, 0, 0, 0, 1, 1, 1, 1}},
		{"large", "l1", append(make([]float32, 15), 1)},
	} {
		if _, err := client.VectorInsert(ctx, &talondbpb.VectorInsertRequest{
			EntityId: "tenant-a", Scope: w.scope, Id: w.id, Vector: w.v,
		}); err != nil {
			t.Fatalf("insert %s/%s: %v", w.scope, w.id, err)
		}
	}

	// Search in each scope's own dim — exactly the right vector returns.
	res, err := client.VectorSearch(ctx, &talondbpb.VectorSearchRequest{
		EntityId: "tenant-a", Scope: "small", Vector: []float32{1, 0, 0}, K: 1,
	})
	if err != nil || len(res.GetHits()) != 1 || res.GetHits()[0].GetId() != "s1" {
		t.Errorf("small search: %v %v", err, res.GetHits())
	}
	res, err = client.VectorSearch(ctx, &talondbpb.VectorSearchRequest{
		EntityId: "tenant-a", Scope: "medium", Vector: []float32{0, 0, 0, 0, 1, 1, 1, 1}, K: 1,
	})
	if err != nil || len(res.GetHits()) != 1 || res.GetHits()[0].GetId() != "m2" {
		t.Errorf("medium search: %v %v", err, res.GetHits())
	}

	// Wrong-dimension query → InvalidArgument from the typed mapper.
	_, err = client.VectorSearch(ctx, &talondbpb.VectorSearchRequest{
		EntityId: "tenant-a", Scope: "small", Vector: []float32{1, 1, 1, 1, 0, 0, 0, 0}, K: 1,
	})
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Errorf("cross-dim search code = %s, want InvalidArgument", got)
	}
}

func TestGRPCVectorUnknownScopeReturnsNotFound(t *testing.T) {
	t.Parallel()
	client, cleanup := dial(t)
	defer cleanup()
	ctx := context.Background()

	_, err := client.VectorSearch(ctx, &talondbpb.VectorSearchRequest{
		EntityId: "tenant-a", Scope: "ghost", Vector: []float32{1, 2, 3}, K: 1,
	})
	if got := status.Code(err); got != codes.NotFound {
		t.Errorf("code = %s, want NotFound", got)
	}
}

func TestGRPCVectorTenantIsolation(t *testing.T) {
	// Two tenants, same scope name, different dimensions. Tenant b's
	// 4-dim search must NOT see tenant a's 3-dim vectors.
	t.Parallel()
	client, cleanup := dial(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := client.VectorInsert(ctx, &talondbpb.VectorInsertRequest{
		EntityId: "tenant-a", Scope: "s", Id: "a1", Vector: []float32{1, 0, 0},
	}); err != nil {
		t.Fatalf("a: %v", err)
	}
	if _, err := client.VectorInsert(ctx, &talondbpb.VectorInsertRequest{
		EntityId: "tenant-b", Scope: "s", Id: "b1", Vector: []float32{1, 0, 0, 0},
	}); err != nil {
		t.Fatalf("b: %v", err)
	}
	res, err := client.VectorSearch(ctx, &talondbpb.VectorSearchRequest{
		EntityId: "tenant-b", Scope: "s", Vector: []float32{1, 0, 0, 0}, K: 5,
	})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(res.GetHits()) != 1 || res.GetHits()[0].GetId() != "b1" {
		t.Errorf("tenant b leak: hits = %v", res.GetHits())
	}
}
