package bboltstore_test

import (
	"context"
	"testing"
)

func TestGroupCountAggregates(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	ctx := context.Background()
	docs := map[string][]byte{
		"a": []byte(`{"item_id":"item-1","type":"inspect","at":100,"outcome":"pass"}`),
		"b": []byte(`{"item_id":"item-1","type":"inspect","at":200,"outcome":"fail"}`),
		"c": []byte(`{"item_id":"item-1","type":"inspect","at":300,"outcome":"pass"}`),
		"d": []byte(`{"item_id":"item-2","type":"inspect","at":400,"outcome":"pass"}`),
	}
	if err := s.BatchPut(ctx, "tenant-a", docs); err != nil {
		t.Fatalf("BatchPut: %v", err)
	}

	g, err := s.GroupCount(ctx, "tenant-a", "item-1", "type", "inspect")
	if err != nil {
		t.Fatalf("GroupCount: %v", err)
	}
	if g.Count != 3 {
		t.Fatalf("Count = %d, want 3", g.Count)
	}
	if !sameDocs(g.DocIDs, []string{"a", "b", "c"}) {
		t.Fatalf("DocIDs = %v, want [a b c]", flatten(g.DocIDs))
	}
	if g.First.UnixNano() != 100 || g.Last.UnixNano() != 300 {
		t.Fatalf("timestamps: first=%d last=%d", g.First.UnixNano(), g.Last.UnixNano())
	}

	g2, _ := s.GroupCount(ctx, "tenant-a", "item-1", "outcome", "pass")
	if g2.Count != 2 {
		t.Fatalf("outcome=pass Count = %d, want 2", g2.Count)
	}
}

func TestGroupCountMissingReturnsZero(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	g, err := s.GroupCount(context.Background(), "tenant-a", "nope", "type", "inspect")
	if err != nil {
		t.Fatalf("GroupCount: %v", err)
	}
	if g.Count != 0 {
		t.Fatalf("Count = %d, want 0", g.Count)
	}
}

func TestGroupCountDecrementOnDelete(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	ctx := context.Background()
	_ = s.Put(ctx, "tenant-a", "a", []byte(`{"item_id":"i1","type":"x"}`))
	_ = s.Put(ctx, "tenant-a", "b", []byte(`{"item_id":"i1","type":"x"}`))
	_ = s.Delete(ctx, "tenant-a", "a")
	g, _ := s.GroupCount(ctx, "tenant-a", "i1", "type", "x")
	if g.Count != 1 {
		t.Fatalf("after delete: Count = %d, want 1", g.Count)
	}
}
