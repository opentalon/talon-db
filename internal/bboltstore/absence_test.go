package bboltstore_test

import (
	"context"
	"testing"
)

func TestLastSeenTracksMax(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	ctx := context.Background()
	_ = s.Put(ctx, "tenant-a", "d1", []byte(`{"item_id":"v1","type":"inspect","at":1000}`))
	_ = s.Put(ctx, "tenant-a", "d2", []byte(`{"item_id":"v1","type":"inspect","at":2000}`))
	_ = s.Put(ctx, "tenant-a", "d3", []byte(`{"item_id":"v1","type":"inspect","at":500}`))

	at, ok, err := s.LastSeen(ctx, "tenant-a", "v1", "inspect")
	if err != nil {
		t.Fatalf("LastSeen: %v", err)
	}
	if !ok || at.UnixNano() != 2000 {
		t.Fatalf("LastSeen: ok=%v at=%d, want true/2000", ok, at.UnixNano())
	}
}

func TestLastSeenAbsent(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	_, ok, err := s.LastSeen(context.Background(), "tenant-a", "no-such-item", "inspect")
	if err != nil {
		t.Fatalf("LastSeen: %v", err)
	}
	if ok {
		t.Fatal("absent: ok=true, want false")
	}
}

func TestLastSeenRecomputesAfterDeletingMax(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	ctx := context.Background()
	_ = s.Put(ctx, "tenant-a", "d1", []byte(`{"item_id":"v1","type":"inspect","at":1000}`))
	_ = s.Put(ctx, "tenant-a", "d2", []byte(`{"item_id":"v1","type":"inspect","at":2000}`))
	_ = s.Delete(ctx, "tenant-a", "d2") // delete the max-owner
	at, ok, err := s.LastSeen(ctx, "tenant-a", "v1", "inspect")
	if err != nil {
		t.Fatalf("LastSeen: %v", err)
	}
	if !ok || at.UnixNano() != 1000 {
		t.Fatalf("after delete: ok=%v at=%d, want true/1000", ok, at.UnixNano())
	}
}

func TestLastSeenPerType(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	ctx := context.Background()
	_ = s.Put(ctx, "tenant-a", "d1", []byte(`{"item_id":"v1","type":"inspect","at":1000}`))
	_ = s.Put(ctx, "tenant-a", "d2", []byte(`{"item_id":"v1","type":"repair","at":500}`))

	at, _, _ := s.LastSeen(ctx, "tenant-a", "v1", "inspect")
	if at.UnixNano() != 1000 {
		t.Fatalf("inspect: %d", at.UnixNano())
	}
	at, _, _ = s.LastSeen(ctx, "tenant-a", "v1", "repair")
	if at.UnixNano() != 500 {
		t.Fatalf("repair: %d", at.UnixNano())
	}
}
