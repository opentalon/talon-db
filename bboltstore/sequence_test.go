package bboltstore

import (
	"context"
	"testing"
	"time"
)

func TestSequenceWalkBasic(t *testing.T) {
	entries := []temporalEntry{
		{At: 0, Type: "inspect", DocID: "a"},
		{At: 100, Type: "fault", DocID: "b"},
	}
	got := sequenceWalk(entries, []string{"inspect", "fault"}, 0)
	if len(got) != 2 || got[0].DocID != "a" || got[1].DocID != "b" {
		t.Fatalf("walk = %+v", got)
	}
}

func TestSequenceWalkRequiresOrder(t *testing.T) {
	// fault before inspect — sequence inspect→fault doesn't match.
	entries := []temporalEntry{
		{At: 0, Type: "fault", DocID: "a"},
		{At: 100, Type: "inspect", DocID: "b"},
	}
	got := sequenceWalk(entries, []string{"inspect", "fault"}, 0)
	if got != nil {
		t.Fatalf("expected nil, got %+v", got)
	}
}

func TestSequenceWalkSkipsIntermediateTypes(t *testing.T) {
	entries := []temporalEntry{
		{At: 0, Type: "inspect", DocID: "a"},
		{At: 50, Type: "noise", DocID: "x"},
		{At: 100, Type: "fault", DocID: "b"},
	}
	got := sequenceWalk(entries, []string{"inspect", "fault"}, 0)
	if len(got) != 2 || got[0].DocID != "a" || got[1].DocID != "b" {
		t.Fatalf("walk skipping noise = %+v", got)
	}
}

func TestSequenceWalkWindowEnforced(t *testing.T) {
	entries := []temporalEntry{
		{At: 0, Type: "inspect", DocID: "a"},
		{At: 100, Type: "fault", DocID: "b"},
	}
	// Span is 100; window is 50. No match.
	got := sequenceWalk(entries, []string{"inspect", "fault"}, 50)
	if got != nil {
		t.Fatalf("walk should fail outside window, got %+v", got)
	}
}

func TestSequenceWalkPicksFirstStartingPoint(t *testing.T) {
	entries := []temporalEntry{
		{At: 0, Type: "inspect", DocID: "first-inspect"},
		{At: 50, Type: "inspect", DocID: "second-inspect"},
		{At: 100, Type: "fault", DocID: "fault"},
	}
	got := sequenceWalk(entries, []string{"inspect", "fault"}, 200)
	if len(got) != 2 || got[0].DocID != "first-inspect" {
		t.Fatalf("walk should pick first-inspect, got %+v", got)
	}
}

func TestSequenceWalkLongerStepList(t *testing.T) {
	entries := []temporalEntry{
		{At: 0, Type: "open", DocID: "a"},
		{At: 50, Type: "investigate", DocID: "b"},
		{At: 100, Type: "resolve", DocID: "c"},
	}
	got := sequenceWalk(entries, []string{"open", "investigate", "resolve"}, 0)
	if len(got) != 3 {
		t.Fatalf("expected 3 events, got %+v", got)
	}
}

func TestSequenceWalkEmptyOnNoFirstStep(t *testing.T) {
	entries := []temporalEntry{
		{At: 0, Type: "noise", DocID: "a"},
	}
	got := sequenceWalk(entries, []string{"inspect", "fault"}, 0)
	if got != nil {
		t.Fatalf("expected nil, got %+v", got)
	}
}

// ---------- integration via SequenceJoin ----------

func TestSequenceJoinScansAllItems(t *testing.T) {
	t.Parallel()
	s, err := Open(tempDB(t))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()

	day := int64(24 * 60 * 60 * 1e9)
	put := func(docID, item, recordType string, at int64) {
		body := `{"item_id":"` + item + `","type":"` + recordType + `","at":` + intToStr(at) + `}`
		if err := s.Put(ctx, "tenant-a", docID, []byte(body)); err != nil {
			t.Fatalf("Put %s: %v", docID, err)
		}
	}
	// truck-1: matching sequence
	put("e1", "truck-1", "inspect", 0)
	put("e2", "truck-1", "fault", 10*day)
	// truck-2: reversed; no match
	put("e3", "truck-2", "fault", 0)
	put("e4", "truck-2", "inspect", 5*day)
	// truck-3: matching but outside window
	put("e5", "truck-3", "inspect", 0)
	put("e6", "truck-3", "fault", 200*day)

	matches, err := s.SequenceJoin(ctx, "tenant-a", nil, []string{"inspect", "fault"}, 30*24*time.Hour)
	if err != nil {
		t.Fatalf("SequenceJoin: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("expected 1 match (truck-1), got %d: %+v", len(matches), matches)
	}
	if matches[0].ItemID != "truck-1" {
		t.Fatalf("matched item = %q, want truck-1", matches[0].ItemID)
	}
}

func TestSequenceJoinFilteredItemIDs(t *testing.T) {
	t.Parallel()
	s, err := Open(tempDB(t))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()

	put := func(docID, item, recordType string, at int64) {
		body := `{"item_id":"` + item + `","type":"` + recordType + `","at":` + intToStr(at) + `}`
		if err := s.Put(ctx, "tenant-a", docID, []byte(body)); err != nil {
			t.Fatalf("Put %s: %v", docID, err)
		}
	}
	put("e1", "truck-1", "inspect", 0)
	put("e2", "truck-1", "fault", 1)
	put("e3", "truck-2", "inspect", 0)
	put("e4", "truck-2", "fault", 1)

	// Restrict to truck-2 only.
	matches, err := s.SequenceJoin(ctx, "tenant-a", []string{"truck-2"}, []string{"inspect", "fault"}, 0)
	if err != nil {
		t.Fatalf("SequenceJoin: %v", err)
	}
	if len(matches) != 1 || matches[0].ItemID != "truck-2" {
		t.Fatalf("expected truck-2 match only, got %+v", matches)
	}
}

func TestSequenceJoinNoStepsReturnsNil(t *testing.T) {
	t.Parallel()
	s, err := Open(tempDB(t))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	matches, err := s.SequenceJoin(context.Background(), "tenant-a", nil, nil, 0)
	if err != nil {
		t.Fatalf("SequenceJoin: %v", err)
	}
	if matches != nil {
		t.Fatalf("empty steps should return nil, got %+v", matches)
	}
}
