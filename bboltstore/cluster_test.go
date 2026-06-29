package bboltstore

import (
	"context"
	"testing"
	"time"
)

// fixedEntries builds a deterministic temporal-entry slice for the
// pure-function unit tests below. Each entry's Type defaults to "x"
// unless overridden by the caller.
func fixedEntries(times ...int64) []temporalEntry {
	out := make([]temporalEntry, len(times))
	for i, t := range times {
		out[i] = temporalEntry{At: t, Type: "x", DocID: docIDFromIndex(i)}
	}
	return out
}

func docIDFromIndex(i int) string {
	switch i {
	case 0:
		return "a"
	case 1:
		return "b"
	case 2:
		return "c"
	case 3:
		return "d"
	case 4:
		return "e"
	case 5:
		return "f"
	}
	return "z"
}

func TestClusterScanBasic(t *testing.T) {
	// Three events within a 10-unit window → one cluster of 3.
	entries := fixedEntries(0, 3, 5)
	got := clusterScan(entries, 10*time.Nanosecond, 3)
	if len(got) != 1 {
		t.Fatalf("got %d clusters, want 1: %+v", len(got), got)
	}
	if len(got[0].Events) != 3 {
		t.Fatalf("cluster size %d, want 3", len(got[0].Events))
	}
}

func TestClusterScanTooFewMembers(t *testing.T) {
	entries := fixedEntries(0, 5)
	got := clusterScan(entries, 100*time.Nanosecond, 3)
	if len(got) != 0 {
		t.Fatalf("expected no clusters, got %+v", got)
	}
}

func TestClusterScanWindowTooNarrow(t *testing.T) {
	// Three events spanning 20 units; window is 5.
	entries := fixedEntries(0, 10, 20)
	got := clusterScan(entries, 5*time.Nanosecond, 3)
	if len(got) != 0 {
		t.Fatalf("expected no clusters, got %+v", got)
	}
}

func TestClusterScanNonOverlapping(t *testing.T) {
	// Six events in two distinct windows: {0,1,2} and {100,101,102}.
	entries := fixedEntries(0, 1, 2, 100, 101, 102)
	got := clusterScan(entries, 5*time.Nanosecond, 3)
	if len(got) != 2 {
		t.Fatalf("got %d clusters, want 2: %+v", len(got), got)
	}
	if len(got[0].Events) != 3 || len(got[1].Events) != 3 {
		t.Fatalf("cluster sizes %d/%d, want 3/3", len(got[0].Events), len(got[1].Events))
	}
	if got[0].First.UnixNano() != 0 || got[1].First.UnixNano() != 100 {
		t.Fatalf("cluster firsts wrong: %d / %d", got[0].First.UnixNano(), got[1].First.UnixNano())
	}
}

func TestClusterScanGreedyExtendsMaximally(t *testing.T) {
	// Five events within window — algorithm extends j as far as possible.
	entries := fixedEntries(0, 1, 2, 3, 4)
	got := clusterScan(entries, 10*time.Nanosecond, 3)
	if len(got) != 1 {
		t.Fatalf("got %d clusters, want 1: %+v", len(got), got)
	}
	if len(got[0].Events) != 5 {
		t.Fatalf("cluster size %d, want 5 (greedy extension)", len(got[0].Events))
	}
}

func TestClusterScanGreedyNoOverlapAdvances(t *testing.T) {
	// First cluster swallows indices 0-3, then 4-6 must form its own.
	entries := fixedEntries(0, 1, 2, 3, 100, 101, 102)
	got := clusterScan(entries, 10*time.Nanosecond, 3)
	if len(got) != 2 {
		t.Fatalf("got %d clusters, want 2: %+v", len(got), got)
	}
	if len(got[0].Events) != 4 {
		t.Fatalf("first cluster size %d, want 4", len(got[0].Events))
	}
	if len(got[1].Events) != 3 {
		t.Fatalf("second cluster size %d, want 3", len(got[1].Events))
	}
}

func TestClusterScanZeroWindowNoUpperBound(t *testing.T) {
	// Window=0 means every consecutive matching event is in the same
	// cluster (treated as no upper bound).
	entries := fixedEntries(0, 1_000_000, 5_000_000_000)
	got := clusterScan(entries, 0, 2)
	if len(got) != 1 || len(got[0].Events) != 3 {
		t.Fatalf("zero-window expected single 3-event cluster, got %+v", got)
	}
}

func TestClusterScanMinSizeOne(t *testing.T) {
	// minSize=1 yields every event as its own cluster (a degenerate
	// but well-defined case).
	entries := fixedEntries(0, 100, 200)
	got := clusterScan(entries, 1*time.Nanosecond, 1)
	if len(got) != 3 {
		t.Fatalf("got %d clusters, want 3 singletons: %+v", len(got), got)
	}
}

func TestClusterScanEmpty(t *testing.T) {
	got := clusterScan(nil, 10*time.Nanosecond, 1)
	if got != nil {
		t.Fatalf("nil entries should yield nil, got %+v", got)
	}
}

func TestClusterScanMinSizeBeyondTotal(t *testing.T) {
	entries := fixedEntries(0, 1)
	got := clusterScan(entries, 100*time.Nanosecond, 5)
	if got != nil {
		t.Fatalf("minSize > len(entries) should yield nil, got %+v", got)
	}
}

// ---- integration test via the public ClusterQuery method ----

func TestClusterQueryEndToEnd(t *testing.T) {
	t.Parallel()
	path := tempDB(t)
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()

	// Three failures within ~30 days, then a long gap, then two more
	// (not enough for minSize=3).
	day := int64(24 * 60 * 60 * 1e9)
	for i, at := range []int64{0, 5 * day, 25 * day, 200 * day, 205 * day} {
		doc := []byte(`{"item_id":"truck-7","type":"failure","at":` + intToStr(at) + `}`)
		if err := s.Put(ctx, "tenant-a", "evt-"+intToStr(int64(i)), doc); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}

	clusters, err := s.ClusterQuery(ctx, "tenant-a", "truck-7", []string{"failure"}, 90*24*time.Hour, 3)
	if err != nil {
		t.Fatalf("ClusterQuery: %v", err)
	}
	if len(clusters) != 1 {
		t.Fatalf("got %d clusters, want 1: %+v", len(clusters), clusters)
	}
	if len(clusters[0].Events) != 3 {
		t.Fatalf("cluster size %d, want 3", len(clusters[0].Events))
	}
}

// tempDB returns a fresh bbolt path inside t.TempDir.
func tempDB(t *testing.T) string {
	t.Helper()
	return t.TempDir() + "/cluster.bbolt"
}

func intToStr(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}
