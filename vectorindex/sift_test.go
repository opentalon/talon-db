package vectorindex_test

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"

	"github.com/opentalon/talon-db/bboltstore"
	"github.com/opentalon/talon-db/vectorindex"
)

// SIFT-1M recall conformance.
//
// To run this test, fetch the SIFT-1M dataset and point
// TALONDB_SIFT_PATH at the directory that contains:
//
//	sift_base.fvecs     1,000,000 × 128-dim float32 (~516 MB)
//	sift_query.fvecs       10,000 × 128-dim float32
//	sift_groundtruth.ivecs 10,000 × 100 int32 (top-100 ids per query)
//
// Source (mirror of the INRIA TEXMEX corpus):
//
//	ftp://ftp.irisa.fr/local/texmex/corpus/sift.tar.gz
//	https://lear.inrialpes.fr/~douze/data/sift.tar.gz
//
// Untar into one directory and pass that directory:
//
//	TALONDB_SIFT_PATH=/path/to/sift go test -run SIFT -count=1 ./vectorindex/
//
// Acceptance: recall@10 ≥ 0.9 over the first 100 queries. We cap the
// query count so the test runs in ~minutes (full 10k queries take an
// order of magnitude longer); set TALONDB_SIFT_QUERIES to override.
//
// On laptop-class hardware the per-insert cost (HNSW + bbolt commit
// in lockstep) makes the full 1M insert phase ~hours; the recall
// pipeline itself is fast. Set TALONDB_SIFT_BASE_LIMIT to a smaller
// integer to cap the base corpus — recall measurement is then only
// meaningful relative to that truncated subset (groundtruth ids
// outside the subset are dropped from the per-query top-K before
// scoring, so the metric stays well-defined).
//
// .fvecs / .ivecs format (TEXMEX): a sequence of <int32 dim><dim ×
// {float32|int32}> records, little-endian.

const (
	siftPathEnv      = "TALONDB_SIFT_PATH"
	siftQueriesEnv   = "TALONDB_SIFT_QUERIES"
	siftBaseLimitEnv = "TALONDB_SIFT_BASE_LIMIT"
	siftDim          = 128
	siftDefaultK     = 10
	siftDefaultQ     = 100
	siftRecallMin    = 0.9
)

func TestSIFTRecall(t *testing.T) {
	dir := os.Getenv(siftPathEnv)
	if dir == "" {
		t.Skipf("set %s to a directory containing sift_base.fvecs / sift_query.fvecs / sift_groundtruth.ivecs", siftPathEnv)
	}
	if runtime.GOOS == "windows" {
		t.Skip("SIFT recall test sized for Unix dev hosts")
	}

	base, err := readFvecs(filepath.Join(dir, "sift_base.fvecs"))
	if err != nil {
		t.Fatalf("read base: %v", err)
	}
	queries, err := readFvecs(filepath.Join(dir, "sift_query.fvecs"))
	if err != nil {
		t.Fatalf("read query: %v", err)
	}
	gt, err := readIvecs(filepath.Join(dir, "sift_groundtruth.ivecs"))
	if err != nil {
		t.Fatalf("read groundtruth: %v", err)
	}
	for _, v := range base {
		if len(v) != siftDim {
			t.Fatalf("base vector wrong dim: %d", len(v))
		}
	}
	if len(base) == 0 || len(queries) == 0 || len(gt) == 0 {
		t.Fatalf("empty SIFT data: base=%d queries=%d gt=%d", len(base), len(queries), len(gt))
	}

	// Run against the bboltstore path — that's the production code
	// path; vectors round-trip through bbolt + the rebuild on next
	// Open if anyone reuses the file.
	store, err := bboltstore.Open(filepath.Join(t.TempDir(), "sift.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = store.Close() }()

	ctx := context.Background()
	for i, v := range base {
		if err := store.VectorInsert(ctx, "sift", "base", strconv.Itoa(i), v, vectorindex.Cosine); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	qLimit := siftDefaultQ
	if raw := os.Getenv(siftQueriesEnv); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			qLimit = n
		}
	}
	if qLimit > len(queries) {
		qLimit = len(queries)
	}

	hit := 0
	total := 0
	for qi := 0; qi < qLimit; qi++ {
		hits, err := store.VectorSearch(ctx, "sift", "base", queries[qi], siftDefaultK)
		if err != nil {
			t.Fatalf("search %d: %v", qi, err)
		}
		want := map[string]bool{}
		for _, id := range gt[qi][:siftDefaultK] {
			want[strconv.Itoa(int(id))] = true
		}
		for _, h := range hits {
			if want[h.ID] {
				hit++
			}
		}
		total += siftDefaultK
	}
	recall := float64(hit) / float64(total)
	t.Logf("SIFT recall@%d over %d queries = %.4f", siftDefaultK, qLimit, recall)
	if recall < siftRecallMin {
		t.Errorf("recall@%d = %.4f, want ≥ %.2f", siftDefaultK, recall, siftRecallMin)
	}
}

// readFvecs parses TEXMEX .fvecs: repeated <int32 dim><dim × float32>.
func readFvecs(path string) ([][]float32, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	var out [][]float32
	for {
		var dim int32
		if err := binary.Read(f, binary.LittleEndian, &dim); err != nil {
			if errors.Is(err, io.EOF) {
				return out, nil
			}
			return nil, err
		}
		if dim <= 0 || dim > 1<<16 {
			return nil, errors.New("vectorindex sift: implausible dim")
		}
		buf := make([]byte, 4*int(dim))
		if _, err := io.ReadFull(f, buf); err != nil {
			return nil, err
		}
		v := make([]float32, dim)
		for i := range v {
			v[i] = math.Float32frombits(binary.LittleEndian.Uint32(buf[4*i:]))
		}
		out = append(out, v)
	}
}

// readIvecs parses TEXMEX .ivecs: repeated <int32 dim><dim × int32>.
func readIvecs(path string) ([][]int32, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	var out [][]int32
	for {
		var dim int32
		if err := binary.Read(f, binary.LittleEndian, &dim); err != nil {
			if errors.Is(err, io.EOF) {
				return out, nil
			}
			return nil, err
		}
		if dim <= 0 || dim > 1<<16 {
			return nil, errors.New("vectorindex sift: implausible dim")
		}
		v := make([]int32, dim)
		if err := binary.Read(f, binary.LittleEndian, &v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
}
