# talon-db

Go-native embedded fact database for the [Talon language](https://github.com/opentalon/talon-language).

**Status:** Phase 3a, pre-alpha. Only the document store layer is implemented today.

## What this is

talon-db is the planned Phase-3 storage backend for Talon, replacing the
Datalevin (JVM) backend behind the `FactStore` interface in
`talon-language`. It is built on five Go primitives: `bbolt` (B+ tree
storage), `roaring` (compressed bitmaps), `vellum` (FST term dictionary),
`hashicorp/raft` (clustering), and `snappy` (compression). Everything
else is custom Go.

The architecture is layered:

```
talon-db
  ├── Document store   (this milestone — bbolt + snappy)
  ├── Index engine     (planned — inverted, temporal, group-by, closures, stats)
  ├── Query engine     (planned — boolean, range, nested, temporal, absence)
  ├── Mutation engine  (planned — pre-commit hooks, reactive triggers, txn)
  ├── Script engine    (planned — bytecode VM, cache, registry)
  └── RETE engine      (planned — incremental match for reactive blocks)
```

## Current milestone

Document store per [talon-language#26](https://github.com/opentalon/talon-language/issues/26):

- Snappy-compressed JSON blobs in a single bbolt file
- Per-tenant bucket isolation (`docs:{entity_id}`, `meta:{entity_id}`)
- ACID transactions via bbolt
- `DocumentStore` interface: `Put` / `Get` / `Delete` / `BatchPut` / `Scan`

## Quick start

```go
import (
    talondb "github.com/opentalon/talon-db"
    "github.com/opentalon/talon-db/bboltstore"
)

var _ talondb.IndexedStore // the surface

store, err := bboltstore.Open("talon.db")
if err != nil { log.Fatal(err) }
defer store.Close()

ctx := context.Background()
_ = store.Put(ctx, "tenant-a", "doc-1", []byte(`{"hello":"world"}`))
doc, _ := store.Get(ctx, "tenant-a", "doc-1")
fmt.Printf("%s\n", doc)
```

## References

- [#15 — RFC: storage engine selection and clustering strategy](https://github.com/opentalon/talon-language/issues/15)
- [#25 — RFC: talon-db umbrella](https://github.com/opentalon/talon-language/issues/25)
- [#26 — Document store + storage engine (bbolt)](https://github.com/opentalon/talon-language/issues/26)
- [#27 — Index engine](https://github.com/opentalon/talon-language/issues/27)
- [#28 — Query engine](https://github.com/opentalon/talon-language/issues/28)
- [#29 — Mutation engine](https://github.com/opentalon/talon-language/issues/29)
- [#30 — Script engine](https://github.com/opentalon/talon-language/issues/30)
- [#89 — RETE-based incremental match engine](https://github.com/opentalon/talon-language/issues/89)

## License

Apache 2.0. See [LICENSE](LICENSE).
