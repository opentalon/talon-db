// Package bboltstore provides the bbolt-backed implementation of
// talondb.DocumentStore. Documents are snappy-compressed and stored in
// per-tenant buckets; metadata (created_at, updated_at, version) lives
// alongside in a sibling bucket and is updated in the same transaction.
package bboltstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	talondb "github.com/opentalon/talon-db"

	"github.com/golang/snappy"
	bolt "go.etcd.io/bbolt"
)

const (
	docsBucketPrefix = "docs:"
	metaBucketPrefix = "meta:"
)

// Store is a bbolt-backed DocumentStore.
type Store struct {
	db  *bolt.DB
	now func() time.Time
}

// Open opens (or creates) a bbolt database at path and returns a Store.
// Callers must call Close when done.
func Open(path string) (*Store, error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("bboltstore: open %q: %w", path, err)
	}
	return &Store{db: db, now: time.Now}, nil
}

// Close closes the underlying bbolt database.
func (s *Store) Close() error {
	return s.db.Close()
}

// docMeta is the metadata stored alongside each document.
type docMeta struct {
	CreatedAt int64 `json:"created_at"`
	UpdatedAt int64 `json:"updated_at"`
	Version   int64 `json:"version"`
}

// Put writes a document. It overwrites any existing document with the
// same (entityID, docID) and bumps the version counter.
func (s *Store) Put(ctx context.Context, entityID, docID string, doc []byte) error {
	if err := validateIDs(entityID, docID); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		return s.putInTx(tx, entityID, docID, doc)
	})
}

// Get returns the document at (entityID, docID), decompressed.
func (s *Store) Get(ctx context.Context, entityID, docID string) ([]byte, error) {
	if err := validateIDs(entityID, docID); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var out []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		docs := tx.Bucket([]byte(docsBucketPrefix + entityID))
		if docs == nil {
			return talondb.ErrNotFound
		}
		raw := docs.Get([]byte(docID))
		if raw == nil {
			return talondb.ErrNotFound
		}
		decoded, err := snappy.Decode(nil, raw)
		if err != nil {
			return fmt.Errorf("bboltstore: snappy decode: %w", err)
		}
		out = decoded
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Delete removes the document at (entityID, docID). Deleting a missing
// document is not an error.
func (s *Store) Delete(ctx context.Context, entityID, docID string) error {
	if err := validateIDs(entityID, docID); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		var oldDoc []byte
		if b := tx.Bucket([]byte(docsBucketPrefix + entityID)); b != nil {
			if prior := b.Get([]byte(docID)); prior != nil {
				decoded, err := snappy.Decode(nil, prior)
				if err != nil {
					return fmt.Errorf("bboltstore: decode prior doc for %q: %w", docID, err)
				}
				oldDoc = decoded
			}
			if err := b.Delete([]byte(docID)); err != nil {
				return err
			}
		}
		if b := tx.Bucket([]byte(metaBucketPrefix + entityID)); b != nil {
			if err := b.Delete([]byte(docID)); err != nil {
				return err
			}
		}
		if oldDoc != nil {
			return indexDocOnDelete(tx, entityID, docID, oldDoc)
		}
		return nil
	})
}

// BatchPut writes multiple documents for a single entity in one atomic
// transaction. If any document is invalid or ctx is cancelled mid-batch,
// no documents are written.
func (s *Store) BatchPut(ctx context.Context, entityID string, docs map[string][]byte) error {
	if err := validateEntityID(entityID); err != nil {
		return err
	}
	for docID := range docs {
		if err := validateDocID(docID); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		for docID, doc := range docs {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := s.putInTx(tx, entityID, docID, doc); err != nil {
				return err
			}
		}
		return nil
	})
}

// Scan visits every document for entityID. Iteration halts when fn
// returns false. The byte slice passed to fn is only valid for the
// duration of the call.
func (s *Store) Scan(ctx context.Context, entityID string, fn func(docID string, doc []byte) bool) error {
	if err := validateEntityID(entityID); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	err := s.db.View(func(tx *bolt.Tx) error {
		docs := tx.Bucket([]byte(docsBucketPrefix + entityID))
		if docs == nil {
			return nil
		}
		return docs.ForEach(func(k, v []byte) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			decoded, err := snappy.Decode(nil, v)
			if err != nil {
				return fmt.Errorf("bboltstore: snappy decode at %q: %w", string(k), err)
			}
			if !fn(string(k), decoded) {
				return errStopScan
			}
			return nil
		})
	})
	if errors.Is(err, errStopScan) {
		return nil
	}
	return err
}

var errStopScan = errors.New("stop scan")

func (s *Store) putInTx(tx *bolt.Tx, entityID, docID string, doc []byte) error {
	docsBucket, err := tx.CreateBucketIfNotExists([]byte(docsBucketPrefix + entityID))
	if err != nil {
		return err
	}
	metaBucket, err := tx.CreateBucketIfNotExists([]byte(metaBucketPrefix + entityID))
	if err != nil {
		return err
	}

	// Capture the prior doc bytes (if any) for index delta computation.
	var oldDoc []byte
	if prior := docsBucket.Get([]byte(docID)); prior != nil {
		decoded, err := snappy.Decode(nil, prior)
		if err != nil {
			return fmt.Errorf("bboltstore: decode prior doc for %q: %w", docID, err)
		}
		oldDoc = decoded
	}

	now := s.now().UnixNano()
	var m docMeta
	if existing := metaBucket.Get([]byte(docID)); existing != nil {
		if err := json.Unmarshal(existing, &m); err != nil {
			return fmt.Errorf("bboltstore: decode meta for %q: %w", docID, err)
		}
		m.UpdatedAt = now
		m.Version++
	} else {
		m = docMeta{CreatedAt: now, UpdatedAt: now, Version: 1}
	}
	metaBytes, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("bboltstore: encode meta for %q: %w", docID, err)
	}
	if err := metaBucket.Put([]byte(docID), metaBytes); err != nil {
		return err
	}

	compressed := snappy.Encode(nil, doc)
	if err := docsBucket.Put([]byte(docID), compressed); err != nil {
		return err
	}
	return indexDocOnPut(tx, entityID, docID, oldDoc, doc)
}

func validateIDs(entityID, docID string) error {
	if err := validateEntityID(entityID); err != nil {
		return err
	}
	return validateDocID(docID)
}

func validateEntityID(entityID string) error {
	if entityID == "" {
		return fmt.Errorf("%w: empty", talondb.ErrInvalidEntityID)
	}
	if strings.Contains(entityID, ":") {
		return fmt.Errorf("%w: contains reserved character ':'", talondb.ErrInvalidEntityID)
	}
	return nil
}

func validateDocID(docID string) error {
	if docID == "" {
		return errors.New("bboltstore: docID must not be empty")
	}
	return nil
}

var _ talondb.DocumentStore = (*Store)(nil)
