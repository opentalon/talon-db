// Package grpcserver implements the talondbpb.TalonDBServiceServer
// interface as a thin translation layer over talondb.IndexedStore.
// Every RPC method delegates to the matching store call and converts
// errors via google.golang.org/grpc/status.
package grpcserver

import (
	"context"
	"errors"
	"math"
	"time"

	talondb "github.com/opentalon/talon-db"
	"github.com/opentalon/talon-db/proto/talondbpb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Server wraps a talondb.IndexedStore.
type Server struct {
	talondbpb.UnimplementedTalonDBServiceServer
	store   talondb.IndexedStore
	version string
}

// New constructs a Server over the given store. version is reported by
// the Health RPC for clients that want to gate behaviour on it.
func New(store talondb.IndexedStore, version string) *Server {
	return &Server{store: store, version: version}
}

// ---------- DocumentStore ----------

func (s *Server) Put(ctx context.Context, req *talondbpb.PutRequest) (*emptypb.Empty, error) {
	if err := s.store.Put(ctx, req.GetEntityId(), req.GetDocId(), req.GetDoc()); err != nil {
		return nil, mapError(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) Get(ctx context.Context, req *talondbpb.GetRequest) (*talondbpb.GetResponse, error) {
	doc, err := s.store.Get(ctx, req.GetEntityId(), req.GetDocId())
	if errors.Is(err, talondb.ErrNotFound) {
		return &talondbpb.GetResponse{Found: false}, nil
	}
	if err != nil {
		return nil, mapError(err)
	}
	return &talondbpb.GetResponse{Doc: doc, Found: true}, nil
}

func (s *Server) Delete(ctx context.Context, req *talondbpb.DeleteRequest) (*emptypb.Empty, error) {
	if err := s.store.Delete(ctx, req.GetEntityId(), req.GetDocId()); err != nil {
		return nil, mapError(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) BatchPut(ctx context.Context, req *talondbpb.BatchPutRequest) (*emptypb.Empty, error) {
	docs := make(map[string][]byte, len(req.GetEntries()))
	for _, e := range req.GetEntries() {
		docs[e.GetDocId()] = e.GetDoc()
	}
	if err := s.store.BatchPut(ctx, req.GetEntityId(), docs); err != nil {
		return nil, mapError(err)
	}
	return &emptypb.Empty{}, nil
}

// ---------- IndexedStore ----------

func (s *Server) Lookup(ctx context.Context, req *talondbpb.LookupRequest) (*talondbpb.DocIDList, error) {
	set, err := s.store.Lookup(ctx, req.GetEntityId(), req.GetTerm())
	if err != nil {
		return nil, mapError(err)
	}
	return docIDListFromSet(set), nil
}

func (s *Server) LookupPrefix(ctx context.Context, req *talondbpb.LookupPrefixRequest) (*talondbpb.DocIDList, error) {
	set, err := s.store.LookupPrefix(ctx, req.GetEntityId(), req.GetPrefix())
	if err != nil {
		return nil, mapError(err)
	}
	return docIDListFromSet(set), nil
}

func (s *Server) LookupNumericRange(ctx context.Context, req *talondbpb.NumericRangeRequest) (*talondbpb.DocIDList, error) {
	if math.IsNaN(req.GetMin()) || math.IsNaN(req.GetMax()) || math.IsInf(req.GetMin(), 0) || math.IsInf(req.GetMax(), 0) {
		return nil, status.Error(codes.InvalidArgument, "talondb: NaN/Inf bound rejected")
	}
	set, err := s.store.LookupNumericRange(ctx, req.GetEntityId(), req.GetAttr(), req.GetMin(), req.GetMax(), talondb.RangeOpts{
		MinExclusive: req.GetMinExclusive(),
		MaxExclusive: req.GetMaxExclusive(),
	})
	if err != nil {
		return nil, mapError(err)
	}
	return docIDListFromSet(set), nil
}

func (s *Server) WindowQuery(ctx context.Context, req *talondbpb.WindowRequest) (*talondbpb.WindowResponse, error) {
	events, err := s.store.WindowQuery(ctx, req.GetEntityId(), req.GetItemId(), req.GetTypes(), time.Duration(req.GetWindowNanos()))
	if err != nil {
		return nil, mapError(err)
	}
	out := &talondbpb.WindowResponse{Events: make([]*talondbpb.TemporalEvent, 0, len(events))}
	for _, e := range events {
		out.Events = append(out.Events, &talondbpb.TemporalEvent{
			DocId:       e.DocID,
			Type:        e.Type,
			AtUnixNanos: e.At.UnixNano(),
		})
	}
	return out, nil
}

func (s *Server) GroupCount(ctx context.Context, req *talondbpb.GroupRequest) (*talondbpb.GroupResponse, error) {
	g, err := s.store.GroupCount(ctx, req.GetEntityId(), req.GetItemId(), req.GetAttr(), req.GetValue())
	if err != nil {
		return nil, mapError(err)
	}
	return &talondbpb.GroupResponse{
		Count:          int64(g.Count),
		FirstUnixNanos: g.First.UnixNano(),
		LastUnixNanos:  g.Last.UnixNano(),
		DocIds:         collectDocIDs(g.DocIDs),
	}, nil
}

func (s *Server) Stats(ctx context.Context, req *talondbpb.StatsRequest) (*talondbpb.StatsResponse, error) {
	st, err := s.store.Stats(ctx, req.GetEntityId(), req.GetAttr())
	if err != nil {
		return nil, mapError(err)
	}
	return &talondbpb.StatsResponse{
		Count: st.Count,
		Mean:  st.Mean,
		M2:    st.M2,
		Min:   st.Min,
		Max:   st.Max,
	}, nil
}

func (s *Server) LastSeen(ctx context.Context, req *talondbpb.LastSeenRequest) (*talondbpb.LastSeenResponse, error) {
	t, ok, err := s.store.LastSeen(ctx, req.GetEntityId(), req.GetItemId(), req.GetRecordType())
	if err != nil {
		return nil, mapError(err)
	}
	out := &talondbpb.LastSeenResponse{Found: ok}
	if ok {
		out.AtUnixNanos = t.UnixNano()
	}
	return out, nil
}

func (s *Server) Ancestors(ctx context.Context, req *talondbpb.AncestorsRequest) (*talondbpb.StringList, error) {
	chain, err := s.store.Ancestors(ctx, req.GetEntityId(), req.GetCategoryId())
	if err != nil {
		return nil, mapError(err)
	}
	return &talondbpb.StringList{Items: chain}, nil
}

func (s *Server) Descendants(ctx context.Context, req *talondbpb.DescendantsRequest) (*talondbpb.DocIDList, error) {
	set, err := s.store.Descendants(ctx, req.GetEntityId(), req.GetRootId())
	if err != nil {
		return nil, mapError(err)
	}
	return docIDListFromSet(set), nil
}

// ---------- Operational ----------

func (s *Server) Health(ctx context.Context, _ *emptypb.Empty) (*talondbpb.HealthResponse, error) {
	return &talondbpb.HealthResponse{Status: "ok", Version: s.version}, nil
}

// ---------- helpers ----------

func docIDListFromSet(set talondb.DocIDSet) *talondbpb.DocIDList {
	ids := collectDocIDs(set)
	return &talondbpb.DocIDList{DocIds: ids}
}

func collectDocIDs(set talondb.DocIDSet) []string {
	if set == nil {
		return nil
	}
	out := make([]string, 0, set.Len())
	set.ForEach(func(id string) bool {
		out = append(out, id)
		return true
	})
	return out
}

func mapError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, talondb.ErrInvalidEntityID) || errors.Is(err, talondb.ErrInvalidValue) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if errors.Is(err, talondb.ErrNotFound) {
		return status.Error(codes.NotFound, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}
