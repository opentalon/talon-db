package grpcserver

import (
	"context"
	"errors"

	"github.com/opentalon/talon-db/proto/talondbpb"
	"github.com/opentalon/talon-db/vectorindex"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// VectorInsert routes the wire metric enum to the vectorindex.Metric
// the in-memory index uses. The metric argument is only honoured on
// the first insert into a (entity, scope) pair; later inserts keep the
// scope's original metric.
func (s *Server) VectorInsert(ctx context.Context, req *talondbpb.VectorInsertRequest) (*emptypb.Empty, error) {
	_ = ctx
	if err := s.vectors.Insert(
		req.GetEntityId(),
		req.GetScope(),
		req.GetId(),
		req.GetVector(),
		metricFromProto(req.GetMetric()),
	); err != nil {
		return nil, mapVectorError(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) VectorSearch(ctx context.Context, req *talondbpb.VectorSearchRequest) (*talondbpb.VectorSearchResponse, error) {
	_ = ctx
	hits, err := s.vectors.Search(
		req.GetEntityId(),
		req.GetScope(),
		req.GetVector(),
		int(req.GetK()),
	)
	if err != nil {
		return nil, mapVectorError(err)
	}
	out := &talondbpb.VectorSearchResponse{Hits: make([]*talondbpb.VectorHit, 0, len(hits))}
	for _, h := range hits {
		out.Hits = append(out.Hits, &talondbpb.VectorHit{
			Id:       h.ID,
			Distance: h.Distance,
		})
	}
	return out, nil
}

func metricFromProto(m talondbpb.VectorMetric) vectorindex.Metric {
	switch m {
	case talondbpb.VectorMetric_VECTOR_METRIC_EUCLIDEAN:
		return vectorindex.Euclidean
	default:
		// UNSPECIFIED + COSINE both fall through to Cosine — Cosine is
		// the right default for embeddings the typical caller carries.
		return vectorindex.Cosine
	}
}

// mapVectorError converts the vectorindex sentinels to gRPC status
// codes. Dimension mismatch + empty-vector are caller bugs
// (InvalidArgument); ScopeNotFound is NotFound; anything else is
// surfaced as Internal so it shows up loudly in logs.
func mapVectorError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, vectorindex.ErrDimensionMismatch),
		errors.Is(err, vectorindex.ErrEmptyVector):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, vectorindex.ErrScopeNotFound):
		return status.Error(codes.NotFound, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
