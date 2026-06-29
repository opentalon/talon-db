package bboltstore

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/golang/snappy"
	bolt "go.etcd.io/bbolt"
)

// QueryClause is the union of every clause type the server-side
// composer accepts. Exactly one of the embedded pointers must be set.
type QueryClause struct {
	Pattern   *QueryPattern
	Predicate *QueryPredicate
	Or        *QueryOr
	Not       *QueryNot
	FullText  *QueryFullText
}

// QueryTerm carries either a variable reference (Var non-empty) or a
// literal value (Literal non-nil). Empty / nil both → wildcard.
type QueryTerm struct {
	Var     string
	Literal any
}

// IsLiteral reports whether the term carries a literal value.
func (t QueryTerm) IsLiteral() bool { return t.Literal != nil }

// IsVar reports whether the term carries a variable reference.
func (t QueryTerm) IsVar() bool { return t.Var != "" }

// QueryPattern matches an entity-attribute-value triple. Variables in
// Entity / Value either bind or constrain depending on whether they
// were already bound by a sibling clause.
type QueryPattern struct {
	Entity    QueryTerm
	Attribute string
	Value     QueryTerm
}

// QueryPredicate is a post-binding comparison. Op is one of the strings
// documented in the proto comment.
type QueryPredicate struct {
	Op    string
	Left  QueryTerm
	Right QueryTerm
}

// QueryOr is the disjunction of N branches. A row matches if any
// branch matches; the first successful branch's bindings flow back to
// the parent for variables that were previously unbound.
type QueryOr struct {
	Branches [][]QueryClause
}

// QueryNot inverts the inner clause list — the parent row only
// matches when none of these clauses match.
type QueryNot struct {
	Body []QueryClause
}

// QueryFullText scans an entity's string-valued attributes for the
// query substring (case-insensitive). When Attribute is set, only
// that attribute is searched.
type QueryFullText struct {
	Entity    QueryTerm
	Query     string
	Attribute string
}

// QueryRequest is the parsed input to Store.Query.
type QueryRequest struct {
	EntityID string
	Find     []string
	Where    []QueryClause
}

// QueryRow is one result row: one value per Find column, in column
// order. Unbound variables appear as nil.
type QueryRow []any

// Query runs the structured composer: anchor narrowing via the
// inverted index, then per-doc Go-side clause evaluation. Returns one
// QueryRow per matched candidate, projected to req.Find.
//
// The algorithm mirrors the talon-language adapter so server-side and
// client-side composition behave identically:
//
//  1. Collect top-level Patterns whose attribute AND value are both
//     literal — these become Lookup anchors.
//  2. AND the per-anchor bitmaps into a candidate set.
//  3. For each candidate, decode the doc and evaluate every Where
//     clause against the doc's attributes + accumulated bindings.
//  4. Project bindings to Find.
func (s *Store) Query(ctx context.Context, req QueryRequest) ([]QueryRow, error) {
	if err := validateEntityID(req.EntityID); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	anchors := collectQueryAnchors(req.Where)
	if len(anchors) == 0 {
		return nil, fmt.Errorf("bboltstore: query has no anchor pattern (literal attr + literal value)")
	}
	candidates, err := s.gatherQueryCandidates(ctx, req.EntityID, anchors)
	if err != nil {
		return nil, err
	}
	if len(candidates) == 0 {
		return nil, nil
	}

	rows := make([]QueryRow, 0, len(candidates))
	for _, docID := range candidates {
		var doc map[string]any
		if err := s.db.View(func(tx *bolt.Tx) error {
			bucket := tx.Bucket([]byte(docsBucketPrefix + req.EntityID))
			if bucket == nil {
				return nil
			}
			raw := bucket.Get([]byte(docID))
			if raw == nil {
				return nil
			}
			decoded, decErr := snappyDecodeBytes(raw)
			if decErr != nil {
				return decErr
			}
			return json.Unmarshal(decoded, &doc)
		}); err != nil {
			// JSON decode errors fall through as "no fields" — the
			// store may hold opaque non-JSON blobs.
			doc = map[string]any{}
		}
		if doc == nil {
			doc = map[string]any{}
		}
		bindings := map[string]any{"?e": parseQueryRecordID(docID)}
		if !matchAllQuery(req.Where, doc, bindings) {
			continue
		}
		row := make(QueryRow, len(req.Find))
		for i, name := range req.Find {
			row[i] = bindings[name]
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// collectQueryAnchors returns every top-level Pattern with a literal
// attribute AND literal value — these are the index-anchored
// narrowing clauses. Patterns inside Or / Not aren't considered
// (their semantics are different).
func collectQueryAnchors(clauses []QueryClause) []*QueryPattern {
	var out []*QueryPattern
	for _, c := range clauses {
		if c.Pattern == nil {
			continue
		}
		p := c.Pattern
		if p.Attribute != "" && p.Value.IsLiteral() {
			out = append(out, p)
		}
	}
	return out
}

// gatherQueryCandidates issues one Lookup per anchor and intersects
// the returned docID lists.
func (s *Store) gatherQueryCandidates(ctx context.Context, entityID string, anchors []*QueryPattern) ([]string, error) {
	var candidates []string
	for i, p := range anchors {
		term := composeQueryTerm(p.Attribute, p.Value.Literal)
		set, err := s.Lookup(ctx, entityID, term)
		if err != nil {
			return nil, err
		}
		got := docIDSetToSorted(set)
		if i == 0 {
			candidates = got
		} else {
			candidates = intersectSortedQueryIDs(candidates, got)
		}
		if len(candidates) == 0 {
			return nil, nil
		}
	}
	return candidates, nil
}

// matchAllQuery evaluates every clause against the in-memory doc +
// accumulated bindings. All clauses must match.
func matchAllQuery(clauses []QueryClause, attrs map[string]any, bindings map[string]any) bool {
	for _, c := range clauses {
		if !matchOneQuery(c, attrs, bindings) {
			return false
		}
	}
	return true
}

func matchOneQuery(c QueryClause, attrs map[string]any, bindings map[string]any) bool {
	switch {
	case c.Pattern != nil:
		return matchQueryPattern(c.Pattern, attrs, bindings)
	case c.Predicate != nil:
		return matchQueryPredicate(c.Predicate, bindings)
	case c.Or != nil:
		return matchQueryOr(c.Or, attrs, bindings)
	case c.Not != nil:
		return matchQueryNot(c.Not, attrs, bindings)
	case c.FullText != nil:
		return matchQueryFullText(c.FullText, attrs)
	}
	return false
}

func matchQueryPattern(p *QueryPattern, attrs map[string]any, bindings map[string]any) bool {
	if p.Attribute == "" {
		return false
	}
	docVal, present := attrs[p.Attribute]
	if !present {
		return false
	}
	switch {
	case p.Value.IsLiteral():
		return equalQueryValues(docVal, p.Value.Literal)
	case p.Value.IsVar():
		if existing, had := bindings[p.Value.Var]; had {
			return equalQueryValues(existing, docVal)
		}
		bindings[p.Value.Var] = docVal
		return true
	}
	return true
}

func matchQueryPredicate(p *QueryPredicate, bindings map[string]any) bool {
	left := resolveQueryTerm(p.Left, bindings)
	right := resolveQueryTerm(p.Right, bindings)
	return evalQueryPredicate(p.Op, left, right)
}

func matchQueryOr(o *QueryOr, attrs map[string]any, bindings map[string]any) bool {
	for _, branch := range o.Branches {
		scratch := cloneQueryBindings(bindings)
		if matchAllQuery(branch, attrs, scratch) {
			for k, v := range scratch {
				if _, had := bindings[k]; !had {
					bindings[k] = v
				}
			}
			return true
		}
	}
	return false
}

func matchQueryNot(n *QueryNot, attrs map[string]any, bindings map[string]any) bool {
	scratch := cloneQueryBindings(bindings)
	return !matchAllQuery(n.Body, attrs, scratch)
}

func matchQueryFullText(f *QueryFullText, attrs map[string]any) bool {
	if f.Query == "" {
		return false
	}
	needle := strings.ToLower(f.Query)
	if f.Attribute != "" {
		v, ok := attrs[f.Attribute]
		if !ok {
			return false
		}
		s, ok := v.(string)
		return ok && strings.Contains(strings.ToLower(s), needle)
	}
	for _, v := range attrs {
		s, ok := v.(string)
		if !ok {
			continue
		}
		if strings.Contains(strings.ToLower(s), needle) {
			return true
		}
	}
	return false
}

// ---------- helpers ----------

func composeQueryTerm(attribute string, value any) string {
	return attribute + ":" + stringifyQueryValue(value)
}

func stringifyQueryValue(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case bool:
		if x {
			return "true"
		}
		return "false"
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64)
	case int:
		return strconv.Itoa(x)
	case int64:
		return strconv.FormatInt(x, 10)
	case float32:
		return strconv.FormatFloat(float64(x), 'g', -1, 64)
	}
	return fmt.Sprint(v)
}

func docIDSetToSorted(set interface {
	ForEach(func(string) bool)
}) []string {
	var ids []string
	set.ForEach(func(id string) bool {
		ids = append(ids, id)
		return true
	})
	sort.Strings(ids)
	return ids
}

func intersectSortedQueryIDs(a, b []string) []string {
	out := make([]string, 0, min(len(a), len(b)))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] < b[j]:
			i++
		case a[i] > b[j]:
			j++
		default:
			out = append(out, a[i])
			i++
			j++
		}
	}
	return out
}

func parseQueryRecordID(docID string) any {
	if n, err := strconv.ParseInt(docID, 10, 64); err == nil {
		return float64(n)
	}
	return docID
}

func resolveQueryTerm(t QueryTerm, bindings map[string]any) any {
	if t.IsVar() {
		return bindings[t.Var]
	}
	return t.Literal
}

func evalQueryPredicate(op string, left, right any) bool {
	switch op {
	case "==":
		return equalQueryValues(left, right)
	case "!=":
		return !equalQueryValues(left, right)
	case "<", "<=", ">", ">=":
		l, lok := queryNumeric(left)
		r, rok := queryNumeric(right)
		if !lok || !rok {
			return false
		}
		switch op {
		case "<":
			return l < r
		case "<=":
			return l <= r
		case ">":
			return l > r
		case ">=":
			return l >= r
		}
	case "starts_with":
		ls, lok := left.(string)
		rs, rok := right.(string)
		return lok && rok && strings.HasPrefix(ls, rs)
	case "ends_with":
		ls, lok := left.(string)
		rs, rok := right.(string)
		return lok && rok && strings.HasSuffix(ls, rs)
	case "contains":
		ls, lok := left.(string)
		rs, rok := right.(string)
		return lok && rok && strings.Contains(ls, rs)
	}
	return false
}

func equalQueryValues(a, b any) bool {
	if a == b {
		return true
	}
	if l, lok := queryNumeric(a); lok {
		if r, rok := queryNumeric(b); rok {
			return l == r
		}
	}
	return false
}

func queryNumeric(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		if math.IsNaN(x) {
			return 0, false
		}
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case int32:
		return float64(x), true
	}
	return 0, false
}

func cloneQueryBindings(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func snappyDecodeBytes(raw []byte) ([]byte, error) {
	return snappy.Decode(nil, raw)
}
