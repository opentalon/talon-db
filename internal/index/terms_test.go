package index

import (
	"reflect"
	"sort"
	"testing"
)

func TestExtractFlatScalars(t *testing.T) {
	t.Parallel()
	got, err := Extract([]byte(`{"status":"active","km":45000,"open":true}`))
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	wantTerms := []string{
		"45000",
		"active",
		"km:45000",
		"open:true",
		"status:active",
		"true",
	}
	sort.Strings(wantTerms)
	if !reflect.DeepEqual(got.Terms, wantTerms) {
		t.Fatalf("Terms\n got: %v\nwant: %v", got.Terms, wantTerms)
	}
	if len(got.Numerics) != 1 || got.Numerics[0].Path != "km" || got.Numerics[0].Value != 45000 {
		t.Fatalf("Numerics: %#v", got.Numerics)
	}
}

func TestExtractNestedComposite(t *testing.T) {
	t.Parallel()
	// #27 spec example: coverage_feature("tierart").slug_values contains "hund"
	got, err := Extract([]byte(`{"coverage_feature":{"tierart":["hund","katze"]}}`))
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	want := []string{"hund", "katze", "tierart:hund", "tierart:katze"}
	sort.Strings(want)
	if !reflect.DeepEqual(got.Terms, want) {
		t.Fatalf("Terms\n got: %v\nwant: %v", got.Terms, want)
	}
}

func TestExtractArrayOfObjects(t *testing.T) {
	t.Parallel()
	got, err := Extract([]byte(`{"coverage":[{"name":"hund","km":10},{"name":"katze","km":20}]}`))
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	want := map[string]bool{
		"hund":       true,
		"katze":      true,
		"10":         true,
		"20":         true,
		"name:hund":  true,
		"name:katze": true,
		"km:10":      true,
		"km:20":      true,
	}
	if len(got.Terms) != len(want) {
		t.Fatalf("term count: got %d, want %d (%v)", len(got.Terms), len(want), got.Terms)
	}
	for _, term := range got.Terms {
		if !want[term] {
			t.Errorf("unexpected term %q", term)
		}
	}
	if len(got.Numerics) != 2 {
		t.Fatalf("Numerics count: got %d, want 2 (%v)", len(got.Numerics), got.Numerics)
	}
}

func TestExtractRejectsNonObjectTopLevel(t *testing.T) {
	t.Parallel()
	cases := []string{`[1,2,3]`, `"hello"`, `42`, `null`, `true`}
	for _, c := range cases {
		t.Run(c, func(t *testing.T) {
			_, err := Extract([]byte(c))
			if err == nil {
				t.Fatalf("expected error for input %q", c)
			}
		})
	}
}

func TestExtractInvalidJSON(t *testing.T) {
	t.Parallel()
	_, err := Extract([]byte(`{not valid`))
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestExtractEmpty(t *testing.T) {
	t.Parallel()
	got, err := Extract(nil)
	if err != nil {
		t.Fatalf("Extract(nil): %v", err)
	}
	if len(got.Terms) != 0 || len(got.Numerics) != 0 {
		t.Fatalf("nil input should yield empty result, got %#v", got)
	}
	got, err = Extract([]byte(`{}`))
	if err != nil {
		t.Fatalf("Extract({}): %v", err)
	}
	if len(got.Terms) != 0 {
		t.Fatalf("empty object should yield no terms, got %v", got.Terms)
	}
}

func TestExtractSkipsNullAndNaN(t *testing.T) {
	t.Parallel()
	got, err := Extract([]byte(`{"a":null,"b":42}`))
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	for _, term := range got.Terms {
		if term == "null" || term == "a:null" {
			t.Fatalf("null leaked into terms: %v", got.Terms)
		}
	}
}

func TestExtractDeduplicatesTerms(t *testing.T) {
	t.Parallel()
	got, err := Extract([]byte(`{"a":"x","b":"x"}`))
	if err != nil {
		t.Fatalf("Extract: %v", err)
	}
	count := 0
	for _, term := range got.Terms {
		if term == "x" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("bare term %q appeared %d times, want 1: %v", "x", count, got.Terms)
	}
}
