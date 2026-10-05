package loqed_test

import (
	"encoding/json"
	"testing"

	loqed "github.com/t3hk0d3/go-loqed"
)

func TestIntDecodesNumbersStringsAndNull(t *testing.T) {
	// Every case starts from 99 so null (no-op) and "" (zero) are observable.
	cases := map[string]loqed.Int{
		`78`: 78, `"78"`: 78, `" 78 "`: 78, `78.0`: 78, `-1`: -1, `"-1"`: -1, `null`: 99, `""`: 0,
	}
	for in, want := range cases {
		got := struct {
			V loqed.Int `json:"v"`
		}{V: 99}
		if err := json.Unmarshal([]byte(`{"v":`+in+`}`), &got); err != nil {
			t.Fatalf("%s: %v", in, err)
		}
		if got.V != want {
			t.Errorf("%s: got %d want %d", in, got.V, want)
		}
	}
}

func TestIntPointerNullIsNil(t *testing.T) {
	var got struct {
		V *loqed.Int `json:"v"`
	}
	if err := json.Unmarshal([]byte(`{"v":null}`), &got); err != nil {
		t.Fatal(err)
	}
	if got.V != nil {
		t.Fatalf("expected nil, got %v", *got.V)
	}
}

func TestIntRejectsGarbage(t *testing.T) {
	var got struct {
		V loqed.Int `json:"v"`
	}
	if err := json.Unmarshal([]byte(`{"v":"abc"}`), &got); err == nil {
		t.Fatal("expected error")
	}
	if err := json.Unmarshal([]byte(`{"v":{}}`), &got); err == nil {
		t.Fatal("expected error for object")
	}
	for _, v := range []string{`"NaN"`, `"Inf"`, `"-Inf"`, `1e30`, `"1e30"`} {
		if err := json.Unmarshal([]byte(`{"v":`+v+`}`), &got); err == nil {
			t.Errorf("%s: expected error", v)
		}
	}
}

func TestFloatBoolString(t *testing.T) {
	var got struct {
		F loqed.Float  `json:"f"`
		B loqed.Bool   `json:"b"`
		C loqed.Bool   `json:"c"`
		S loqed.String `json:"s"`
		N loqed.String `json:"n"`
	}
	in := `{"f":"10.37","b":1,"c":"false","s":"abc","n":42}`
	if err := json.Unmarshal([]byte(in), &got); err != nil {
		t.Fatal(err)
	}
	if got.F != 10.37 || got.B != true || got.C != false || got.S != "abc" || got.N != "42" {
		t.Fatalf("unexpected decode: %+v", got)
	}
}
