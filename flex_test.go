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

func TestKeyIDOnlyRealKeys(t *testing.T) {
	// 255 and "" are what the bridge and the cloud send when no key acted.
	intp := func(v int) *int { return &v }
	cases := map[string]*int{
		`0`: intp(0), `"0"`: intp(0), `7`: intp(7), `"7"`: intp(7), `254`: intp(254),
		`255`: nil, `"255"`: nil, `""`: nil, `null`: nil, `-1`: nil, `999`: nil, `"abc"`: nil, `1.5`: nil,
	}
	for in, want := range cases {
		var got struct {
			K *loqed.KeyID `json:"k"`
		}
		if err := json.Unmarshal([]byte(`{"k":`+in+`}`), &got); err != nil {
			t.Fatalf("%s: %v", in, err)
		}
		p := got.K.Ptr()
		if (p == nil) != (want == nil) || (p != nil && *p != *want) {
			t.Errorf("%s: got %v want %v", in, p, want)
		}
	}
	var absent struct {
		K *loqed.KeyID `json:"k"`
	}
	if err := json.Unmarshal([]byte(`{}`), &absent); err != nil || absent.K.Ptr() != nil {
		t.Fatalf("absent: %v %v", absent.K.Ptr(), err)
	}
}

func TestKeyIDRejectsObjects(t *testing.T) {
	var got struct {
		K loqed.KeyID `json:"k"`
	}
	for _, in := range []string{`{}`, `[]`} {
		if err := json.Unmarshal([]byte(`{"k":`+in+`}`), &got); err == nil {
			t.Errorf("%s: expected error", in)
		}
	}
}
