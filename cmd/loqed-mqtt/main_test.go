package main

import "testing"

func TestHealthURL(t *testing.T) {
	cases := map[string]string{
		":8099":            "http://127.0.0.1:8099/healthz",
		"0.0.0.0:8099":     "http://127.0.0.1:8099/healthz",
		"[::]:8099":        "http://127.0.0.1:8099/healthz",
		"192.0.2.5:9000":   "http://192.0.2.5:9000/healthz",
		"[2001:db8::1]:80": "http://[2001:db8::1]:80/healthz",
	}
	for in, want := range cases {
		if got, err := healthURL(in); err != nil || got != want {
			t.Errorf("%s: %q %v", in, got, err)
		}
	}
}
