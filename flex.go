package loqed

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Int decodes a JSON number or numeric string; "" decodes as 0 and null
// leaves the value unchanged. Use *Int to tell null/absent apart from zero.
type Int int64

func (i *Int) UnmarshalJSON(b []byte) error {
	s, null, err := scalarText(b)
	if err != nil || null {
		return err
	}
	if s == "" {
		*i = 0
		return nil
	}
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		*i = Int(n)
		return nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return fmt.Errorf("loqed: invalid integer %q", s)
	}
	if math.IsNaN(f) || math.IsInf(f, 0) || f < -(1<<63) || f >= 1<<63 {
		return fmt.Errorf("loqed: integer %q out of range", s)
	}
	*i = Int(f)
	return nil
}

// Float decodes a JSON number or numeric string; "" decodes as 0 and null
// leaves the value unchanged.
type Float float64

func (f *Float) UnmarshalJSON(b []byte) error {
	s, null, err := scalarText(b)
	if err != nil || null {
		return err
	}
	if s == "" {
		*f = 0
		return nil
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return fmt.Errorf("loqed: invalid number %q", s)
	}
	*f = Float(v)
	return nil
}

// Bool decodes true/false, 1/0 and their string forms.
type Bool bool

func (v *Bool) UnmarshalJSON(b []byte) error {
	s, null, err := scalarText(b)
	if err != nil || null {
		return err
	}
	switch strings.ToLower(s) {
	case "1", "true":
		*v = true
	case "0", "false", "":
		*v = false
	default:
		return fmt.Errorf("loqed: invalid boolean %q", s)
	}
	return nil
}

// String decodes a JSON string, number or boolean as text.
type String string

func (v *String) UnmarshalJSON(b []byte) error {
	s, null, err := scalarText(b)
	if err != nil || null {
		return err
	}
	*v = String(s)
	return nil
}

// scalarText returns the text of a JSON scalar; null reports null=true.
func scalarText(b []byte) (text string, null bool, err error) {
	b = bytes.TrimSpace(b)
	switch {
	case len(b) == 0 || string(b) == "null":
		return "", true, nil
	case b[0] == '"':
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return "", false, err
		}
		return strings.TrimSpace(s), false, nil
	case b[0] == '{' || b[0] == '[':
		return "", false, fmt.Errorf("loqed: expected a scalar, got %.20s", b)
	default:
		return string(b), false, nil
	}
}
