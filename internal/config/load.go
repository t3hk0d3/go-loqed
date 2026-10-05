package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"reflect"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// EnvPrefix marks environment variables that override settings.
// Nesting uses "__": LOQED_MQTT__BASE_TOPIC sets mqtt.base_topic.
const EnvPrefix = "LOQED_"

// Sources lists where settings come from, lowest precedence first.
type Sources struct {
	OptionsFile string   // HA add-on /data/options.json; skipped if missing
	ConfigFile  string   // --config YAML; must exist when set
	Environ     []string // os.Environ()
}

func Load(src Sources) (Config, error) {
	cfg := Defaults()
	if src.OptionsFile != "" {
		if err := decodeFile(src.OptionsFile, &cfg, true, true); err != nil {
			return Config{}, err
		}
	}
	if src.ConfigFile != "" {
		if err := decodeFile(src.ConfigFile, &cfg, false, false); err != nil {
			return Config{}, err
		}
	}
	if err := applyEnv(&cfg, src.Environ); err != nil {
		return Config{}, err
	}
	cfg.fillDefaults()
	return cfg, nil
}

// decodeFile overlays a YAML file onto cfg. With isJSON the file is parsed
// as JSON first (yaml.v3 rejects valid JSON such as "\/" or surrogate-pair
// escapes, which Python's json.dumps emits for the add-on options), then
// applied through the same YAML decoder so unknown keys still fail.
func decodeFile(path string, cfg *Config, optional, isJSON bool) error {
	b, err := os.ReadFile(path) //nolint:gosec // G304: path is the operator's own config file
	if optional && errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	if isJSON && len(bytes.TrimSpace(b)) > 0 {
		var v any
		if err := json.Unmarshal(b, &v); err != nil {
			return fmt.Errorf("config: %s: %w", path, err)
		}
		if b, err = yaml.Marshal(v); err != nil {
			return fmt.Errorf("config: %s: %w", path, err)
		}
	}
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(cfg); err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("config: %s: %w", path, err)
	}
	return nil
}

func applyEnv(cfg *Config, environ []string) error {
	for _, kv := range environ {
		k, v, ok := strings.Cut(kv, "=")
		if !ok || !strings.HasPrefix(k, EnvPrefix) {
			continue
		}
		path := strings.Split(strings.ToLower(strings.TrimPrefix(k, EnvPrefix)), "__")
		if err := setPath(reflect.ValueOf(cfg).Elem(), path, v); err != nil {
			return fmt.Errorf("config: %s: %w", k, err)
		}
	}
	return nil
}

var unmarshalerType = reflect.TypeFor[yaml.Unmarshaler]()

func setPath(v reflect.Value, path []string, raw string) error {
	if len(path) == 0 {
		return setValue(v, raw)
	}
	if v.Kind() != reflect.Struct {
		return fmt.Errorf("%q is not a section", path[0])
	}
	t := v.Type()
	for i := range t.NumField() {
		name, _, _ := strings.Cut(t.Field(i).Tag.Get("yaml"), ",")
		if name == path[0] {
			return setPath(v.Field(i), path[1:], raw)
		}
	}
	return fmt.Errorf("unknown setting %q", path[0])
}

// setValue assigns raw text by field type. Strings are taken verbatim so
// passwords containing YAML syntax stay intact; structured values (maps,
// durations) are parsed as YAML/JSON.
func setValue(v reflect.Value, raw string) error {
	if v.Kind() == reflect.Map || reflect.PointerTo(v.Type()).Implements(unmarshalerType) {
		dec := yaml.NewDecoder(strings.NewReader(raw))
		dec.KnownFields(true)
		if err := dec.Decode(v.Addr().Interface()); err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		return nil
	}
	switch v.Kind() {
	case reflect.String:
		v.SetString(raw)
	case reflect.Int:
		n, err := strconv.Atoi(strings.TrimSpace(raw))
		if err != nil {
			return fmt.Errorf("expected an integer: %w", err)
		}
		v.SetInt(int64(n))
	case reflect.Bool:
		b, err := strconv.ParseBool(strings.TrimSpace(raw))
		if err != nil {
			return fmt.Errorf("expected true or false: %w", err)
		}
		v.SetBool(b)
	case reflect.Slice:
		if v.Type().Elem().Kind() != reflect.String {
			return fmt.Errorf("unsupported list type %s", v.Type())
		}
		var out []string
		for _, p := range strings.Split(raw, ",") {
			if p = strings.TrimSpace(p); p != "" {
				out = append(out, p)
			}
		}
		v.Set(reflect.ValueOf(out))
	default:
		return fmt.Errorf("unsupported setting type %s", v.Type())
	}
	return nil
}
