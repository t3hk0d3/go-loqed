package mqtt_test

import (
	"os/exec"
	"slices"
	"strings"
	"testing"
)

const (
	module   = "github.com/t3hk0d3/go-loqed/loqed-mqtt"
	pahoPath = "github.com/eclipse/paho.mqtt.golang"
)

// goList returns, for every non-test package matched by pattern, the
// packages listed by the given template field.
func goList(t *testing.T, field, pattern string) map[string][]string {
	t.Helper()
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go tool not available")
	}
	out, err := exec.Command(goBin, "list", "-f", `{{.ImportPath}} {{join .`+field+` " "}}`, pattern).Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	pkgs := map[string][]string{}
	for line := range strings.Lines(string(out)) {
		f := strings.Fields(line)
		if len(f) > 0 {
			pkgs[f[0]] = f[1:]
		}
	}
	return pkgs
}

// Only internal/mqtt talks to the MQTT library; internal/testutil is a
// test-only helper (its subscriber is a paho client) and is exempt.
func TestOnlyMQTTPackageImportsPaho(t *testing.T) {
	for pkg, imports := range goList(t, "Imports", module+"/...") {
		if pkg == module+"/internal/mqtt" || pkg == module+"/internal/testutil" {
			continue
		}
		if slices.Contains(imports, pahoPath) {
			t.Errorf("%s imports the MQTT library", pkg)
		}
	}
}

func TestMQTTPackageDoesNotDependOnHomeAssistant(t *testing.T) {
	deps := goList(t, "Deps", module+"/internal/mqtt")[module+"/internal/mqtt"]
	if len(deps) == 0 {
		t.Fatal("no dependencies listed for internal/mqtt")
	}
	if slices.Contains(deps, module+"/internal/mqtt/hass") {
		t.Error("internal/mqtt depends on internal/mqtt/hass")
	}
}
