package gateway

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// wallOnlyCalls lists where the module may use time operations that drop
// Go's monotonic clock reading (UTC, Local, In, Round, Truncate, time.Unix*,
// time.Parse*). Method names are matched without type information, so a
// Duration.Round would count too: list it here with a comment.
// Such values are for display and persistence only: a deadline, timeout or
// window derived from them would follow wall-clock steps (NTP corrections).
// A new call must be added here deliberately, after checking it never feeds
// timing.
var wallOnlyCalls = map[string]int{
	"internal/model/model.go":        4, // command_status received_at/updated_at (UTC + Truncate)
	"internal/model/webhooks.go":     2, // webhook list fetched_at
	"internal/auth/expiry.go":        2, // token expiry from the JWT (time.Unix + UTC)
	"internal/gateway/supervisor.go": 2, // state document last_event_at
	"internal/gateway/refresher.go":  1, // persisted cache fetched_at
}

func TestWallClockOnlyTimeCallsStayAtSerializationSites(t *testing.T) {
	root := filepath.Join("..", "..")
	got := map[string]int{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return err
		}
		f, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			switch sel.Sel.Name {
			case "UTC", "Local", "In", "Round", "Truncate":
				got[filepath.ToSlash(rel)]++
			case "Unix", "UnixMilli", "UnixMicro", "Parse", "ParseInLocation":
				if id, ok := sel.X.(*ast.Ident); ok && id.Name == "time" {
					got[filepath.ToSlash(rel)]++
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for file, n := range got {
		if n != wallOnlyCalls[file] {
			t.Errorf("%s: %d wall-clock-only time calls, allowed %d; deadlines and windows must use values from the injected clock, never UTC/Truncate/parsed times", file, n, wallOnlyCalls[file])
		}
	}
	for file, n := range wallOnlyCalls {
		if got[file] == 0 && n > 0 {
			t.Errorf("%s: allow-list entry has no calls any more; remove it", file)
		}
	}
}
