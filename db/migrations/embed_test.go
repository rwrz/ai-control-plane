package migrations

import (
	"io/fs"
	"strings"
	"testing"
)

// The whole point of embedding is that Latest() equals what golang-migrate will
// have written to schema_migrations. A drift between the two turns the boot
// check into a permanent false alarm, which is worse than no check at all —
// people learn to ignore it.
func TestLatestMatchesHighestUpMigration(t *testing.T) {
	entries, err := fs.ReadDir(Files, ".")
	if err != nil {
		t.Fatal(err)
	}

	ups := 0
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".up.sql") {
			ups++
		}
	}
	if ups == 0 {
		t.Fatal("no .up.sql files embedded; the go:embed directive is not matching")
	}

	// Migrations are sequential from 001, so the count is the highest version.
	// This is the invariant `migrate create -seq` maintains, and a gap would
	// mean a migration was deleted rather than reverted.
	if got := Latest(); got != ups {
		t.Errorf("Latest() = %d, but %d up-migrations are embedded; "+
			"either a version was skipped or a file was removed", got, ups)
	}
}

// Every up must have a down. A missing one is only discovered when a rollback
// is needed, which is the worst possible moment to discover it.
func TestEveryUpMigrationHasADown(t *testing.T) {
	entries, err := fs.ReadDir(Files, ".")
	if err != nil {
		t.Fatal(err)
	}

	downs := map[string]bool{}
	for _, e := range entries {
		if name, ok := strings.CutSuffix(e.Name(), ".down.sql"); ok {
			downs[name] = true
		}
	}
	for _, e := range entries {
		name, ok := strings.CutSuffix(e.Name(), ".up.sql")
		if !ok {
			continue
		}
		if !downs[name] {
			t.Errorf("%s.up.sql has no matching .down.sql", name)
		}
	}
}
