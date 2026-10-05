package db

import (
	"strings"
	"testing"
)

func TestSplitStatements(t *testing.T) {
	script := "-- header comment\r\nCREATE TABLE a (\n  id NUMBER -- inline is kept\n);\n\nINSERT INTO a VALUES (1);\nSELECT 1 FROM dual"
	got := SplitStatements(script)
	if len(got) != 3 {
		t.Fatalf("got %d statements: %q", len(got), got)
	}
	if strings.HasSuffix(got[0], ";") || !strings.HasPrefix(got[0], "CREATE TABLE a") {
		t.Errorf("stmt 1 = %q", got[0])
	}
	if got[1] != "INSERT INTO a VALUES (1)" || got[2] != "SELECT 1 FROM dual" {
		t.Errorf("stmts = %q", got[1:])
	}
}

func TestEmbeddedMigrationsSplitCleanly(t *testing.T) {
	files, _ := migrationFS.ReadDir("migrations")
	if len(files) == 0 {
		t.Fatal("no migrations embedded")
	}
	for _, f := range files {
		b, _ := migrationFS.ReadFile("migrations/" + f.Name())
		for _, stmt := range SplitStatements(string(b)) {
			if strings.Contains(stmt, ";") {
				t.Errorf("%s: statement still contains ';': %.80q", f.Name(), stmt)
			}
		}
	}
}
