// Package db opens the Oracle connection pool and applies embedded schema migrations.
package db

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/sijms/go-ora/v2/network"

	_ "github.com/sijms/go-ora/v2" // registers the "oracle" driver
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// Open connects to Oracle using a go-ora URL such as
// oracle://app:app@localhost:1521/FREEPDB1 and waits until the database accepts connections.
func Open(ctx context.Context, url string, waitFor time.Duration) (*sql.DB, error) {
	db, err := sql.Open("oracle", url)
	if err != nil {
		return nil, fmt.Errorf("open oracle: %w", err)
	}
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(30 * time.Minute)

	deadline := time.Now().Add(waitFor)
	for {
		pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err = db.PingContext(pingCtx)
		cancel()
		if err == nil {
			return db, nil
		}
		if time.Now().After(deadline) {
			db.Close()
			return nil, fmt.Errorf("oracle not reachable after %s: %w", waitFor, err)
		}
		slog.Info("waiting for oracle", "err", err)
		select {
		case <-ctx.Done():
			db.Close()
			return nil, ctx.Err()
		case <-time.After(3 * time.Second):
		}
	}
}

// Migrate applies every embedded migration that is not yet recorded in schema_migrations.
// Each file is applied statement by statement because Oracle executes one statement per call.
func Migrate(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `CREATE TABLE schema_migrations (
		version    VARCHAR2(100) PRIMARY KEY,
		applied_at TIMESTAMP DEFAULT SYS_EXTRACT_UTC(SYSTIMESTAMP) NOT NULL)`)
	if err != nil && OracleCode(err) != 955 { // ORA-00955: name is already used by an existing object
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	files, err := fs.Glob(migrationFS, "migrations/*.sql")
	if err != nil {
		return err
	}
	sort.Strings(files)

	for _, file := range files {
		version := strings.TrimSuffix(strings.TrimPrefix(file, "migrations/"), ".sql")
		var n int
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM schema_migrations WHERE version = :1`, version).Scan(&n); err != nil {
			return fmt.Errorf("check migration %s: %w", version, err)
		}
		if n > 0 {
			continue
		}
		content, err := migrationFS.ReadFile(file)
		if err != nil {
			return err
		}
		// DDL auto-commits in Oracle, so a failed migration can leave partial state;
		// statements are kept idempotent-friendly and the failure is reported loudly.
		for i, stmt := range SplitStatements(string(content)) {
			if _, err := db.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("migration %s statement %d: %w\n%s", version, i+1, err, stmt)
			}
		}
		if _, err := db.ExecContext(ctx, `INSERT INTO schema_migrations (version) VALUES (:1)`, version); err != nil {
			return fmt.Errorf("record migration %s: %w", version, err)
		}
		slog.Info("applied migration", "version", version)
	}
	return nil
}

// SplitStatements splits a SQL script on semicolons that end a line, dropping
// comment-only lines. It does not handle PL/SQL blocks, which the schema avoids.
func SplitStatements(script string) []string {
	var stmts []string
	var cur strings.Builder
	for _, line := range strings.Split(strings.ReplaceAll(script, "\r\n", "\n"), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "--") {
			continue
		}
		if strings.HasSuffix(trimmed, ";") {
			cur.WriteString(strings.TrimSuffix(line, ";"))
			if s := strings.TrimSpace(cur.String()); s != "" {
				stmts = append(stmts, s)
			}
			cur.Reset()
			continue
		}
		cur.WriteString(line)
		cur.WriteString("\n")
	}
	if s := strings.TrimSpace(cur.String()); s != "" {
		stmts = append(stmts, s)
	}
	return stmts
}

// OracleCode returns the ORA- error number wrapped in err, or 0.
func OracleCode(err error) int {
	var oe *network.OracleError
	if errors.As(err, &oe) {
		return oe.ErrCode
	}
	return 0
}

// IsUniqueViolation reports whether err is ORA-00001 (unique constraint violated).
func IsUniqueViolation(err error) bool { return OracleCode(err) == 1 }
