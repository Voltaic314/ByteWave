package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Voltaic314/ByteWave/code/db/tables"

	_ "github.com/marcboeker/go-duckdb"
)

type SeedConfig struct {
	DBPath        string  `json:"db_path"`
	SrcPath       string  `json:"src_path"`
	DstPath       string  `json:"dst_path"`
	Overwrite     bool    `json:"overwrite"`
	SchemaDir     string  `json:"schema_dir"`
	Seed          int64   `json:"seed"`
	NumFiles      int     `json:"num_files"`
	MaxDepth      int     `json:"max_depth"`
	Fanout        int     `json:"fanout"`
	IncludeDstPct float64 `json:"include_dst_pct"`
	DstOnlyPct    float64 `json:"dst_only_pct"`
	LastModified  string  `json:"last_modified"`
}

func main() {
	cfgPath := "tests/traversal_tests/config.json"
	if env := os.Getenv("BW_SEED_CONFIG"); env != "" {
		cfgPath = env
	}
	cfg, err := loadConfig(cfgPath)
	if err != nil {
		fatalf("load config: %v", err)
	}

	// defaults
	if strings.TrimSpace(cfg.DBPath) == "" {
		cfg.DBPath = "tests/traversal_tests/test_traversal.db"
	}
	if strings.TrimSpace(cfg.SrcPath) == "" {
		cfg.SrcPath = "tests/traversal_tests/starting_src_folder"
	}
	if strings.TrimSpace(cfg.DstPath) == "" {
		cfg.DstPath = "tests/traversal_tests/starting_dst_folder"
	}
	if cfg.NumFiles <= 0 {
		cfg.NumFiles = 1000
	}
	if cfg.MaxDepth <= 0 {
		cfg.MaxDepth = 4
	}
	if cfg.Fanout <= 0 {
		cfg.Fanout = 5
	}
	if cfg.IncludeDstPct < 0 || cfg.IncludeDstPct > 1 {
		cfg.IncludeDstPct = 0.7
	}
	if cfg.DstOnlyPct < 0 || cfg.DstOnlyPct > 1 {
		cfg.DstOnlyPct = 0.05
	}
	if strings.TrimSpace(cfg.LastModified) == "" {
		cfg.LastModified = "2025-01-01T00:00:00Z"
	}

	absDB, _ := filepath.Abs(cfg.DBPath)
	absSrc, _ := filepath.Abs(cfg.SrcPath)
	absDst, _ := filepath.Abs(cfg.DstPath)

	fmt.Printf("📁 DB:  %s\n", absDB)
	fmt.Printf("📁 SRC: %s\n", absSrc)
	fmt.Printf("📁 DST: %s\n", absDst)

	if cfg.Overwrite {
		// Remove DB file and any WAL files
		if err := os.RemoveAll(absDB); err != nil {
			fatalf("remove existing DB: %v", err)
		}
		if err := os.RemoveAll(absDB + ".wal"); err != nil && !os.IsNotExist(err) {
			fatalf("remove existing DB WAL: %v", err)
		}
	}

	// Clean & (re)create roots on disk so DB and FS are in sync
	if err := os.RemoveAll(absSrc); err != nil {
		fatalf("remove src root: %v", err)
	}
	if err := os.RemoveAll(absDst); err != nil {
		fatalf("remove dst root: %v", err)
	}
	if err := os.MkdirAll(absSrc, 0o755); err != nil {
		fatalf("mkdir src: %v", err)
	}
	if err := os.MkdirAll(absDst, 0o755); err != nil {
		fatalf("mkdir dst: %v", err)
	}

	db, err := sql.Open("duckdb", absDB)
	if err != nil {
		fatalf("open duckdb: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := applyTablesFromLive(ctx, tx); err != nil {
		fatalf("apply live tables: %v", err)
	}

	lastMod, err := time.Parse(time.RFC3339, cfg.LastModified)
	if err != nil {
		fatalf("parse last_modified: %v", err)
	}

	// Insert root nodes ONLY - let traversal discover everything else
	if err := insertSourceRoot(ctx, tx, absSrc, lastMod); err != nil {
		fatalf("insert src root: %v", err)
	}
	if err := insertDestRoot(ctx, tx, absDst, lastMod); err != nil {
		fatalf("insert dst root: %v", err)
	}

	// Create expected tables for comparison after traversal
	if err := createExpectedTables(ctx, tx); err != nil {
		fatalf("create expected tables: %v", err)
	}

	// Prepared statements for expected results (what traversal should discover)
	srcExpectedStmt, err := tx.PrepareContext(ctx, `
		INSERT INTO source_nodes_expected (
			path, name, identifier, parent_id, type, level, size,
			last_modified, traversal_status, upload_status,
			traversal_attempts, upload_attempts, error_ids
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'successful', 'pending', 1, 0, NULL)
	`)
	if err != nil {
		fatalf("prepare src expected insert: %v", err)
	}
	defer srcExpectedStmt.Close()

	dstExpectedStmt, err := tx.PrepareContext(ctx, `
		INSERT INTO destination_nodes_expected (
			path, name, identifier, parent_id, type, level, size,
			last_modified, traversal_status, traversal_attempts, error_ids
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'successful', 1, NULL)
	`)
	if err != nil {
		fatalf("prepare dst expected insert: %v", err)
	}
	defer dstExpectedStmt.Close()

	// -------- FILESYSTEM & EXPECTED RESULTS GENERATOR --------

	// RNG
	seed := cfg.Seed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	rng := rand.New(rand.NewSource(seed))
	fmt.Printf("🎲 Seed: %d\n", seed)

	// helpers
	toDBPath := func(rel string) string {
		rel = filepath.ToSlash(rel)
		if rel == "" || rel == "." {
			return "/"
		}
		return "/" + strings.TrimPrefix(rel, "/")
	}
	parentDBPath := func(dbPath string) string {
		if dbPath == "/" {
			return ""
		}
		dir := filepath.ToSlash(filepath.Dir(strings.TrimPrefix(dbPath, "/")))
		if dir == "." || dir == "" {
			return "/"
		}
		return "/" + dir
	}

	createdSrcDirs := map[string]bool{"/": true}
	createdDstDirs := map[string]bool{"/": true}

	ensureSrcDirExpected := func(dbPath, absPath string, level int) error {
		if createdSrcDirs[dbPath] {
			return nil
		}
		_, err := srcExpectedStmt.ExecContext(ctx,
			dbPath,
			filepath.Base(absPath),
			absPath,
			parentDBPath(dbPath),
			"folder",
			level,
			0,
			lastMod,
		)
		if err == nil {
			createdSrcDirs[dbPath] = true
		}
		return err
	}
	ensureDstDirExpected := func(dbPath, absPath string, level int) error {
		if createdDstDirs[dbPath] {
			return nil
		}
		_, err := dstExpectedStmt.ExecContext(ctx,
			dbPath,
			filepath.Base(absPath),
			absPath,
			parentDBPath(dbPath),
			"folder",
			level,
			0,
			lastMod,
		)
		if err == nil {
			createdDstDirs[dbPath] = true
		}
		return err
	}

	// Phase A — build directory pool up to max_depth
	type dirNode struct {
		relPath string
		level   int
	}
	buildDirPool := func() []dirNode {
		pool := []dirNode{{relPath: "", level: 0}} // root "/"
		q := []dirNode{{relPath: "", level: 0}}

		for len(q) > 0 {
			cur := q[0]
			q = q[1:]

			if cur.level >= cfg.MaxDepth {
				continue
			}
			// Create between 1..Fanout subdirs under this dir
			numDirs := 1 + rng.Intn(cfg.Fanout)
			// Optional softening: sometimes reduce directory count
			if rng.Float64() < 0.30 && numDirs > 1 {
				numDirs--
			}

			for d := 0; d < numDirs; d++ {
				dirName := fmt.Sprintf("dir_%d", len(pool)) // stable-ish name
				relDir := filepath.Join(cur.relPath, dirName)
				absDirSrc := filepath.Join(absSrc, relDir)
				absDirDst := filepath.Join(absDst, relDir)

				// FS
				if err := os.MkdirAll(absDirSrc, 0o755); err != nil {
					fatalf("mkdir src subdir: %v", err)
				}
				if err := os.MkdirAll(absDirDst, 0o755); err != nil {
					fatalf("mkdir dst subdir: %v", err)
				}

				// DB - populate expected tables
				dbPath := toDBPath(relDir)
				if err := ensureSrcDirExpected(dbPath, absDirSrc, cur.level+1); err != nil {
					fatalf("insert src expected dir: %v", err)
				}
				if err := ensureDstDirExpected(dbPath, absDirDst, cur.level+1); err != nil {
					fatalf("insert dst expected dir: %v", err)
				}

				n := dirNode{relPath: relDir, level: cur.level + 1}
				pool = append(pool, n)
				q = append(q, n)
			}
		}

		// Guarantee at least one non-root dir if depth > 0
		if cfg.MaxDepth > 0 && len(pool) == 1 {
			relDir := "dir_forced_0"
			absDirSrc := filepath.Join(absSrc, relDir)
			absDirDst := filepath.Join(absDst, relDir)
			if err := os.MkdirAll(absDirSrc, 0o755); err != nil {
				fatalf("mkdir forced src: %v", err)
			}
			if err := os.MkdirAll(absDirDst, 0o755); err != nil {
				fatalf("mkdir forced dst: %v", err)
			}
			dbPath := toDBPath(relDir)
			if err := ensureSrcDirExpected(dbPath, absDirSrc, 1); err != nil {
				fatalf("insert forced src expected dir: %v", err)
			}
			if err := ensureDstDirExpected(dbPath, absDirDst, 1); err != nil {
				fatalf("insert forced dst expected dir: %v", err)
			}
			pool = append(pool, dirNode{relPath: relDir, level: 1})
		}

		// Optional: shuffle the dir pool for more randomness in placement
		rng.Shuffle(len(pool), func(i, j int) { pool[i], pool[j] = pool[j], pool[i] })

		return pool
	}

	dirPool := buildDirPool()

	// Phase B — place exactly NumFiles round-robin across dirPool
	filesMade := 0
	for filesMade < cfg.NumFiles {
		d := dirPool[filesMade%len(dirPool)] // round-robin target dir

		fname := fmt.Sprintf("f_%d.txt", filesMade)
		relFile := filepath.Join(d.relPath, fname)

		// SRC file
		absFileSrc := filepath.Join(absSrc, relFile)
		if err := os.MkdirAll(filepath.Dir(absFileSrc), 0o755); err != nil {
			fatalf("mkdir src parent: %v", err)
		}
		if err := os.WriteFile(absFileSrc, []byte("test"), 0o644); err != nil {
			fatalf("write src file: %v", err)
		}
		sinfo, _ := os.Stat(absFileSrc)

		dbPath := toDBPath(relFile)
		parent := parentDBPath(dbPath)

		// ensure parent dirs (src expected)
		parentAbsSrc := absSrc
		if parent != "/" {
			parentAbsSrc = filepath.Join(absSrc, strings.TrimPrefix(parent, "/"))
		}
		if err := ensureSrcDirExpected(parent, parentAbsSrc, d.level); err != nil {
			fatalf("ensure src expected parent dir: %v", err)
		}

		// src expected row
		if _, err := srcExpectedStmt.ExecContext(ctx,
			dbPath, filepath.Base(absFileSrc), absFileSrc, parent,
			"file", d.level+1, sinfo.Size(), lastMod,
		); err != nil {
			fatalf("insert src expected file: %v", err)
		}

		// Mirror on DST with probability
		if rng.Float64() < cfg.IncludeDstPct {
			absFileDst := filepath.Join(absDst, relFile)
			if err := os.MkdirAll(filepath.Dir(absFileDst), 0o755); err != nil {
				fatalf("mkdir dst parent: %v", err)
			}
			if err := os.WriteFile(absFileDst, []byte("test"), 0o644); err != nil {
				fatalf("write dst file: %v", err)
			}
			dinfo, _ := os.Stat(absFileDst)

			parentAbsDst := absDst
			if parent != "/" {
				parentAbsDst = filepath.Join(absDst, strings.TrimPrefix(parent, "/"))
			}
			if err := ensureDstDirExpected(parent, parentAbsDst, d.level); err != nil {
				fatalf("ensure dst expected parent dir: %v", err)
			}

			if _, err := dstExpectedStmt.ExecContext(ctx,
				dbPath, filepath.Base(absFileDst), absFileDst, parent,
				"file", d.level+1, dinfo.Size(), lastMod,
			); err != nil {
				fatalf("insert dst expected file: %v", err)
			}
		}

		// occasional dst-only sibling at same dir
		if rng.Float64() < cfg.DstOnlyPct {
			onlyName := fmt.Sprintf("dst_only_%d.txt", filesMade)
			relOnly := filepath.Join(d.relPath, onlyName)
			absOnlyDst := filepath.Join(absDst, relOnly)
			if err := os.MkdirAll(filepath.Dir(absOnlyDst), 0o755); err != nil {
				fatalf("mkdir dst-only parent: %v", err)
			}
			if err := os.WriteFile(absOnlyDst, []byte("dst-only"), 0o644); err != nil {
				fatalf("write dst-only file: %v", err)
			}
			oInfo, _ := os.Stat(absOnlyDst)
			dbOnlyPath := toDBPath(relOnly)
			onlyParent := parentDBPath(dbOnlyPath)
			onlyParentAbsDst := absDst
			if onlyParent != "/" {
				onlyParentAbsDst = filepath.Join(absDst, strings.TrimPrefix(onlyParent, "/"))
			}
			if err := ensureDstDirExpected(onlyParent, onlyParentAbsDst, d.level); err != nil {
				fatalf("ensure dst expected parent dir (dst-only): %v", err)
			}
			if _, err := dstExpectedStmt.ExecContext(ctx,
				dbOnlyPath, filepath.Base(absOnlyDst), absOnlyDst, onlyParent,
				"file", d.level+1, oInfo.Size(), lastMod,
			); err != nil {
				fatalf("insert dst expected-only file: %v", err)
			}
		}

		filesMade++
	}

	// -------------------------------------

	if err := tx.Commit(); err != nil {
		fatalf("commit: %v", err)
	}

	fmt.Printf("✅ Generated exactly %d files (≤ depth %d) and populated expected results.\n", cfg.NumFiles, cfg.MaxDepth)
	fmt.Println("   Tables: source_nodes (root only), destination_nodes (root only), *_expected (full data), audit_log, *_errors")
}

// createExpectedTables creates the *_expected tables to compare traversal results against
func createExpectedTables(ctx context.Context, tx *sql.Tx) error {
	// Create source_nodes_expected table
	srcExpectedDDL := `CREATE TABLE IF NOT EXISTS source_nodes_expected (
		path VARCHAR PRIMARY KEY,
		name VARCHAR NOT NULL,
		identifier VARCHAR NOT NULL,
		parent_id VARCHAR,
		type VARCHAR NOT NULL,
		level INTEGER NOT NULL,
		size BIGINT NOT NULL,
		last_modified TIMESTAMP NOT NULL,
		traversal_status VARCHAR NOT NULL DEFAULT 'pending',
		upload_status VARCHAR NOT NULL DEFAULT 'pending',
		traversal_attempts INTEGER NOT NULL DEFAULT 0,
		upload_attempts INTEGER NOT NULL DEFAULT 0,
		error_ids VARCHAR,
		last_error VARCHAR
	)`
	if _, err := tx.ExecContext(ctx, srcExpectedDDL); err != nil {
		return fmt.Errorf("creating source_nodes_expected: %w", err)
	}
	fmt.Println("📜 Created table: source_nodes_expected")

	// Create destination_nodes_expected table
	dstExpectedDDL := `CREATE TABLE IF NOT EXISTS destination_nodes_expected (
		path VARCHAR PRIMARY KEY,
		name VARCHAR NOT NULL,
		identifier VARCHAR NOT NULL,
		parent_id VARCHAR,
		type VARCHAR NOT NULL,
		level INTEGER NOT NULL,
		size BIGINT NOT NULL,
		last_modified TIMESTAMP NOT NULL,
		traversal_status VARCHAR NOT NULL DEFAULT 'pending',
		traversal_attempts INTEGER NOT NULL DEFAULT 0,
		error_ids VARCHAR,
		last_error VARCHAR
	)`
	if _, err := tx.ExecContext(ctx, dstExpectedDDL); err != nil {
		return fmt.Errorf("creating destination_nodes_expected: %w", err)
	}
	fmt.Println("📜 Created table: destination_nodes_expected")

	return nil
}

// ---------- helpers ----------

func loadConfig(path string) (*SeedConfig, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg SeedConfig
	if err := json.Unmarshal(b, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func fatalf(f string, a ...any) {
	fmt.Printf("❌ "+f+"\n", a...)
	os.Exit(1)
}

func applyTablesFromLive(ctx context.Context, tx *sql.Tx) error {
	tbls := []interface {
		Name() string
		Schema() string
	}{
		tables.AuditLogTable{},
		tables.SourceNodesTable{},
		tables.DestinationNodesTable{},
	}
	for _, t := range tbls {
		ddl := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", t.Name(), t.Schema())
		if _, err := tx.ExecContext(ctx, ddl); err != nil {
			return fmt.Errorf("creating table %q: %w", t.Name(), err)
		}
		fmt.Printf("📜 Ensured table: %s\n", t.Name())
	}
	// TEMP until you add these to the tables package:
	if _, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS src_nodes_errors (id VARCHAR PRIMARY KEY)`); err != nil {
		return fmt.Errorf("creating src_nodes_errors: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS dst_nodes_errors (id VARCHAR PRIMARY KEY)`); err != nil {
		return fmt.Errorf("creating dst_nodes_errors: %w", err)
	}
	fmt.Println("📜 Ensured tables: src_nodes_errors, dst_nodes_errors")
	return nil
}

func insertSourceRoot(ctx context.Context, tx *sql.Tx, absSrc string, lastMod time.Time) error {
	stmt := `
INSERT INTO source_nodes (
    path, name, identifier, parent_id, type, level, size,
    last_modified, traversal_status, upload_status,
    traversal_attempts, upload_attempts, error_ids
) VALUES ('/', ?, ?, '', 'folder', 0, 0, ?, 'pending', 'pending', 0, 0, NULL)
`
	_, err := tx.ExecContext(ctx, stmt, filepath.Base(absSrc), absSrc, lastMod)
	return err
}

func insertDestRoot(ctx context.Context, tx *sql.Tx, absDst string, lastMod time.Time) error {
	stmt := `
INSERT INTO destination_nodes (
    path, name, identifier, parent_id, type, level, size,
    last_modified, traversal_status, traversal_attempts, error_ids
) VALUES ('/', ?, ?, '', 'folder', 0, 0, ?, 'pending', 0, NULL)
`
	_, err := tx.ExecContext(ctx, stmt, filepath.Base(absDst), absDst, lastMod)
	return err
}

// ---------- end helpers ----------
