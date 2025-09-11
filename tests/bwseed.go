package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/Voltaic314/ByteWave/tests/db"
	"github.com/Voltaic314/ByteWave/tests/db/tables"
	typesdb "github.com/Voltaic314/ByteWave/tests/types/db"
	"github.com/google/uuid"
	_ "github.com/marcboeker/go-duckdb"
)

// ParentNode represents a parent node from the Oracle DB for child generation
type ParentNode struct {
	ID   string `json:"id"`
	Path string `json:"path"`
}

// OracleNode struct removed - using direct DB inserts instead of in-memory accumulation
func main() {
	cfgPath := "tests/traversal_tests/mock_fs_config.json"
	if env := os.Getenv("BW_SEED_CONFIG"); env != "" {
		cfgPath = env
	}
	cfg, err := loadConfig(cfgPath)
	if err != nil {
		fatalf("load config: %v", err)
	}

	// Validate config
	if err := validateConfig(cfg); err != nil {
		fatalf("invalid config: %v", err)
	}

	// Set up RNG
	seed := cfg.Generation.Seed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	rng := rand.New(rand.NewSource(seed))
	fmt.Printf("🎲 Seed: %d\n", seed)

	// Clean up existing Oracle DB
	oraclePath, _ := filepath.Abs(cfg.Databases.Oracle)
	if err := os.RemoveAll(oraclePath); err != nil && !os.IsNotExist(err) {
		fatalf("remove existing oracle db: %v", err)
	}
	if err := os.RemoveAll(oraclePath + ".wal"); err != nil && !os.IsNotExist(err) {
		fatalf("remove existing oracle wal: %v", err)
	}

	// Initialize Oracle DB with write queues
	oracleDB, err := db.NewDB(oraclePath)
	if err != nil {
		fatalf("create oracle db: %v", err)
	}
	defer oracleDB.Close()

	// Set up write queues for Oracle tables
	oracleDB.InitWriteQueue("source_nodes", typesdb.NodeWriteQueue, 1000, 100*time.Millisecond)
	oracleDB.InitWriteQueue("destination_nodes", typesdb.NodeWriteQueue, 1000, 100*time.Millisecond)

	// Create Oracle tables
	if err := createOracleTables(oracleDB); err != nil {
		fatalf("create oracle tables: %v", err)
	}

	// Generate tree structure using sliding window approach
	fmt.Println("🌳 Generating tree structure...")
	totalSrcNodes, totalDstNodes, err := generateTreeLevelByLevel(cfg, rng, oracleDB)
	if err != nil {
		fatalf("generate tree: %v", err)
	}

	// Force flush all queues
	oracleDB.ForceFlushTable("source_nodes")
	oracleDB.ForceFlushTable("destination_nodes")

	fmt.Printf("✅ Generated %d src nodes and %d dst nodes successfully!\n", totalSrcNodes, totalDstNodes)
}

func loadConfig(path string) (*tables.OracleTestConfig, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg tables.OracleTestConfig
	if err := json.Unmarshal(b, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func validateConfig(cfg *tables.OracleTestConfig) error {
	if cfg.Generation.MinChildFolders < 0 || cfg.Generation.MaxChildFolders < cfg.Generation.MinChildFolders {
		return fmt.Errorf("invalid child folder range: min=%d, max=%d", cfg.Generation.MinChildFolders, cfg.Generation.MaxChildFolders)
	}
	if cfg.Generation.MinChildFiles < 0 || cfg.Generation.MaxChildFiles < cfg.Generation.MinChildFiles {
		return fmt.Errorf("invalid child file range: min=%d, max=%d", cfg.Generation.MinChildFiles, cfg.Generation.MaxChildFiles)
	}
	if cfg.Generation.MinDepth < 1 || cfg.Generation.MaxDepth < cfg.Generation.MinDepth {
		return fmt.Errorf("invalid depth range: min=%d, max=%d", cfg.Generation.MinDepth, cfg.Generation.MaxDepth)
	}
	if cfg.Generation.DstProb < 0.0 || cfg.Generation.DstProb > 1.0 {
		return fmt.Errorf("invalid dst_prob: %f (must be 0.0-1.0)", cfg.Generation.DstProb)
	}
	return nil
}

func createOracleTables(oracleDB *db.DB) error {
	// Use existing table definitions
	tables := []interface {
		Name() string
		Schema() string
	}{
		tables.OracleSrcNodesTable{},
		tables.OracleDstNodesTable{},
	}

	for _, table := range tables {
		ddl := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", table.Name(), table.Schema())
		if err := oracleDB.Write(ddl); err != nil {
			return fmt.Errorf("creating table %q: %w", table.Name(), err)
		}
		fmt.Printf("📜 Created Oracle table: %s\n", table.Name())
	}

	return nil
}

func generateTreeLevelByLevel(cfg *tables.OracleTestConfig, rng *rand.Rand, oracleDB *db.DB) (int64, int64, error) {
	// Generate random depth within range
	depth := cfg.Generation.MinDepth + rng.Intn(cfg.Generation.MaxDepth-cfg.Generation.MinDepth+1)
	fmt.Printf("🎯 Target depth: %d\n", depth)

	var totalSrcNodes, totalDstNodes int64

	// Generate root node (always exists in both src and dst)
	rootSrcID := generateUUID()
	rootDstID := generateUUID()

	// Insert root nodes immediately
	if err := insertRootNodes(oracleDB, rootSrcID, rootDstID); err != nil {
		return 0, 0, fmt.Errorf("insert root nodes: %w", err)
	}
	totalSrcNodes++
	totalDstNodes++

	// Process each level by querying the database
	currentLevel := 1
	for currentLevel <= depth {
		fmt.Printf("📁 Processing level %d...\n", currentLevel)

		// Query database for nodes that need children at this level
		srcCount, dstCount, err := generateChildrenForLevelFromDB(cfg, rng, oracleDB, currentLevel)
		if err != nil {
			return 0, 0, fmt.Errorf("generate children for level %d: %w", currentLevel, err)
		}

		if srcCount == 0 {
			fmt.Printf("📁 No more nodes to process at level %d, stopping\n", currentLevel)
			break
		}

		totalSrcNodes += srcCount
		totalDstNodes += dstCount
		currentLevel++
	}

	return totalSrcNodes, totalDstNodes, nil
}

func insertRootNodes(oracleDB *db.DB, rootSrcID, rootDstID string) error {
	// Root path is always "/"
	rootPath := "/"

	// Insert src root node
	srcQuery := `INSERT INTO source_nodes (id, parent_id, name, path, type, size, level, checked) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
	oracleDB.QueueWrite("source_nodes", srcQuery, rootSrcID, "", "root", rootPath, "folder", 0, 0, false)

	// Insert dst root node
	dstQuery := `INSERT INTO destination_nodes (id, parent_id, name, path, type, size, level, checked) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
	oracleDB.QueueWrite("destination_nodes", dstQuery, rootDstID, "", "root", rootPath, "folder", 0, 0, false)

	return nil
}

func generateChildrenForLevelFromDB(cfg *tables.OracleTestConfig, rng *rand.Rand, oracleDB *db.DB, level int) (int64, int64, error) {
	var totalSrcCount, totalDstCount int64
	const batchSize = 1000 // Process 1000 parents at a time
	var lastSeenRowID int64 = 0

	// Force flush before querying to ensure we have the latest data
	oracleDB.ForceFlushTable("source_nodes")
	oracleDB.ForceFlushTable("destination_nodes")

	for {
		// Query for folder nodes at the current level that need children
		// Use rowid-based pagination for O(1) performance (rowid is monotonic)
		query := `SELECT s.rowid, s.id, s.path FROM source_nodes s
			WHERE s.level = ? AND s.type = 'folder' AND s.rowid > ? 
			ORDER BY s.rowid LIMIT ?`

		rows, err := oracleDB.Query("source_nodes", query, level-1, lastSeenRowID, batchSize)
		if err != nil {
			return 0, 0, fmt.Errorf("query parents for level %d: %w", level, err)
		}

		var parents []ParentNode
		var maxRowID int64 = lastSeenRowID
		for rows.Next() {
			var rowID int64
			var ID, parentPath string
			if err := rows.Scan(&rowID, &ID, &parentPath); err != nil {
				rows.Close()
				return 0, 0, fmt.Errorf("scan parent row: %w", err)
			}
			parents = append(parents, ParentNode{ID: ID, Path: parentPath})
			if rowID > maxRowID {
				maxRowID = rowID
			}
		}
		rows.Close()

		if len(parents) == 0 {
			// No more parents to process
			break
		}

		// Update lastSeenRowID for pagination
		lastSeenRowID = maxRowID

		// Generate children for this batch of parents
		srcCount, dstCount, err := generateChildrenForBatch(cfg, rng, oracleDB, parents, level)
		if err != nil {
			return 0, 0, fmt.Errorf("generate children for batch: %w", err)
		}

		totalSrcCount += srcCount
		totalDstCount += dstCount

		fmt.Printf("📁 Processed %d parents at level %d, generated %d src + %d dst children\n",
			len(parents), level, srcCount, dstCount)

		// If we got fewer results than batch size, we've reached the end
		if len(parents) < batchSize {
			break
		}
	}

	return totalSrcCount, totalDstCount, nil
}

func generateChildrenForBatch(cfg *tables.OracleTestConfig, rng *rand.Rand, oracleDB *db.DB, parents []ParentNode, level int) (int64, int64, error) {
	var srcCount, dstCount int64

	// Process each parent in this batch
	for _, parent := range parents {
		// Generate random number of folders for this parent
		numFolders := cfg.Generation.MinChildFolders + rng.Intn(cfg.Generation.MaxChildFolders-cfg.Generation.MinChildFolders+1)
		for i := 0; i < numFolders; i++ {
			srcID := generateUUID()
			folderName := fmt.Sprintf("folder_%d", i)
			folderPath := buildPath(parent.Path, folderName)

			// Check if this folder should exist in dst
			shouldExistInDst := rng.Float64() < cfg.Generation.DstProb
			var dstID string
			if shouldExistInDst {
				dstID = generateUUID()
			}

			// Always insert src folder
			srcQuery := `INSERT INTO source_nodes (id, parent_id, name, path, type, size, level, checked) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
			oracleDB.QueueWrite("source_nodes", srcQuery, srcID, parent.ID, folderName, folderPath, "folder", 0, level, false)
			srcCount++

			// Only insert dst folder if it should exist
			if shouldExistInDst {
				dstQuery := `INSERT INTO destination_nodes (id, parent_id, name, path, type, size, level, checked) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
				oracleDB.QueueWrite("destination_nodes", dstQuery, dstID, parent.ID, folderName, folderPath, "folder", 0, level, false)
				dstCount++
			}
		}

		// Generate random number of files for this parent
		numFiles := cfg.Generation.MinChildFiles + rng.Intn(cfg.Generation.MaxChildFiles-cfg.Generation.MinChildFiles+1)
		for i := 0; i < numFiles; i++ {
			srcID := generateUUID()
			fileName := fmt.Sprintf("file_%d.txt", i)
			filePath := buildPath(parent.Path, fileName)

			// Check if this file should exist in dst
			shouldExistInDst := rng.Float64() < cfg.Generation.DstProb
			var dstID string
			if shouldExistInDst {
				dstID = generateUUID()
			}

			// Always insert src file
			srcQuery := `INSERT INTO source_nodes (id, parent_id, name, path, type, size, level, checked) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
			fileSize := int64(100 + rng.Intn(900)) // Random size 100-999 bytes
			oracleDB.QueueWrite("source_nodes", srcQuery, srcID, parent.ID, fileName, filePath, "file", fileSize, level, false)
			srcCount++

			// Only insert dst file if it should exist
			if shouldExistInDst {
				dstQuery := `INSERT INTO destination_nodes (id, parent_id, name, path, type, size, level, checked) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
				oracleDB.QueueWrite("destination_nodes", dstQuery, dstID, parent.ID, fileName, filePath, "file", fileSize, level, false)
				dstCount++
			}
		}
	}

	return srcCount, dstCount, nil
}

func generateUUID() string {
	return uuid.New().String()
}

// buildPath constructs the full path for a node based on its parent's path and name
func buildPath(parentPath, name string) string {
	if parentPath == "" {
		// Root node
		return "/"
	}
	return parentPath + "/" + name
}

func fatalf(f string, a ...any) {
	fmt.Printf("❌ "+f+"\n", a...)
	os.Exit(1)
}
