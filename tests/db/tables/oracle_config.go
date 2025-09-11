package tables

// OracleTestConfig represents the configuration for Oracle test harness
type OracleTestConfig struct {
	Databases struct {
		BW     string `json:"bw"`
		Oracle string `json:"oracle"`
	} `json:"databases"`
	Network struct {
		Address string `json:"address"`
		Port    int    `json:"port"`
	} `json:"network"`
	Generation struct {
		Seed            int64   `json:"seed"`
		MinChildFolders int     `json:"min_child_folders"`
		MaxChildFolders int     `json:"max_child_folders"`
		MinChildFiles   int     `json:"min_child_files"`
		MaxChildFiles   int     `json:"max_child_files"`
		MinDepth        int     `json:"min_depth"`
		MaxDepth        int     `json:"max_depth"`
		DstProb         float64 `json:"dst_prob"` // Probability of placing node in dst (0.0-1.0)
	} `json:"generation"`
}
