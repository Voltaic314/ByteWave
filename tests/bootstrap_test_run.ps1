# Requires: Go (CGO-ready for DuckDB), run from repo root
# Usage:
#   .\tests\bootstrap_test_run.ps1
#   .\tests\bootstrap_test_run.ps1 -Config "tests\traversal_tests\config_large.json"

[CmdletBinding()]
param(
  [string]$Config = "tests\traversal_tests\config.json"
)

$ErrorActionPreference = "Stop"

Write-Host "[ByteWave] Using config: $Config"

if (-not (Test-Path $Config)) {
  Write-Host "[WARNING] Config file not found at $Config. The seeder will fall back to its defaults."
} else {
  # Provide the config to the seeder via env var (your Go code reads BW_SEED_CONFIG)
  $resolved = Resolve-Path $Config
  $env:BW_SEED_CONFIG = $resolved
  Write-Host "[ByteWave] BW_SEED_CONFIG => $resolved"
}

# Optional: sanity check Go is available
try {
  $goVersion = & go version
  Write-Host "[ByteWave] $goVersion"
} catch {
  Write-Host "[ERROR] Go is not available on PATH."
  exit 1
}

# 1) Clean up any existing test artifacts 
Write-Host "[ByteWave] Cleaning up previous test artifacts..."
$dbPath = "tests\traversal_tests\test_traversal.db"
$walPath = "tests\traversal_tests\test_traversal.db.wal"
if (Test-Path $dbPath) {
    Remove-Item $dbPath -Force
    Write-Host "[ByteWave] Removed existing DB file"
}
if (Test-Path $walPath) {
    Remove-Item $walPath -Force
    Write-Host "[ByteWave] Removed existing DB WAL file"
}

# 2) Seed DuckDB + generate synthetic tree
Write-Host "[ByteWave] Seeding test DB and generating filesystem tree..."
& go run .\tests\bwseed.go
if ($LASTEXITCODE -ne 0) {
  Write-Host "[ERROR] Seeder failed with exit code $LASTEXITCODE"
  exit $LASTEXITCODE
}

# 3) Launch ByteWave
Write-Host "[ByteWave] Launching ByteWave..."
& go run .\code\main.go
if ($LASTEXITCODE -ne 0) {
  Write-Host "[ERROR] ByteWave exited with code $LASTEXITCODE"
  exit $LASTEXITCODE
}

Write-Host "[ByteWave] Done."
