# tests/bootstrap_test_run.ps1

Write-Host "[ByteWave] Checking for Python virtual environment..."

if (!(Test-Path -Path "./venv")) {
    Write-Host "[ByteWave] No venv found. Creating virtual environment..."
    python -m venv venv
} else {
    Write-Host "[ByteWave] Virtual environment already exists. Skipping venv creation."
}

# also do python.exe -m pip install --upgrade pip and then also install duckdb module
Write-Host "[ByteWave] Upgrading pip..."
python.exe -m pip install --upgrade pip

Write-Host "[ByteWave] Installing duckdb module..."
python.exe -m pip install duckdb

# Activate the Python venv and run the reset script
$pythonPath = ".\venv\Scripts\python.exe"
$setupScript = "tests\traversal_tests\setup_db.py"

if (Test-Path $pythonPath) {
    Write-Host "[ByteWave] Running setup script to prepare test database..."
    & $pythonPath $setupScript
} else {
    Write-Host "[ERROR] Could not find Python executable at $pythonPath"
    exit 1
}

# Run your Go app
Write-Host "[ByteWave] Launching ByteWave..."
go run code/main.go
