<#
.SYNOPSIS
    Automated Windows setup for compiling ByteWave (Go + DuckDB).
    Run in PowerShell as Administrator.
#>

function Show-Section($msg) {
    Write-Host "`n=== $msg ===" -ForegroundColor Cyan
}

function Show-Step($msg) {
    Write-Host "[*] $msg" -ForegroundColor Yellow
}

function Show-Success($msg) {
    Write-Host "[OK] $msg" -ForegroundColor Green
}

function Show-Warning($msg) {
    Write-Host "[!] $msg" -ForegroundColor Magenta
}

$DuckDBVersion   = "v1.2.2"
$DuckDBDownload  = "https://github.com/duckdb/duckdb/releases/download/$DuckDBVersion/libduckdb-windows-amd64.zip"
$DuckDBTarget    = "$env:USERPROFILE\duckdb"
$MsysInstaller   = "https://github.com/msys2/msys2-installer/releases/latest/download/msys2-x86_64-latest.exe"
$GoMinVersion    = "1.21"

Write-Host "ByteWave Windows Dev Setup Script`n" -ForegroundColor White

# --- 1. Install MSYS2 ---
Show-Section "MSYS2 Setup"
if (-Not (Test-Path "C:\msys64")) {
    Show-Step "Downloading MSYS2 installer..."
    $installer = "$env:TEMP\msys2-installer.exe"
    Invoke-WebRequest $MsysInstaller -OutFile $installer
    Start-Process $installer -ArgumentList "/S" -Wait
    Show-Success "MSYS2 installed."
} else {
    Show-Success "MSYS2 already present."
}

# --- 2. Install MinGW GCC ---
Show-Section "GCC Setup"
Show-Step "Installing MinGW-w64 GCC..."
Start-Process "C:\msys64\usr\bin\bash.exe" -ArgumentList "-lc", "pacman -Sy --noconfirm mingw-w64-x86_64-gcc" -Wait
Show-Success "GCC installed (mingw-w64)."

# --- 3. DuckDB binaries ---
Show-Section "DuckDB Binaries"
if (-Not (Test-Path $DuckDBTarget)) {
    Show-Step "Downloading DuckDB ($DuckDBVersion)..."
    $zip = "$env:TEMP\duckdb.zip"
    Invoke-WebRequest $DuckDBDownload -OutFile $zip
    Expand-Archive $zip -DestinationPath $DuckDBTarget
    Show-Success "DuckDB extracted to $DuckDBTarget"
} else {
    Show-Success "DuckDB already present at $DuckDBTarget"
}

# --- 4. Environment variables ---
Show-Section "Environment Setup"
$gccPath = "C:\msys64\mingw64\bin"
[System.Environment]::SetEnvironmentVariable("PATH", "$env:PATH;$gccPath;$DuckDBTarget", [System.EnvironmentVariableTarget]::User)
[System.Environment]::SetEnvironmentVariable("CGO_ENABLED", "1", [System.EnvironmentVariableTarget]::User)
[System.Environment]::SetEnvironmentVariable("CC", "x86_64-w64-mingw32-gcc", [System.EnvironmentVariableTarget]::User)
[System.Environment]::SetEnvironmentVariable("CGO_CFLAGS", "-I$DuckDBTarget", [System.EnvironmentVariableTarget]::User)
[System.Environment]::SetEnvironmentVariable("CGO_LDFLAGS", "-L$DuckDBTarget -lduckdb", [System.EnvironmentVariableTarget]::User)

Show-Success "Environment variables configured."

# --- Wrap up ---
Show-Section "Final Notes"
Show-Warning "Restart PowerShell to apply PATH/Env changes."
Write-Host "`nNext steps:" -ForegroundColor White
Write-Host " - Install Go $GoMinVersion+ if not already (https://go.dev/dl/)" -ForegroundColor White
Write-Host " - cd into ByteWave repo" -ForegroundColor White
Write-Host " - Run: go build -o bytewave.exe code/main.go" -ForegroundColor White

Show-Success "Setup complete!"
