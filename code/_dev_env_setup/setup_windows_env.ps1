<#
.SYNOPSIS
    Automated Windows setup for compiling ByteWave (Go + DuckDB).
    Run in PowerShell as Administrator.
    This script follows the ByteWave Windows Compilation Guide.
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

function Show-Error($msg) {
    Write-Host "[ERROR] $msg" -ForegroundColor Red
}

# Configuration
$DuckDBVersion = "v1.2.2"
$DuckDBDownload = "https://github.com/duckdb/duckdb/releases/download/$DuckDBVersion/libduckdb-windows-amd64.zip"
$DuckDBTarget = "$env:USERPROFILE\duckdb"
$MsysInstaller = "https://github.com/msys2/msys2-installer/releases/latest/download/msys2-x86_64-latest.exe"
$GoMinVersion = "1.21"

Write-Host "ByteWave Windows Dev Setup Script`n" -ForegroundColor White
Write-Host "Following the ByteWave Windows Compilation Guide" -ForegroundColor White

# --- 1. Install MSYS2 (Headless) ---
Show-Section "MSYS2 Setup"
if (-Not (Test-Path "C:\msys64")) {
    Show-Step "Downloading MSYS2 installer..."
    $installer = "$env:TEMP\msys2-installer.exe"
    try {
        Invoke-WebRequest $MsysInstaller -OutFile $installer
        Show-Step "Installing MSYS2 (headless)..."
        
        # Install MSYS2 silently with UTF-8 support
        $process = Start-Process $installer -ArgumentList "--accept-messages", "--accept-licenses", "--confirm-command", "--root", "C:\msys64", "--locale", "en_US.UTF-8" -Wait -PassThru
        
        if ($process.ExitCode -eq 0) {
            Show-Success "MSYS2 installed successfully."
        } else {
            Show-Error "MSYS2 installation failed with exit code: $($process.ExitCode)"
            exit 1
        }
    } catch {
        Show-Error "Failed to download or install MSYS2: $_"
        exit 1
    }
} else {
    Show-Success "MSYS2 already present at C:\msys64"
}

# --- 2. Update MSYS2 and Install GCC ---
Show-Section "MSYS2 Update and GCC Installation"
Show-Step "Updating MSYS2 packages..."

# Create a batch file to run MSYS2 commands
$batchFile = "$env:TEMP\msys2_setup.bat"
$batchContent = @"
@echo off
C:\msys64\usr\bin\bash.exe -lc "pacman -Syu --noconfirm"
C:\msys64\usr\bin\bash.exe -lc "pacman -S mingw-w64-x86_64-gcc --noconfirm"
C:\msys64\usr\bin\bash.exe -lc "pacman -S base-devel --noconfirm"
"@

$batchContent | Out-File -FilePath $batchFile -Encoding ASCII

try {
    Show-Step "Running MSYS2 updates and installing GCC..."
    $process = Start-Process $batchFile -Wait -PassThru -NoNewWindow
    
    if ($process.ExitCode -eq 0) {
        Show-Success "MSYS2 updated and GCC installed successfully."
    } else {
        Show-Warning "MSYS2 setup completed with warnings (exit code: $($process.ExitCode))"
    }
} catch {
    Show-Error "Failed to run MSYS2 setup: $_"
    exit 1
} finally {
    # Clean up batch file
    if (Test-Path $batchFile) {
        Remove-Item $batchFile -Force
    }
}

# --- 3. DuckDB Binaries ---
Show-Section "DuckDB Binaries"
if (-Not (Test-Path $DuckDBTarget)) {
    Show-Step "Downloading DuckDB ($DuckDBVersion)..."
    try {
        $zip = "$env:TEMP\duckdb.zip"
        Invoke-WebRequest $DuckDBDownload -OutFile $zip
        Expand-Archive $zip -DestinationPath $DuckDBTarget -Force
        Show-Success "DuckDB extracted to $DuckDBTarget"
    } catch {
        Show-Error "Failed to download or extract DuckDB: $_"
        exit 1
    }
} else {
    Show-Success "DuckDB already present at $DuckDBTarget"
}

# --- 4. Environment Variables (Current Session) ---
Show-Section "Environment Setup (Current Session)"
$gccPath = "C:\msys64\mingw64\bin"

# Set environment variables for current session
$env:PATH += ";$gccPath;$DuckDBTarget"
$env:CGO_ENABLED = "1"
$env:CC = "x86_64-w64-mingw32-gcc"
$env:CGO_CFLAGS = "-I$DuckDBTarget"
$env:CGO_LDFLAGS = "-L$DuckDBTarget -lduckdb"

Show-Success "Environment variables set for current session."

# --- 5. Verify Installation ---
Show-Section "Verification"
Show-Step "Verifying GCC installation..."
try {
    $gccVersion = & "$gccPath\x86_64-w64-mingw32-gcc.exe" --version 2>$null
    if ($gccVersion) {
        Show-Success "GCC found: $($gccVersion[0])"
    } else {
        Show-Warning "GCC not found in PATH, but may be available in MSYS2"
    }
} catch {
    Show-Warning "Could not verify GCC installation: $_"
}

Show-Step "Verifying DuckDB files..."
$duckdbFiles = @("duckdb.dll", "duckdb.lib", "duckdb.h")
$missingFiles = @()
foreach ($file in $duckdbFiles) {
    if (Test-Path "$DuckDBTarget\$file") {
        Show-Success "Found: $file"
    } else {
        $missingFiles += $file
        Show-Warning "Missing: $file"
    }
}

if ($missingFiles.Count -gt 0) {
    Show-Warning "Some DuckDB files are missing. Check the extraction."
}

# --- 6. Permanent Environment Variables ---
Show-Section "Permanent Environment Setup"
Show-Step "Setting permanent environment variables..."
try {
    [System.Environment]::SetEnvironmentVariable("PATH", "$env:PATH", [System.EnvironmentVariableTarget]::User)
    [System.Environment]::SetEnvironmentVariable("CGO_ENABLED", "1", [System.EnvironmentVariableTarget]::User)
    [System.Environment]::SetEnvironmentVariable("CC", "x86_64-w64-mingw32-gcc", [System.EnvironmentVariableTarget]::User)
    [System.Environment]::SetEnvironmentVariable("CGO_CFLAGS", "-I$DuckDBTarget", [System.EnvironmentVariableTarget]::User)
    [System.Environment]::SetEnvironmentVariable("CGO_LDFLAGS", "-L$DuckDBTarget -lduckdb", [System.EnvironmentVariableTarget]::User)
    Show-Success "Permanent environment variables configured."
} catch {
    Show-Warning "Failed to set permanent environment variables: $_"
}

# --- Wrap up ---
Show-Section "Final Notes"
Show-Warning "Restart PowerShell to apply permanent PATH/Env changes."
Write-Host "`nNext steps:" -ForegroundColor White
Write-Host " - Install Go $GoMinVersion+ if not already (https://go.dev/dl/)" -ForegroundColor White
Write-Host " - cd into ByteWave repo" -ForegroundColor White
Write-Host " - Run: go build -o bytewave.exe code/main.go" -ForegroundColor White
Write-Host " - Or run: go run code/main.go" -ForegroundColor White

Show-Success "Setup complete! You can now compile ByteWave with CGO support."
