# Full EN news backfill 2025-09-01 -> today through SSH SOCKS5 tunnel.
#
# Usage:
#   .\vpn\run_full_backfill.ps1                 # foreground (terminal stays open, shows ticks)
#   .\vpn\run_full_backfill.ps1 -Background     # detached, survives terminal close
#   .\vpn\run_full_backfill.ps1 -From 2025-09-01 -To 2025-10-01    # custom range
#
# In background mode: process stdout -> db\_en_full.out, stderr -> db\_en_full.err.
# Follow progress: Get-Content -Wait db\_en_full.out

[CmdletBinding()]
param(
    [string]$From    = "2025-09-20",
    [string]$To      = "today",
    [string]$Sources = "bbc,guardian,fox,aljazeera,euronews,france24",
    [string]$RunId   = "en_archive_full_v1",
    [double]$Delay        = 2.0,
    [double]$ArchiveDelay = 1.0,
    [switch]$Background
)

$ErrorActionPreference = "Stop"

# Always run from project root, regardless of where script was invoked.
$ProjectRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $ProjectRoot

$Python = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $Python)) {
    Write-Error "Python venv not found at $Python. Activate or fix path."
    exit 1
}

# Ensure db/ exists for logs.
$DbDir = Join-Path $ProjectRoot "db"
if (-not (Test-Path $DbDir)) { New-Item -ItemType Directory -Path $DbDir | Out-Null }

# Banner so the user sees what is about to happen.
Write-Host ""
Write-Host "============================================================"
Write-Host "  EN news backfill via SSH SOCKS5 tunnel"
Write-Host "============================================================"
Write-Host "  from        : $From"
Write-Host "  to          : $To"
Write-Host "  sources     : $Sources"
Write-Host "  run_id      : $RunId"
Write-Host "  delay/arch  : ${Delay}s / ${ArchiveDelay}s"
Write-Host "  background  : $Background"
Write-Host "============================================================"
Write-Host ""

$argList = @(
    "-m", "vpn.run_full_backfill",
    "--from", $From,
    "--to", $To,
    "--sources", $Sources,
    "--run-id", $RunId,
    "--delay", $Delay,
    "--archive-delay", $ArchiveDelay
)

if ($Background) {
    $stdoutFile = Join-Path $ProjectRoot "db\_en_full.out"
    $stderrFile = Join-Path $ProjectRoot "db\_en_full.err"

    Write-Host "Launching detached. Stdout -> db\_en_full.out, stderr -> db\_en_full.err"
    Write-Host ""

    $proc = Start-Process -FilePath $Python `
                          -ArgumentList $argList `
                          -RedirectStandardOutput $stdoutFile `
                          -RedirectStandardError  $stderrFile `
                          -WindowStyle Hidden `
                          -PassThru

    $pidFile = Join-Path $ProjectRoot "db\_en_full.pid"
    Set-Content -Path $pidFile -Value $proc.Id -Encoding ASCII

    Write-Host "  PID         : $($proc.Id)  (saved to db\_en_full.pid)"
    Write-Host ""
    Write-Host "To follow progress:"
    Write-Host "    Get-Content -Wait db\_en_full.out"
    Write-Host ""
    Write-Host "To stop:"
    Write-Host "    Stop-Process -Id $($proc.Id)"
    Write-Host ""
} else {
    # Foreground: stream Python output directly into this terminal.
    & $Python @argList
    exit $LASTEXITCODE
}
