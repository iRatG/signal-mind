# Phase B — Train Scanner overnight runner
# Usage: .\run_phase_b.ps1
#
# Runs M5+M6/AND ensemble on all Train data, validates confirmed signals
# on Val, checks Test. Results go to analytics/phase_b/.
#
# Estimated runtime: 20-40 minutes.

$ErrorActionPreference = "Stop"
$startTime = Get-Date

Write-Host ""
Write-Host "=========================================="
Write-Host "  Phase B Train Scanner"
Write-Host "  Started: $startTime"
Write-Host "=========================================="
Write-Host ""

# Make sure we're in the project directory
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $scriptDir

# Check that venv python exists
$pyexe = ".\venv\Scripts\python.exe"
if (-not (Test-Path $pyexe)) {
    $pyexe = ".\.venv\Scripts\python.exe"
}
if (-not (Test-Path $pyexe)) {
    Write-Host "ERROR: Python venv not found. Tried .\venv\Scripts\python.exe and .\.venv\Scripts\python.exe"
    exit 1
}

Write-Host "Python: $pyexe"
Write-Host ""

# Pre-flight: run unit tests
Write-Host "--- Pre-flight: unit tests ---"
& $pyexe -m pytest analytics/testbed/tests/ -q 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "ERROR: Unit tests failed. Aborting scan."
    exit 1
}
Write-Host "Tests OK."
Write-Host ""

# Create output dir
$outDir = "analytics\phase_b"
if (-not (Test-Path $outDir)) {
    New-Item -ItemType Directory -Path $outDir | Out-Null
}

# Run the scanner (output goes to console + tee to latest_run.log)
$logFile = "$outDir\latest_run.log"
Write-Host "--- Starting Phase B scan ---"
Write-Host "Log: $logFile"
Write-Host ""

& $pyexe -m src.pipeline_v2.train_scanner 2>&1 | Tee-Object -FilePath $logFile

$exitCode = $LASTEXITCODE
$endTime = Get-Date
$elapsed = ($endTime - $startTime).TotalMinutes

Write-Host ""
Write-Host "=========================================="
if ($exitCode -eq 0) {
    Write-Host "  DONE. Elapsed: $([math]::Round($elapsed, 1)) min"
    Write-Host "  Results: $outDir\"
    Write-Host "  Report:  $(Get-ChildItem $outDir -Filter '*_report.md' | Sort-Object LastWriteTime -Descending | Select-Object -First 1 -ExpandProperty Name)"
} else {
    Write-Host "  FAILED (exit code $exitCode)"
    Write-Host "  Check log: $logFile"
}
Write-Host "  Ended: $endTime"
Write-Host "=========================================="
Write-Host ""
