# Overnight fine-grained lag scan
# Runs M5+M6 on every lag 1..90 (daily resolution) for all
# instruments x topics. ~6930 hypotheses, ~35 min.
# Results: analytics/phase_b/finegrained_*.csv + report
#
# Usage: .\run_overnight.ps1
#   or:  Start-Process powershell -ArgumentList "-File run_overnight.ps1" -WindowStyle Normal

$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $MyInvocation.MyCommand.Path)

$startTime = Get-Date
Write-Host "Overnight scan started: $startTime" -ForegroundColor Cyan

$outDir = "analytics\phase_b"
if (-not (Test-Path $outDir)) { New-Item -ItemType Directory -Path $outDir | Out-Null }
$logFile = "$outDir\overnight_$([datetime]::UtcNow.ToString('yyyyMMdd_HHmmss')).log"

.\.venv\Scripts\python -W ignore -m src.pipeline_v2.rolling_scanner 2>&1 | Tee-Object -FilePath $logFile

$endTime = Get-Date
$mins = [math]::Round(($endTime - $startTime).TotalMinutes, 1)
Write-Host ""
Write-Host "Done in $mins min. Log: $logFile" -ForegroundColor Green
Write-Host "Report: $(Get-ChildItem $outDir -Filter 'finegrained_*_report.md' | Sort LastWriteTime -Desc | Select -First 1 -Exp Name)"
