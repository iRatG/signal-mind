# Signal Mind — межсессионный анализ (13:00)
# Читает все прошлые результаты, обновляет session_config.json,
# пишет аналитический отчёт. Следующая сессия (14:00) стартует умнее.

$ProjectDir = "c:\project\signal_mind"
$LogDir     = "$ProjectDir\analytics\phase_b\analysis"
$Python     = "$ProjectDir\.venv\Scripts\python.exe"
$Today      = (Get-Date).ToString("yyyyMMdd_HHmm")
$RunLog     = "$LogDir\analyzer_$Today.log"

New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
Set-Location $ProjectDir

"[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] ====== INTER-SESSION ANALYSIS STARTED ======" |
    Out-File $RunLog -Encoding utf8

& $Python -m src.pipeline_v2.inter_session_analyzer 2>&1 | Tee-Object -FilePath $RunLog -Append

$ExitCode = $LASTEXITCODE
"[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] ====== ANALYSIS FINISHED (exit=$ExitCode) ======" |
    Out-File $RunLog -Append -Encoding utf8

# Показать итог в консоли (если запущено вручную)
$ConfigPath = "$ProjectDir\analytics\phase_b\session_config.json"
if (Test-Path $ConfigPath) {
    Write-Host "`n=== session_config.json обновлён ===" -ForegroundColor Green
    $cfg = Get-Content $ConfigPath -Encoding utf8 | ConvertFrom-Json
    Write-Host "  Сессий проанализировано : $($cfg.sessions_analyzed)"
    Write-Host "  Val pass rate           : $($cfg.val_pass_rate)"
    Write-Host "  IC gate (след. сессия)  : $($cfg.ic_gate)  [$($cfg.ic_gate_reason)]"
    Write-Host "  Ensemble (след. сессия) : $($cfg.ensemble)"
    Write-Host "  Best feature            : $($cfg.best_feature)"
    Write-Host "  Stable signals          : $($cfg.stable_signals.Count)"
    Write-Host ""
}
