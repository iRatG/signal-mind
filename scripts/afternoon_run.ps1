# Signal Mind — дневная сессия (14:00 → 21:00)
# Запускается ПОСЛЕ analyze_and_update.ps1.
# Читает session_config.json — стартует с накопленными знаниями всех прошлых сессий.

$ProjectDir = "c:\project\signal_mind"
$LogDir     = "$ProjectDir\analytics\phase_b\night_search"
$Python     = "$ProjectDir\.venv\Scripts\python.exe"
$Today      = (Get-Date).ToString("yyyyMMdd_HHmm")
$RunLog     = "$LogDir\scheduler_afternoon_$Today.log"
$ConfigPath = "$ProjectDir\analytics\phase_b\session_config.json"

Set-Location $ProjectDir

"[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] ====== AFTERNOON SEARCH STARTED ======" |
    Out-File $RunLog -Encoding utf8
"[$(Get-Date -Format 'HH:mm:ss')] Mode: --loop-hours 7 --session-config $ConfigPath" |
    Out-File $RunLog -Append -Encoding utf8

if (Test-Path $ConfigPath) {
    "[$(Get-Date -Format 'HH:mm:ss')] Session config found — starting with accumulated knowledge" |
        Out-File $RunLog -Append -Encoding utf8
} else {
    "[$(Get-Date -Format 'HH:mm:ss')] No session config — starting fresh" |
        Out-File $RunLog -Append -Encoding utf8
}

& $Python -m src.pipeline_v2.night_search `
    --loop-hours 7 `
    --session-config $ConfigPath `
    2>&1 | Tee-Object -FilePath $RunLog -Append

$ExitCode = $LASTEXITCODE
"[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] ====== AFTERNOON SEARCH FINISHED (exit=$ExitCode) ======" |
    Out-File $RunLog -Append -Encoding utf8
