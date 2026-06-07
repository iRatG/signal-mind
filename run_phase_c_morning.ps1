# Phase C — утренний цикл
# Task Scheduler: 07:00 MSK (04:00 UTC) → стоп 14:00 MSK (11:00 UTC)

$ROOT    = "C:\project\signal_mind"
$PYTHON  = "$ROOT\.venv\Scripts\python.exe"
$LOG_DIR = "$ROOT\analytics\phase_c\logs"

if (-not (Test-Path $LOG_DIR)) { New-Item -ItemType Directory -Path $LOG_DIR | Out-Null }

$DATE    = (Get-Date -Format "yyyyMMdd")
$LOGFILE = "$LOG_DIR\phase_c_morning_$DATE.log"

"[$(Get-Date -Format 'HH:mm:ss')] Phase C MORNING run starting (stop at 11:00 UTC)" |
    Tee-Object -FilePath $LOGFILE -Append

& $PYTHON -m src.pipeline_c.phase_c_overnight `
    --no-extract `
    --stop-at 11:00 `
    2>&1 | Tee-Object -FilePath $LOGFILE -Append

"[$(Get-Date -Format 'HH:mm:ss')] Phase C MORNING run finished" |
    Tee-Object -FilePath $LOGFILE -Append
