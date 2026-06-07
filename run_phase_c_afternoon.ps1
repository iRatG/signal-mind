# Phase C — дневной цикл
# Task Scheduler: 15:00 MSK (12:00 UTC) → стоп 21:00 MSK (18:00 UTC)

$ROOT    = "C:\project\signal_mind"
$PYTHON  = "$ROOT\.venv\Scripts\python.exe"
$LOG_DIR = "$ROOT\analytics\phase_c\logs"

if (-not (Test-Path $LOG_DIR)) { New-Item -ItemType Directory -Path $LOG_DIR | Out-Null }

$DATE    = (Get-Date -Format "yyyyMMdd")
$LOGFILE = "$LOG_DIR\phase_c_afternoon_$DATE.log"

"[$(Get-Date -Format 'HH:mm:ss')] Phase C AFTERNOON run starting (stop at 18:00 UTC)" |
    Tee-Object -FilePath $LOGFILE -Append

& $PYTHON -m src.pipeline_c.phase_c_overnight `
    --no-extract `
    --stop-at 18:00 `
    2>&1 | Tee-Object -FilePath $LOGFILE -Append

"[$(Get-Date -Format 'HH:mm:ss')] Phase C AFTERNOON run finished" |
    Tee-Object -FilePath $LOGFILE -Append
