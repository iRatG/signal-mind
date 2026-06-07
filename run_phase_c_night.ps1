# Phase C — ежедневный ночной Ouroboros-цикл
# Запускается Task Scheduler в 22:00, останавливается в 05:30
# Лог пишется в analytics/phase_c/logs/

$ROOT   = "C:\project\signal_mind"
$PYTHON = "$ROOT\.venv\Scripts\python.exe"
$LOG_DIR = "$ROOT\analytics\phase_c\logs"

if (-not (Test-Path $LOG_DIR)) { New-Item -ItemType Directory -Path $LOG_DIR | Out-Null }

$DATE    = (Get-Date -Format "yyyyMMdd")
$LOGFILE = "$LOG_DIR\phase_c_$DATE.log"

"[$(Get-Date -Format 'HH:mm:ss')] Phase C night run starting" | Tee-Object -FilePath $LOGFILE -Append

& $PYTHON -m src.pipeline_c.phase_c_overnight `
    --no-extract `
    --stop-at 05:30 `
    2>&1 | Tee-Object -FilePath $LOGFILE -Append

"[$(Get-Date -Format 'HH:mm:ss')] Phase C night run finished" | Tee-Object -FilePath $LOGFILE -Append
