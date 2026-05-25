# Signal Mind — ночной поиск сигналов
# Запускается по расписанию в 22:00. Финиш ~05:00 (7 часов Ouroboros).
# Результаты: analytics\phase_b\night_search\night_report_*.md

$ProjectDir = "c:\project\signal_mind"
$LogDir     = "$ProjectDir\analytics\phase_b\night_search"
$Python     = "$ProjectDir\.venv\Scripts\python.exe"
$Today      = (Get-Date).ToString("yyyyMMdd_HHmm")
$RunLog     = "$LogDir\scheduler_$Today.log"

Set-Location $ProjectDir

"[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] ====== NIGHT SEARCH STARTED ======" | Out-File $RunLog -Encoding utf8
"[$(Get-Date -Format 'HH:mm:ss')] Project: $ProjectDir"                           | Out-File $RunLog -Append -Encoding utf8
"[$(Get-Date -Format 'HH:mm:ss')] Mode: --loop-hours 7"                           | Out-File $RunLog -Append -Encoding utf8

# Запуск — stdout + stderr идут в лог и на экран (если запущено вручную)
& $Python -m src.pipeline_v2.night_search --loop-hours 7 2>&1 | Tee-Object -FilePath $RunLog -Append

$ExitCode = $LASTEXITCODE
"[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] ====== NIGHT SEARCH FINISHED (exit=$ExitCode) ======" | Out-File $RunLog -Append -Encoding utf8
