# Signal Mind — утренняя проверка
# Запускай каждое утро: .\scripts\morning_check.ps1
# Показывает: последний отчёт ночи, сигналы, статус.

$ProjectDir = "c:\project\signal_mind"
$NightDir   = "$ProjectDir\analytics\phase_b\night_search"

# ── Найти последний отчёт ──────────────────────────────────────────────────────
$Reports = Get-ChildItem "$NightDir\night_report_*.md" -ErrorAction SilentlyContinue |
           Sort-Object LastWriteTime -Descending

if (-not $Reports) {
    Write-Host "`nНет отчётов в $NightDir" -ForegroundColor Red
    exit 1
}

$Latest = $Reports[0]
$Age    = (Get-Date) - $Latest.LastWriteTime

Write-Host ""
Write-Host "╔══════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║   SIGNAL MIND — Утренняя сводка          ║" -ForegroundColor Cyan
Write-Host "╚══════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""
Write-Host "Отчёт : $($Latest.Name)" -ForegroundColor Yellow
Write-Host "Время : $($Latest.LastWriteTime.ToString('yyyy-MM-dd HH:mm'))" -ForegroundColor Yellow
Write-Host "Давность: $([int]$Age.TotalHours)ч $($Age.Minutes)мин назад" -ForegroundColor Yellow
Write-Host ""

# ── Быстрые метрики из отчёта ──────────────────────────────────────────────────
$Content = Get-Content $Latest.FullName -Encoding utf8

# Считаем строки с PASS (val-confirmed сигналы)
$ValPass     = ($Content | Select-String "PASS" -SimpleMatch).Count
$TrainLines  = ($Content | Select-String "^\| " -SimpleMatch | Where-Object { $_ -notmatch "Phase|Config|Instru" }).Count
$NoSignals   = ($Content | Select-String "no confirmed" -SimpleMatch).Count

Write-Host "─── Итог ────────────────────────────────────" -ForegroundColor DarkGray
if ($ValPass -gt 0) {
    Write-Host "  Val-confirmed сигналов: $ValPass  ✅" -ForegroundColor Green
} elseif ($TrainLines -gt 0) {
    Write-Host "  Train сигналов: $TrainLines  (Val не прошли)" -ForegroundColor Yellow
} else {
    Write-Host "  Сигналов не найдено" -ForegroundColor Red
}
Write-Host ""

# ── Вывести секцию Val-Confirmed из отчёта ────────────────────────────────────
$InValSection = $false
foreach ($line in $Content) {
    if ($line -match "## Val-Confirmed") { $InValSection = $true }
    if ($InValSection -and $line -match "^## " -and $line -notmatch "Val-Confirmed") { $InValSection = $false }
    if ($InValSection) {
        if ($line -match "PASS|Instrument|---|passed") {
            Write-Host $line -ForegroundColor Green
        } elseif ($line -match "no signals") {
            Write-Host $line -ForegroundColor Red
        } else {
            Write-Host $line
        }
    }
}

Write-Host ""
Write-Host "─── Файлы последних 24 часов ────────────────" -ForegroundColor DarkGray
Get-ChildItem $NightDir |
    Where-Object { $_.LastWriteTime -gt (Get-Date).AddHours(-24) } |
    Sort-Object LastWriteTime |
    ForEach-Object {
        $size = [math]::Round($_.Length / 1KB, 1)
        Write-Host ("  {0,-45} {1,6} KB  [{2}]" -f $_.Name, $size, $_.LastWriteTime.ToString("HH:mm"))
    }

# ── Ledger summary ────────────────────────────────────────────────────────────
$LedgerSummary = "$ProjectDir\analytics\phase_b\ledger_summary.md"
if (Test-Path $LedgerSummary) {
    Write-Host ""
    Write-Host "─── Научный журнал (ledger) ─────────────────" -ForegroundColor DarkGray
    $LedgerContent = Get-Content $LedgerSummary -Encoding utf8
    # Показать только секцию ОБЩАЯ ЧЕСТНОСТЬ
    $InSection = $false
    $LineCount  = 0
    foreach ($line in $LedgerContent) {
        if ($line -match "## ОБЩАЯ ЧЕСТНОСТЬ") { $InSection = $true }
        if ($InSection -and $line -match "^## " -and $line -notmatch "ОБЩАЯ") { break }
        if ($InSection -and $LineCount -lt 20) {
            if ($line -match "🔴|FAIL|CONCERN")  { Write-Host $line -ForegroundColor Red }
            elseif ($line -match "🟡|WARN")       { Write-Host $line -ForegroundColor Yellow }
            elseif ($line -match "✅|реальный")   { Write-Host $line -ForegroundColor Green }
            else { Write-Host $line }
            $LineCount++
        }
    }
    Write-Host ""
    Write-Host "Полный отчёт: analytics\phase_b\ledger_summary.md" -ForegroundColor DarkGray
}

# ── Проверить, не завис ли процесс ────────────────────────────────────────────
Write-Host ""
$Running = Get-Process python -ErrorAction SilentlyContinue |
           Where-Object { $_.CommandLine -like "*night_search*" }
if ($Running) {
    Write-Host "⚡ Процесс night_search сейчас запущен (PID $($Running.Id))" -ForegroundColor Cyan
} else {
    Write-Host "Процесс не запущен (завершился или ещё не стартовал)" -ForegroundColor DarkGray
}

Write-Host ""
Write-Host "Расписание: 06:00 (ночная финишировала) | 13:00 анализ | 14:00 дневная | 22:00 ночная" -ForegroundColor DarkGray
Write-Host ""
