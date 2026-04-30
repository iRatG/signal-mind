"""Signal Mind — Analytics Report Generator.

Reads telemetry.jsonl, signals.jsonl, metrics.jsonl, experiments.db
and produces a self-contained HTML report with Chart.js charts.

Usage:
    python -m analytics.generate_report
    python -m analytics.generate_report --out analytics/report.html
"""
import json
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

DB_DIR     = Path(__file__).parents[1] / "db"
OUT_FILE      = Path(sys.argv[sys.argv.index("--out") + 1]) if "--out" in sys.argv else \
                Path(__file__).parent / "report.html"
CHARTJS_PATH  = Path(__file__).parent / "chart.umd.min.js"


# ── Load data ──────────────────────────────────────────────────────────────

def load_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    out = []
    for line in path.read_text(encoding="utf-8").strip().splitlines():
        line = line.strip()
        if line:
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return out


telemetry = load_jsonl(DB_DIR / "telemetry.jsonl")
signals   = load_jsonl(DB_DIR / "signals.jsonl")
metrics   = load_jsonl(DB_DIR / "metrics.jsonl")

con = sqlite3.connect(DB_DIR / "experiments.db")
exp_rows = con.execute(
    "SELECT session_id, iteration, confirmed, signal_score, lag_days, "
    "sql_attempts, sql_success, sql_v1_error_type, sql_v2_error_type, sql_v3_error_type "
    "FROM experiments ORDER BY id"
).fetchall()
con.close()


# ── Aggregate metrics ──────────────────────────────────────────────────────

total_iters      = len(telemetry)
confirmed_count  = sum(1 for t in telemetry if t.get("confirmed") is True)
partial_count    = sum(1 for t in telemetry if t.get("confirmed") is None)
rejected_count   = sum(1 for t in telemetry if t.get("confirmed") is False)
confirm_rate     = round(confirmed_count / total_iters * 100, 1) if total_iters else 0

total_tokens     = sum(
    t.get("tok_gen_in", 0) + t.get("tok_gen_out", 0) +
    t.get("tok_eval_in", 0) + t.get("tok_eval_out", 0)
    for t in telemetry
)
total_wall_s     = sum(t.get("t_total_ms", 0) for t in telemetry) / 1000
avg_iter_s       = round(total_wall_s / total_iters, 1) if total_iters else 0
avg_tokens_iter  = round(total_tokens / total_iters) if total_iters else 0

repair_count     = sum(1 for t in telemetry if t.get("sql_attempts", 1) > 1)
failed_count     = sum(1 for t in telemetry if not t.get("sql_success", True))

# Sessions from metrics
sessions_count   = len(metrics)

# Unique confirmed signal score ≥ 70
top_signals = sorted(
    [s for s in signals if s.get("confirmed") is True and (s.get("signal_score") or 0) >= 70],
    key=lambda x: x.get("signal_score", 0), reverse=True
)
seen_hyp, unique_top = set(), []
for s in top_signals:
    key = (s.get("hypothesis") or "")[:60]
    if key not in seen_hyp:
        seen_hyp.add(key)
        unique_top.append(s)

# Estimated cost (DeepSeek chat: ~$0.014 per 1M input tokens, ~$0.028 per 1M output tokens)
tok_in  = sum(t.get("tok_gen_in", 0) + t.get("tok_eval_in", 0) for t in telemetry)
tok_out = sum(t.get("tok_gen_out", 0) + t.get("tok_eval_out", 0) for t in telemetry)
cost_usd = round(tok_in / 1_000_000 * 0.014 + tok_out / 1_000_000 * 0.028, 2)


# ── Chart data ─────────────────────────────────────────────────────────────

BUCKET = 50  # iterations per bucket for outcome chart

def bucket_outcomes(tels, size=BUCKET):
    buckets, labels = [], []
    for i in range(0, len(tels), size):
        chunk = tels[i:i+size]
        c = sum(1 for t in chunk if t.get("confirmed") is True)
        p = sum(1 for t in chunk if t.get("confirmed") is None)
        r = sum(1 for t in chunk if t.get("confirmed") is False)
        buckets.append((c, p, r))
        labels.append(f"{i+1}-{i+len(chunk)}")
    return labels, buckets


def rolling_confirm_rate(tels, window=30):
    rates, labels = [], []
    for i in range(window - 1, len(tels)):
        chunk = tels[i - window + 1: i + 1]
        r = sum(1 for t in chunk if t.get("confirmed") is True) / window * 100
        rates.append(round(r, 1))
        labels.append(i + 1)
    return labels, rates


def cumulative_tokens(tels, step=10):
    cum, labels = [], []
    total = 0
    for i, t in enumerate(tels):
        total += t.get("tok_gen_in", 0) + t.get("tok_gen_out", 0) + \
                 t.get("tok_eval_in", 0) + t.get("tok_eval_out", 0)
        if (i + 1) % step == 0:
            cum.append(total)
            labels.append(i + 1)
    return labels, cum


def timing_series(tels, step=20):
    rag_vals, news_vals, sql_vals, gen_vals, eval_vals, labels = [], [], [], [], [], []
    for i in range(0, len(tels), step):
        chunk = tels[i:i+step]
        n = len(chunk)
        rag_vals.append(round(sum(t.get("t_rag_ms", 0) for t in chunk) / n / 1000, 2))
        news_vals.append(round(sum(t.get("t_news_ms", 0) for t in chunk) / n / 1000, 2))
        sql_vals.append(round(sum(t.get("t_sql_ms", 0) for t in chunk) / n / 1000, 2))
        gen_vals.append(round(sum(t.get("t_llm_gen_ms", 0) for t in chunk) / n / 1000, 2))
        eval_vals.append(round(sum(t.get("t_llm_eval_ms", 0) for t in chunk) / n / 1000, 2))
        labels.append(i + step // 2)
    return labels, rag_vals, news_vals, sql_vals, gen_vals, eval_vals


def score_histogram(tels):
    buckets = defaultdict(int)
    for t in tels:
        s = t.get("signal_score") or 0
        b = (s // 10) * 10
        buckets[b] += 1
    xs = list(range(0, 110, 10))
    return xs, [buckets[x] for x in xs]


def lag_stats(rows):
    lag_data = defaultdict(lambda: {"n": 0, "conf": 0, "scores": []})
    for row in rows:
        _, _, confirmed, score, lag, *_ = row
        lag_data[lag]["n"] += 1
        if confirmed:
            lag_data[lag]["conf"] += 1
        if score:
            lag_data[lag]["scores"].append(score)
    result = {}
    for lag in sorted(lag_data):
        d = lag_data[lag]
        result[lag] = {
            "n": d["n"],
            "conf_rate": round(d["conf"] / d["n"] * 100, 1) if d["n"] else 0,
            "avg_score": round(sum(d["scores"]) / len(d["scores"]), 1) if d["scores"] else 0,
        }
    return result


def sql_attempts_dist(rows):
    dist = defaultdict(int)
    for row in rows:
        attempts = row[5]
        dist[attempts] += 1
    return dict(sorted(dist.items()))


def error_types(rows):
    counts = defaultdict(int)
    for row in rows:
        for et in [row[7], row[8], row[9]]:
            if et:
                counts[et] += 1
    return dict(sorted(counts.items(), key=lambda x: -x[1])[:8])


def context_growth(tels, step=20):
    rag_chars, news_chars, know_chars, labels = [], [], [], []
    for i in range(0, len(tels), step):
        chunk = tels[i:i+step]
        n = len(chunk)
        rag_chars.append(round(sum(t.get("ctx_rag_chars", 0) for t in chunk) / n))
        news_chars.append(round(sum(t.get("ctx_news_chars", 0) for t in chunk) / n))
        know_chars.append(round(sum(t.get("ctx_knowledge_chars", 0) for t in chunk) / n))
        labels.append(i + step // 2)
    return labels, rag_chars, news_chars, know_chars


# Compute all chart data
bucket_labels, bucket_data          = bucket_outcomes(telemetry)
roll_labels, roll_rates             = rolling_confirm_rate(telemetry)
cum_labels, cum_tokens              = cumulative_tokens(telemetry)
timing_labels, t_rag, t_news, t_sql, t_gen, t_eval = timing_series(telemetry)
score_xs, score_ys                  = score_histogram(telemetry)
lag_result                          = lag_stats(exp_rows)
attempts_dist                       = sql_attempts_dist(exp_rows)
err_types                           = error_types(exp_rows)
ctx_labels, ctx_rag, ctx_news, ctx_know = context_growth(telemetry)

# Top 10 confirmed signals for table
top10 = unique_top[:10]

# Pre-build HTML fragments (avoid nested f-string quote issues)
_lag_stats_rows = "".join(
    f'<div class="stat-row"><span>{k}d (n={v["n"]})</span>'
    f'<span class="stat-val">{v["conf_rate"]}% / avg {v["avg_score"]}</span></div>'
    for k, v in lag_result.items()
)
_top10_rows = "".join(
    f'<tr><td><span class="badge badge-green">{s.get("signal_score")}</span></td>'
    f'<td>{(s.get("hypothesis") or "")[:120]}</td>'
    f'<td style="color:#8b949e">{(s.get("finding") or "")[:100]}</td></tr>'
    for s in top10
)
_err_block = ""
if err_types:
    _err_rows = "".join(
        f'<tr><td><code style="color:#79c0ff">{k}</code></td><td>{v}</td></tr>'
        for k, v in err_types.items()
    )
    _err_block = (
        '<div class="section-title">Типы SQL ошибок (repair events)</div>'
        '<div class="card-wide"><table><thead><tr>'
        '<th>Тип ошибки</th><th>Кол-во</th>'
        f'</tr></thead><tbody>{_err_rows}</tbody></table></div>'
    )
_tok_gen_in_avg  = round(sum(t.get("tok_gen_in", 0) for t in telemetry) / total_iters) if total_iters else 0
_tok_gen_out_avg = round(sum(t.get("tok_gen_out", 0) for t in telemetry) / total_iters) if total_iters else 0
_tok_eval_in_avg = round(sum(t.get("tok_eval_in", 0) for t in telemetry) / total_iters) if total_iters else 0
_tok_eval_out_avg= round(sum(t.get("tok_eval_out", 0) for t in telemetry) / total_iters) if total_iters else 0

lag_labels      = [f"{k}d" for k in lag_result]
lag_conf_rates  = [v["conf_rate"] for v in lag_result.values()]
lag_avg_scores  = [v["avg_score"] for v in lag_result.values()]
lag_ns          = [v["n"] for v in lag_result.values()]

conf_series  = [d[0] for d in bucket_data]
part_series  = [d[1] for d in bucket_data]
rej_series   = [d[2] for d in bucket_data]

avg_timing_total = round(sum(t.get("t_total_ms", 0) for t in telemetry) / total_iters / 1000, 1) if total_iters else 0
avg_rag_ms  = round(sum(t.get("t_rag_ms", 0) for t in telemetry) / total_iters) if total_iters else 0
avg_news_ms = round(sum(t.get("t_news_ms", 0) for t in telemetry) / total_iters) if total_iters else 0
avg_sql_ms  = round(sum(t.get("t_sql_ms", 0) for t in telemetry) / total_iters) if total_iters else 0
avg_gen_ms  = round(sum(t.get("t_llm_gen_ms", 0) for t in telemetry) / total_iters) if total_iters else 0
avg_eval_ms = round(sum(t.get("t_llm_eval_ms", 0) for t in telemetry) / total_iters) if total_iters else 0


# ── HTML template ──────────────────────────────────────────────────────────

def j(obj):
    return json.dumps(obj, ensure_ascii=False)


HTML = f"""<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Signal Mind — Marathon Analytics Report</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: 'Segoe UI', Arial, sans-serif; background: #0d1117; color: #c9d1d9; line-height: 1.6; }}
  h1 {{ font-size: 1.8rem; font-weight: 700; color: #58a6ff; }}
  h2 {{ font-size: 1.15rem; font-weight: 600; color: #79c0ff; margin-bottom: 12px; }}
  h3 {{ font-size: 0.95rem; font-weight: 600; color: #8b949e; text-transform: uppercase; letter-spacing: .05em; margin-bottom: 8px; }}
  .header {{ background: #161b22; border-bottom: 1px solid #30363d; padding: 24px 32px; }}
  .header p {{ color: #8b949e; font-size: 0.9rem; margin-top: 4px; }}
  .container {{ max-width: 1400px; margin: 0 auto; padding: 24px 32px; }}
  .kpi-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 16px; margin-bottom: 32px; }}
  .kpi {{ background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 20px; }}
  .kpi .val {{ font-size: 2rem; font-weight: 700; color: #58a6ff; line-height: 1.1; }}
  .kpi .lbl {{ font-size: 0.8rem; color: #8b949e; margin-top: 4px; text-transform: uppercase; letter-spacing: .04em; }}
  .kpi.green .val {{ color: #3fb950; }}
  .kpi.yellow .val {{ color: #d29922; }}
  .kpi.red .val {{ color: #f85149; }}
  .kpi.purple .val {{ color: #bc8cff; }}
  .grid-2 {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 24px; }}
  .grid-3 {{ display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 20px; margin-bottom: 24px; }}
  .card {{ background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 20px; }}
  .card-wide {{ background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 20px; margin-bottom: 24px; }}
  canvas {{ width: 100% !important; }}
  .section-title {{ font-size: 0.75rem; text-transform: uppercase; letter-spacing: .08em; color: #58a6ff; border-bottom: 1px solid #21262d; padding-bottom: 6px; margin: 32px 0 20px; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 0.85rem; }}
  th {{ text-align: left; padding: 8px 12px; color: #8b949e; border-bottom: 1px solid #30363d; font-weight: 600; font-size: 0.78rem; text-transform: uppercase; }}
  td {{ padding: 8px 12px; border-bottom: 1px solid #21262d; vertical-align: top; }}
  tr:last-child td {{ border-bottom: none; }}
  tr:hover td {{ background: #1c2128; }}
  .badge {{ display: inline-block; padding: 2px 8px; border-radius: 12px; font-size: 0.75rem; font-weight: 600; }}
  .badge-green {{ background: #1a3a2a; color: #3fb950; }}
  .badge-blue  {{ background: #1a2a3a; color: #58a6ff; }}
  .badge-red   {{ background: #3a1a1a; color: #f85149; }}
  .arch-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 16px; margin-bottom: 24px; }}
  .arch-card {{ background: #0d1117; border: 1px solid #30363d; border-radius: 8px; padding: 16px; }}
  .arch-card .icon {{ font-size: 1.6rem; margin-bottom: 8px; }}
  .arch-card .title {{ font-size: 0.85rem; font-weight: 700; color: #c9d1d9; margin-bottom: 6px; }}
  .arch-card .items {{ font-size: 0.78rem; color: #8b949e; line-height: 1.8; }}
  .stat-row {{ display: flex; justify-content: space-between; padding: 6px 0; border-bottom: 1px solid #21262d; font-size: 0.85rem; }}
  .stat-row:last-child {{ border-bottom: none; }}
  .stat-val {{ color: #c9d1d9; font-weight: 600; }}
  @media (max-width: 900px) {{ .grid-2, .grid-3, .arch-grid {{ grid-template-columns: 1fr; }} }}
</style>
</head>
<body>

<div class="header">
  <h1>Signal Mind — Marathon Analytics</h1>
  <p>9-hour autonomous Ouroboros run &nbsp;·&nbsp; {total_iters:,} iterations &nbsp;·&nbsp; {sessions_count} sessions &nbsp;·&nbsp; Generated 2026-04-30</p>
</div>

<div class="container">

  <!-- Architecture overview -->
  <div class="section-title">Архитектура системы — 4 слоя</div>
  <div class="arch-grid">
    <div class="arch-card">
      <div class="icon">🗄️</div>
      <div class="title">Слой 1 — Данные</div>
      <div class="items">
        DuckDB: MOEX, форекс, макро<br>
        news_daily: 1710 дней × 7 тем<br>
        ChromaDB: 17 492 чанка PDF<br>
        SQLite: 2.52M новостей (9.9 GB)
      </div>
    </div>
    <div class="arch-card">
      <div class="icon">🔁</div>
      <div class="title">Слой 2 — Агент</div>
      <div class="items">
        Hypothesis → SQL → Execute<br>
        Evaluate → Reflect → Repeat<br>
        LAG sweep: 0/7/14/30/60/90d<br>
        22 темы + 25% рандом-джамп
      </div>
    </div>
    <div class="arch-card">
      <div class="icon">🧠</div>
      <div class="title">Слой 3 — Обучение</div>
      <div class="items">
        SQL repair loop (max 3 попытки)<br>
        knowledge.md — confirmed паттерны<br>
        sql_patterns.md — few-shot память<br>
        experiments.db — датасет A/B/C
      </div>
    </div>
    <div class="arch-card">
      <div class="icon">📡</div>
      <div class="title">Слой 4 — Телеметрия</div>
      <div class="items">
        telemetry.jsonl — токены/шаг<br>
        signals.jsonl — все сигналы<br>
        metrics.jsonl — сессии<br>
        watchdog.log — uptime
      </div>
    </div>
  </div>

  <!-- KPI bar -->
  <div class="section-title">Ключевые показатели</div>
  <div class="kpi-grid">
    <div class="kpi blue"><div class="val">{total_iters:,}</div><div class="lbl">Итераций</div></div>
    <div class="kpi green"><div class="val">{confirmed_count:,}</div><div class="lbl">Confirmed ({confirm_rate}%)</div></div>
    <div class="kpi yellow"><div class="val">{partial_count}</div><div class="lbl">Partial</div></div>
    <div class="kpi red"><div class="val">{rejected_count:,}</div><div class="lbl">Rejected</div></div>
    <div class="kpi purple"><div class="val">{len(unique_top)}</div><div class="lbl">Топ сигналов ≥70</div></div>
    <div class="kpi"><div class="val">{round(total_tokens/1_000_000,1)}M</div><div class="lbl">Токенов всего</div></div>
    <div class="kpi"><div class="val">${cost_usd}</div><div class="lbl">Стоимость (DeepSeek)</div></div>
    <div class="kpi"><div class="val">{avg_iter_s}s</div><div class="lbl">Среднее время / итер</div></div>
    <div class="kpi green"><div class="val">{repair_count}</div><div class="lbl">SQL repair (0 failed)</div></div>
    <div class="kpi"><div class="val">{round(total_wall_s/3600,1)}h</div><div class="lbl">Wall time (LLM)</div></div>
  </div>

  <!-- Charts row 1 -->
  <div class="section-title">Качество сигналов</div>
  <div class="grid-2">
    <div class="card">
      <h2>Исходы по итерациям (по {BUCKET} итераций)</h2>
      <canvas id="outcomesChart" height="220"></canvas>
    </div>
    <div class="card">
      <h2>Rolling Confirmation Rate (окно 30 итераций)</h2>
      <canvas id="rollingChart" height="220"></canvas>
    </div>
  </div>

  <!-- Score distribution + lag -->
  <div class="grid-2">
    <div class="card">
      <h2>Распределение Signal Score</h2>
      <canvas id="scoreChart" height="220"></canvas>
    </div>
    <div class="card">
      <h2>Эффективность лагов (news → market)</h2>
      <canvas id="lagChart" height="220"></canvas>
    </div>
  </div>

  <!-- Tokens + Timing -->
  <div class="section-title">Токены и производительность</div>
  <div class="card-wide">
    <h2>Накопленный расход токенов</h2>
    <canvas id="tokensChart" height="120"></canvas>
  </div>

  <div class="card-wide">
    <h2>Среднее время по шагам (секунды, avg за {20} итераций)</h2>
    <canvas id="timingChart" height="140"></canvas>
  </div>

  <!-- Context + SQL -->
  <div class="section-title">Контекст и SQL надёжность</div>
  <div class="grid-2">
    <div class="card">
      <h2>Размер контекста (avg chars, окно {20} итер)</h2>
      <canvas id="contextChart" height="220"></canvas>
    </div>
    <div class="card">
      <h2>SQL repair — распределение попыток</h2>
      <canvas id="repairChart" height="220"></canvas>
    </div>
  </div>

  <!-- Timing breakdown summary -->
  <div class="section-title">Разбивка времени (avg за все итерации)</div>
  <div class="grid-3">
    <div class="card">
      <h3>Среднее время шага</h3>
      <div class="stat-row"><span>RAG (ChromaDB)</span><span class="stat-val">{avg_rag_ms} ms</span></div>
      <div class="stat-row"><span>News (SQLite → news_daily)</span><span class="stat-val">{avg_news_ms} ms</span></div>
      <div class="stat-row"><span>SQL (DuckDB)</span><span class="stat-val">{avg_sql_ms} ms</span></div>
      <div class="stat-row"><span>LLM Generate</span><span class="stat-val">{avg_gen_ms} ms</span></div>
      <div class="stat-row"><span>LLM Evaluate</span><span class="stat-val">{avg_eval_ms} ms</span></div>
      <div class="stat-row"><span><strong>Total</strong></span><span class="stat-val"><strong>{avg_timing_total} s</strong></span></div>
    </div>
    <div class="card">
      <h3>Токены (avg / итерация)</h3>
      <div class="stat-row"><span>Gen input</span><span class="stat-val">{_tok_gen_in_avg:,}</span></div>
      <div class="stat-row"><span>Gen output</span><span class="stat-val">{_tok_gen_out_avg:,}</span></div>
      <div class="stat-row"><span>Eval input</span><span class="stat-val">{_tok_eval_in_avg:,}</span></div>
      <div class="stat-row"><span>Eval output</span><span class="stat-val">{_tok_eval_out_avg:,}</span></div>
      <div class="stat-row"><span><strong>Total avg/iter</strong></span><span class="stat-val"><strong>{avg_tokens_iter:,}</strong></span></div>
      <div class="stat-row"><span><strong>Total all iters</strong></span><span class="stat-val"><strong>{round(total_tokens/1_000_000,2)}M</strong></span></div>
    </div>
    <div class="card">
      <h3>Лучший лаг</h3>
      {_lag_stats_rows}
    </div>
  </div>

  <!-- Top signals table -->
  <div class="section-title">Топ-10 подтверждённых сигналов (score ≥ 70)</div>
  <div class="card-wide">
    <table>
      <thead>
        <tr>
          <th>Score</th>
          <th>Гипотеза</th>
          <th>Finding (краткое)</th>
        </tr>
      </thead>
      <tbody>
        {_top10_rows}
      </tbody>
    </table>
  </div>

  <!-- Error types -->
  {_err_block}

</div><!-- /container -->

<script>
// Global Chart.js v4 defaults
Chart.defaults.color = '#8b949e';
Chart.defaults.borderColor = '#30363d';
Chart.defaults.plugins.legend.labels.color = '#8b949e';
Chart.defaults.plugins.legend.labels.boxWidth = 12;

function scaleOpts(extra) {{
  return Object.assign({{
    ticks: {{ color: '#8b949e', maxTicksLimit: 14 }},
    grid:  {{ color: '#21262d' }}
  }}, extra || {{}});
}}
function baseOpts(extraScales) {{
  return {{ scales: {{ x: scaleOpts(), y: scaleOpts(), ...(extraScales || {{}}) }} }};
}}

// 1. Outcomes stacked bar
new Chart(document.getElementById('outcomesChart'), {{
  type: 'bar',
  data: {{
    labels: {j(bucket_labels)},
    datasets: [
      {{ label: 'Confirmed', data: {j(conf_series)}, backgroundColor: '#1f6335', borderRadius: 2 }},
      {{ label: 'Partial',   data: {j(part_series)}, backgroundColor: '#7d4e05', borderRadius: 2 }},
      {{ label: 'Rejected',  data: {j(rej_series)},  backgroundColor: '#4a1a1a', borderRadius: 2 }}
    ]
  }},
  options: {{ scales: {{
    x: Object.assign(scaleOpts(), {{ stacked: true }}),
    y: Object.assign(scaleOpts(), {{ stacked: true }})
  }} }}
}});

// 2. Rolling rate
new Chart(document.getElementById('rollingChart'), {{
  type: 'line',
  data: {{
    labels: {j(roll_labels)},
    datasets: [{{
      label: 'Confirmation rate % (30-iter window)',
      data: {j(roll_rates)},
      borderColor: '#3fb950', backgroundColor: 'rgba(63,185,80,0.08)',
      fill: true, tension: 0.3, pointRadius: 0, borderWidth: 2
    }}]
  }},
  options: baseOpts()
}});

// 3. Score histogram
new Chart(document.getElementById('scoreChart'), {{
  type: 'bar',
  data: {{
    labels: {j([f'{x}-{x+9}' for x in score_xs])},
    datasets: [{{
      label: 'Итераций',
      data: {j(score_ys)},
      backgroundColor: '#1f4a7d', borderColor: '#58a6ff', borderWidth: 1
    }}]
  }},
  options: baseOpts()
}});

// 4. Lag chart (dual axis)
new Chart(document.getElementById('lagChart'), {{
  type: 'bar',
  data: {{
    labels: {j(lag_labels)},
    datasets: [
      {{ label: 'Confirmation rate %', data: {j(lag_conf_rates)}, backgroundColor: '#1f6335', borderRadius: 3, yAxisID: 'y' }},
      {{ label: 'Avg score', data: {j(lag_avg_scores)}, type: 'line', borderColor: '#58a6ff', backgroundColor: 'transparent', borderWidth: 2, pointRadius: 4, yAxisID: 'y2' }}
    ]
  }},
  options: {{ scales: {{
    x:  scaleOpts(),
    y:  scaleOpts({{ ticks: {{ color: '#3fb950' }}, title: {{ display: true, text: 'Confirmed %', color: '#3fb950' }} }}),
    y2: scaleOpts({{ position: 'right', grid: {{ drawOnChartArea: false }}, ticks: {{ color: '#58a6ff' }}, title: {{ display: true, text: 'Avg score', color: '#58a6ff' }} }})
  }} }}
}});

// 5. Cumulative tokens
new Chart(document.getElementById('tokensChart'), {{
  type: 'line',
  data: {{
    labels: {j(cum_labels)},
    datasets: [{{
      label: 'Накопленные токены',
      data: {j(cum_tokens)},
      borderColor: '#bc8cff', backgroundColor: 'rgba(188,140,255,0.07)',
      fill: true, tension: 0.2, pointRadius: 0, borderWidth: 2
    }}]
  }},
  options: {{ scales: {{
    x: scaleOpts(),
    y: scaleOpts({{ ticks: {{ color: '#8b949e', callback: function(v) {{ return (v/1000000).toFixed(1)+'M'; }} }} }})
  }} }}
}});

// 6. Timing stacked area
new Chart(document.getElementById('timingChart'), {{
  type: 'line',
  data: {{
    labels: {j(timing_labels)},
    datasets: [
      {{ label: 'LLM Eval',   data: {j(t_eval)},  backgroundColor: 'rgba(248,81,73,0.55)',  borderColor: '#f85149', fill: 'origin', tension: 0.2, pointRadius: 0, borderWidth: 1.5 }},
      {{ label: 'LLM Gen',    data: {j(t_gen)},   backgroundColor: 'rgba(88,166,255,0.55)', borderColor: '#58a6ff', fill: 'origin', tension: 0.2, pointRadius: 0, borderWidth: 1.5 }},
      {{ label: 'SQL',        data: {j(t_sql)},   backgroundColor: 'rgba(121,192,255,0.4)', borderColor: '#79c0ff', fill: 'origin', tension: 0.2, pointRadius: 0, borderWidth: 1.5 }},
      {{ label: 'News',       data: {j(t_news)},  backgroundColor: 'rgba(210,153,34,0.45)', borderColor: '#d29922', fill: 'origin', tension: 0.2, pointRadius: 0, borderWidth: 1.5 }},
      {{ label: 'RAG',        data: {j(t_rag)},   backgroundColor: 'rgba(63,185,80,0.4)',   borderColor: '#3fb950', fill: 'origin', tension: 0.2, pointRadius: 0, borderWidth: 1.5 }}
    ]
  }},
  options: baseOpts()
}});

// 7. Context sizes
new Chart(document.getElementById('contextChart'), {{
  type: 'line',
  data: {{
    labels: {j(ctx_labels)},
    datasets: [
      {{ label: 'RAG chars',       data: {j(ctx_rag)},   borderColor: '#3fb950', backgroundColor: 'transparent', tension: 0.3, pointRadius: 0, borderWidth: 2 }},
      {{ label: 'News chars',      data: {j(ctx_news)},  borderColor: '#d29922', backgroundColor: 'transparent', tension: 0.3, pointRadius: 0, borderWidth: 2 }},
      {{ label: 'Knowledge chars', data: {j(ctx_know)},  borderColor: '#bc8cff', backgroundColor: 'transparent', tension: 0.3, pointRadius: 0, borderWidth: 2 }}
    ]
  }},
  options: baseOpts()
}});

// 8. SQL repair
new Chart(document.getElementById('repairChart'), {{
  type: 'bar',
  data: {{
    labels: {j([f'{k} попытка(и)' for k in attempts_dist])},
    datasets: [{{
      label: 'Итераций',
      data: {j(list(attempts_dist.values()))},
      backgroundColor: ['#1f6335','#7d4e05','#4a1a1a','#2a1a4a'].slice(0, {len(attempts_dist)})
    }}]
  }},
  options: baseOpts()
}});
</script>
</body>
</html>"""

# Embed Chart.js inline if available, otherwise fall back to CDN
if CHARTJS_PATH.exists():
    chartjs_js = CHARTJS_PATH.read_text(encoding="utf-8")
    HTML = HTML.replace(
        '<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>',
        f"<script>{chartjs_js}</script>"
    )

OUT_FILE.parent.mkdir(exist_ok=True)
OUT_FILE.write_text(HTML, encoding="utf-8")
print(f"Report written to: {OUT_FILE.resolve()}")
print(f"  {total_iters:,} iterations | {confirmed_count} confirmed ({confirm_rate}%) | {len(unique_top)} top signals")
print(f"  {round(total_tokens/1e6,1)}M tokens | ${cost_usd} est. cost | {round(total_wall_s/3600,1)}h wall time")
