"""Generate dark-theme PNG charts for Markdown article."""
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.gridspec as gridspec
from matplotlib.lines import Line2D
import numpy as np
import duckdb, os

OUT = 'habr/sample/charts'
os.makedirs(OUT, exist_ok=True)

BG, BG2, BG3 = '#0f1117', '#161b27', '#1e2535'
GRID, TEXT, DIM = '#2a3347', '#d4dbe8', '#7a8599'
BLUE, GREEN, AMBER, RED, PURPLE = '#4f8ef7', '#6ee7b7', '#f59e0b', '#f87171', '#a78bfa'

def darkax(ax):
    ax.set_facecolor(BG2)
    ax.tick_params(colors=DIM, labelsize=9)
    ax.xaxis.label.set_color(DIM); ax.yaxis.label.set_color(DIM)
    for sp in ax.spines.values(): sp.set_color(GRID)
    ax.grid(color=GRID, linewidth=0.5, alpha=0.6)
    ax.title.set_color(TEXT)
    return ax

def save(fig, name):
    fig.savefig(f'{OUT}/{name}', dpi=150, bbox_inches='tight', facecolor=BG)
    plt.close(fig)
    print(f'  saved: {name}')

# ── Data ──────────────────────────────────────────────────────
con = duckdb.connect('db/signal_mind.duckdb', read_only=True)
rows = con.execute("""
    SELECT DATE_TRUNC('month', d.trade_date)::DATE,
           ROUND(AVG(d.close),2), ROUND(AVG(s.moexfn_finance),1),
           ROUND(AVG(m.usd_rub),2), ROUND(AVG(m.key_rate_pct),2)
    FROM market_data d
    JOIN v_moex_sectors s ON s.trade_date = d.trade_date
    JOIN v_market_context m ON m.trade_date = d.trade_date
    WHERE d.instrument = 'BRENT'
      AND d.trade_date BETWEEN '2022-01-01' AND '2025-12-31'
      AND d.close IS NOT NULL AND s.moexfn_finance IS NOT NULL
    GROUP BY 1 ORDER BY 1
""").fetchall()
con.close()

months   = [r[0] for r in rows]
brent    = np.array([r[1] for r in rows], dtype=float)
moexfn   = np.array([r[2] for r in rows], dtype=float)
usd_rub  = np.array([r[3] for r in rows], dtype=float)
key_rate = np.array([r[4] for r in rows], dtype=float)
xs = np.arange(len(months))
xl = [str(m)[:7] for m in months]

# ── Chart 1: Main dual-axis ────────────────────────────────────
fig = plt.figure(figsize=(14, 6), facecolor=BG)
ax1 = darkax(fig.add_subplot(111))
ax2 = ax1.twinx()
ax2.set_facecolor(BG2)
ax2.tick_params(colors=DIM, labelsize=9)
for sp in ax2.spines.values(): sp.set_color(GRID)

ax1.fill_between(xs, brent, alpha=0.12, color=BLUE)
l1, = ax1.plot(xs, brent, color=BLUE, lw=2.2, label='Brent ($/барр)')
ax2.fill_between(xs, moexfn, alpha=0.08, color=GREEN)
l2, = ax2.plot(xs, moexfn, color=GREEN, lw=2.2, label='MOEXFN', linestyle='--')

pi = int(np.argmax(brent))
ax1.annotate(f'Пик Brent\n${brent[pi]:.0f}', xy=(pi, brent[pi]),
    xytext=(pi-3, brent[pi]+9), color=AMBER, fontsize=8, ha='center',
    arrowprops=dict(arrowstyle='->', color=AMBER, lw=1))
ax1.axvline(x=18, color=PURPLE, lw=1, alpha=0.6, linestyle=':')
ax1.text(18.3, 63, 'ЦБ 7.5→13%', color=PURPLE, fontsize=8, rotation=90, va='bottom')
ax1.axvline(x=33, color=RED, lw=1, alpha=0.6, linestyle=':')
ax1.text(33.3, 63, 'ЦБ 21%', color=RED, fontsize=8, rotation=90, va='bottom')

ax1.set_xticks(xs[::4]); ax1.set_xticklabels([xl[i] for i in range(0,len(xl),4)], rotation=30, ha='right')
ax1.set_ylabel('Brent, $/барр', color=BLUE)
ax2.set_ylabel('MOEXFN', color=GREEN)
ax1.set_title('Brent и MOEXFN (финансовый сектор) 2022–2025', color=TEXT, fontsize=13, pad=12)
fig.legend([l1,l2], ['Brent ($/барр)','MOEXFN'], loc='upper right',
    bbox_to_anchor=(0.97,0.93), facecolor=BG3, edgecolor=GRID, labelcolor=TEXT, fontsize=9)
fig.tight_layout(); save(fig, 'chart1_main.png')

# ── Chart 2: Lags bar ──────────────────────────────────────────
lags = [0, 7, 14, 30, 60, 90, 120]
rs   = [0.5649, 0.5849, 0.6010, 0.6387, 0.6798, 0.7021, 0.7161]
fig = plt.figure(figsize=(9, 4), facecolor=BG)
ax = darkax(fig.add_subplot(111))
clrs = [BLUE]*5 + [RED, RED]
bars = ax.bar([f'{l}д' for l in lags], rs, color=clrs, edgecolor=GRID, linewidth=0.5)
for bar, r in zip(bars, rs):
    ax.text(bar.get_x()+bar.get_width()/2, r+0.003, f'{r:.4f}',
        ha='center', va='bottom', fontsize=9, color=TEXT)
ax.set_ylim(0.45, 0.77); ax.set_ylabel('|r| Пирсона', color=DIM)
ax.set_title('|r| Brent→MOEXFN по лагам — сигнал усиливается с горизонтом', color=TEXT, fontsize=12, pad=10)
ax.axhline(0.7, color=RED, lw=1, linestyle='--', alpha=0.6)
ax.text(5.6, 0.703, 'r=0.70', color=RED, fontsize=8)
fig.tight_layout(); save(fig, 'chart2_lags.png')

# ── Chart 3: Ruble + Rate ──────────────────────────────────────
fig = plt.figure(figsize=(14, 5), facecolor=BG)
gs = gridspec.GridSpec(1, 2, figure=fig, wspace=0.3)

ax_r = darkax(fig.add_subplot(gs[0]))
ax_r.fill_between(xs, usd_rub, alpha=0.15, color=AMBER)
ax_r.plot(xs, usd_rub, color=AMBER, lw=2)
ax_r.axhline(80, color=RED, lw=1, linestyle='--', alpha=0.7)
ax_r.fill_between(xs, usd_rub, 80, where=(usd_rub<80), alpha=0.18, color=GREEN, label='Сильный рубль (<80)')
ax_r.fill_between(xs, usd_rub, 80, where=(usd_rub>=80), alpha=0.12, color=RED,   label='Слабый рубль (≥80)')
ax_r.set_xticks(xs[::4]); ax_r.set_xticklabels([xl[i] for i in range(0,len(xl),4)], rotation=30, ha='right')
ax_r.set_ylabel('USD/RUB', color=AMBER)
ax_r.set_title('Курс рубля — связующее звено', color=TEXT, fontsize=11, pad=8)
ax_r.legend(facecolor=BG3, edgecolor=GRID, labelcolor=TEXT, fontsize=8, loc='upper left')

ax_k = darkax(fig.add_subplot(gs[1]))
ax_k.fill_between(xs, key_rate, alpha=0.15, color=PURPLE)
ax_k.step(xs, key_rate, color=PURPLE, lw=2, where='post')
ax_k.axhline(16, color=RED, lw=1, linestyle='--', alpha=0.5)
ax_k.text(0.5, 16.5, 'Порог: ≥16% — сигнал слабеет', color=RED, fontsize=8)
ax_k.set_xticks(xs[::4]); ax_k.set_xticklabels([xl[i] for i in range(0,len(xl),4)], rotation=30, ha='right')
ax_k.set_ylabel('Ставка, %', color=PURPLE)
ax_k.set_title('Ключевая ставка ЦБ РФ', color=TEXT, fontsize=11, pad=8)
ax_k.yaxis.set_major_formatter(plt.FuncFormatter(lambda x,_: f'{int(x)}%'))
fig.suptitle('Переменные механизма: рубль и ключевая ставка', color=TEXT, fontsize=12, y=1.01)
fig.tight_layout(); save(fig, 'chart3_ruble_rate.png')

# ── Chart 4: Yearly + Rolling quarterly ───────────────────────
fig = plt.figure(figsize=(14, 5), facecolor=BG)
gs = gridspec.GridSpec(1, 2, figure=fig, wspace=0.35)

ax_y = darkax(fig.add_subplot(gs[0]))
years  = ['2022','2023','2024','2025']
yearly = [-0.574,-0.113,-0.097,0.265]
yc = [RED if v<-0.3 else (DIM if v<0 else GREEN) for v in yearly]
brs = ax_y.bar(years, yearly, color=yc, edgecolor=GRID, linewidth=0.5, width=0.5)
for b, v in zip(brs, yearly):
    ax_y.text(b.get_x()+b.get_width()/2, v+(0.01 if v>=0 else -0.045),
        f'{v:+.3f}', ha='center', fontsize=9, color=TEXT)
ax_y.axhline(0, color=GRID, lw=1)
ax_y.set_ylim(-0.72, 0.45); ax_y.set_ylabel('r Пирсона', color=DIM)
ax_y.set_title('r по годам (lag=90д)', color=TEXT, fontsize=11, pad=8)

ax_q = darkax(fig.add_subplot(gs[1]))
qtrs  = ['22Q1','22Q2','22Q3','22Q4','23Q1','23Q2','23Q3','23Q4',
         '24Q1','24Q2','24Q3','24Q4','25Q1','25Q2','25Q3','25Q4']
rolling = [-0.6827,0.7018,-0.6997,-0.6184,-0.4443,-0.8031,-0.6082,-0.4149,
           -0.5733,0.728,0.4742,-0.501,0.0743,-0.2529,-0.4473,-0.1433]
qx = np.arange(len(qtrs))
qc = [RED+'bb' if v<0 else GREEN+'bb' for v in rolling]
ax_q.bar(qx, rolling, color=qc, edgecolor=GRID, linewidth=0.3, width=0.7)
ax_q.axhline(0, color=GRID, lw=1)
ax_q.set_xticks(qx[::2]); ax_q.set_xticklabels(qtrs[::2], rotation=30, ha='right', fontsize=8)
ax_q.set_ylim(-1.0, 0.9); ax_q.set_ylabel('r Пирсона', color=DIM)
ax_q.set_title('Квартальная корреляция (скользящая)', color=TEXT, fontsize=11, pad=8)
p1 = mpatches.Patch(color=RED+'bb', label='Сигнал активен (r<0)')
p2 = mpatches.Patch(color=GREEN+'bb', label='Сигнал инвертирован (r>0)')
ax_q.legend(handles=[p1,p2], facecolor=BG3, edgecolor=GRID, labelcolor=TEXT, fontsize=7.5, loc='lower right')
fig.suptitle('Нестабильность сигнала: знак меняется', color=TEXT, fontsize=12, y=1.01)
fig.tight_layout(); save(fig, 'chart4_stability.png')

# ── Chart 5: Scatter ───────────────────────────────────────────
fig = plt.figure(figsize=(9, 6), facecolor=BG)
ax = darkax(fig.add_subplot(111))
year_colors = {2022: RED, 2023: BLUE, 2024: AMBER, 2025: GREEN}
for m, b, fn in zip(months, brent, moexfn):
    ax.scatter(b, fn, color=year_colors.get(m.year, DIM), alpha=0.78, s=60, edgecolors='none')
coeffs = np.polyfit(brent, moexfn, 1)
xl2 = np.linspace(brent.min()-5, brent.max()+5, 100)
ax.plot(xl2, np.polyval(coeffs, xl2), color=DIM, lw=1.5, linestyle='--', alpha=0.7)
legend_h = [mpatches.Patch(color=c, label=str(y)) for y,c in year_colors.items()]
legend_h.append(Line2D([0],[0], color=DIM, linestyle='--', label='Тренд'))
ax.legend(handles=legend_h, facecolor=BG3, edgecolor=GRID, labelcolor=TEXT,
    fontsize=9, title='Год', title_fontsize=9, loc='upper right')
ax.set_xlabel('Brent, $/барр'); ax.set_ylabel('MOEXFN')
ax.set_title('Scatter: Brent vs MOEXFN — месячные средние 2022–2025\nr = −0.70  |  45 точек',
    color=TEXT, fontsize=11, pad=10)
fig.tight_layout(); save(fig, 'chart5_scatter.png')

# ── Chart 6: Regime horizontal bars ───────────────────────────
fig = plt.figure(figsize=(11, 5), facecolor=BG)
ax = darkax(fig.add_subplot(111))
labels6 = ['Весь период 2022–2025\n(n=799)',
           'Сильный рубль (USD/RUB<80)\n(n=302)',
           'Слабый рубль (USD/RUB≥80)\n(n=497)',
           '2022 год отдельно\n(n=199)',
           '2023–2024 (ставка ≥16%)\n(n≈400)']
vals6 = [-0.702, -0.851, -0.524, -0.574, -0.100]
clrs6 = [BLUE, RED, AMBER, RED, DIM]
brs = ax.barh(range(len(labels6)), vals6, color=clrs6, edgecolor=GRID, linewidth=0.4, height=0.55)
ax.axvline(0, color=GRID, lw=1)
ax.axvline(-0.5, color=GRID, lw=0.5, linestyle=':', alpha=0.4)
ax.set_yticks(range(len(labels6))); ax.set_yticklabels(labels6, fontsize=9)
ax.set_xlim(-1.0, 0.2)
ax.set_xlabel('r Пирсона (lag=90д)', color=DIM)
ax.set_title('Режимный анализ: сигнал работает по-разному', color=TEXT, fontsize=12, pad=10)
for i, v in enumerate(vals6):
    ax.text(v-0.025, i, f'{v:+.3f}', ha='right', va='center', fontsize=9, color=TEXT, fontweight='bold')
fig.tight_layout(); save(fig, 'chart6_regime.png')

print('\nAll 6 charts generated successfully.')
