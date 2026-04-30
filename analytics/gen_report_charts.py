"""Generate static PNG charts for marathon report."""
import re
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
from pathlib import Path

BG    = '#0d1117'
BG2   = '#161b22'
GRID  = '#21262d'
BLUE  = '#58a6ff'
GREEN = '#3fb950'
RED   = '#f85149'
AMBER = '#d29922'
DIM   = '#8b949e'

Path('analytics/marathon_charts').mkdir(exist_ok=True)

with open('analytics/report.html', 'r', encoding='utf-8') as f:
    html = f.read()

def setup(fig, ax):
    fig.patch.set_facecolor(BG)
    if isinstance(ax, (list, np.ndarray)):
        for a in np.array(ax).flat:
            a.set_facecolor(BG2)
            a.tick_params(colors=DIM)
            a.xaxis.label.set_color(DIM)
            a.yaxis.label.set_color(DIM)
            for spine in a.spines.values():
                spine.set_edgecolor(GRID)
            a.grid(color=GRID, linewidth=0.5)
    else:
        ax.set_facecolor(BG2)
        ax.tick_params(colors=DIM)
        ax.xaxis.label.set_color(DIM)
        ax.yaxis.label.set_color(DIM)
        for spine in ax.spines.values():
            spine.set_edgecolor(GRID)
        ax.grid(color=GRID, linewidth=0.5)

# --- Chart 1: Outcomes stacked bar ---
m = re.search(r"label: 'Confirmed', data: \[([\d, ]+)\]", html)
confirmed = list(map(int, m.group(1).split(', ')))
m = re.search(r"label: 'Partial',   data: \[([\d, ]+)\]", html)
partial = list(map(int, m.group(1).split(', ')))
m = re.search(r"label: 'Rejected',  data: \[([\d, ]+)\]", html)
rejected = list(map(int, m.group(1).split(', ')))

labels_out = [f'{i*50+1}–{(i+1)*50}' if (i+1)*50 <= 1943 else f'{i*50+1}–1943'
              for i in range(len(confirmed))]
x = np.arange(len(confirmed))

fig, ax = plt.subplots(figsize=(12, 4))
setup(fig, ax)
ax.bar(x, confirmed, label='Confirmed', color='#1f6335')
ax.bar(x, partial,   bottom=confirmed, label='Partial', color='#7d4e05')
bottom2 = [c+p for c,p in zip(confirmed, partial)]
ax.bar(x, rejected,  bottom=bottom2,   label='Rejected', color='#4a1a1a')
ax.set_xticks(x[::4])
ax.set_xticklabels(labels_out[::4], rotation=45, ha='right', fontsize=7, color=DIM)
ax.set_title('Итерации: confirmed / partial / rejected', color='#c9d1d9', fontsize=11, pad=10)
legend = ax.legend(facecolor=BG2, edgecolor=GRID, labelcolor='#c9d1d9', fontsize=8)
plt.tight_layout()
plt.savefig('analytics/marathon_charts/chart1_outcomes.png', dpi=120, bbox_inches='tight', facecolor=BG)
plt.close()
print('chart1_outcomes.png ✓')

# --- Chart 2: Rolling confirmation rate ---
m = re.search(r"label: 'Confirmation rate % \(30-iter window\)'.*?data: \[([0-9., \n]+)\]",
              html, re.DOTALL)
rolling = list(map(float, re.sub(r'\s+', ' ', m.group(1)).strip().split(', ')))
iters = list(range(30, 30 + len(rolling)))
# Downsample to every 5th for cleaner chart
step = 5
r_x = iters[::step]
r_y = rolling[::step]

fig, ax = plt.subplots(figsize=(12, 3.5))
setup(fig, ax)
ax.fill_between(r_x, r_y, alpha=0.12, color=GREEN)
ax.plot(r_x, r_y, color=GREEN, linewidth=1.5)
ax.axhline(67.7, color=RED, linewidth=1, linestyle='--', alpha=0.6, label='Заявленный 67.7%')
ax.axhline(20, color=AMBER, linewidth=1, linestyle='--', alpha=0.6, label='Реальный ~20%')
ax.set_ylim(0, 100)
ax.set_xlabel('Итерация', color=DIM, fontsize=9)
ax.set_ylabel('Confirmation rate %', color=DIM, fontsize=9)
ax.set_title('Скользящий confirmed rate (окно 30 итераций)', color='#c9d1d9', fontsize=11, pad=10)
legend = ax.legend(facecolor=BG2, edgecolor=GRID, labelcolor='#c9d1d9', fontsize=8)
plt.tight_layout()
plt.savefig('analytics/marathon_charts/chart2_rolling.png', dpi=120, bbox_inches='tight', facecolor=BG)
plt.close()
print('chart2_rolling.png ✓')

# --- Chart 3: KPI summary as visual card ---
kpis = [
    ('1 943', 'Итераций'),
    ('409M', 'Токенов'),
    ('$5.76', 'Стоимость'),
    ('14.4с', 'Сред. время/итерация'),
    ('22/0', 'SQL repair: событий/ошибок'),
    ('0', 'Перезапусков watchdog'),
]
fig, axes = plt.subplots(1, 6, figsize=(14, 2.5))
setup(fig, axes)
colors = [BLUE, BLUE, GREEN, AMBER, GREEN, GREEN]
for ax, (val, lbl), col in zip(axes, kpis, colors):
    ax.text(0.5, 0.62, val, ha='center', va='center', fontsize=18, fontweight='bold',
            color=col, transform=ax.transAxes)
    ax.text(0.5, 0.25, lbl, ha='center', va='center', fontsize=7.5,
            color=DIM, transform=ax.transAxes)
    ax.set_xticks([]); ax.set_yticks([])
    ax.grid(False)
fig.suptitle('Ночь тишины — итоги марафона', color='#c9d1d9', fontsize=12, y=1.02)
plt.tight_layout()
plt.savefig('analytics/marathon_charts/chart3_kpi.png', dpi=120, bbox_inches='tight', facecolor=BG)
plt.close()
print('chart3_kpi.png ✓')

print('\nВсе графики марафона сгенерированы → analytics/marathon_charts/')
