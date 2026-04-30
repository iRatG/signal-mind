"""Conceptual architecture diagram — big labels, no details."""
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
import matplotlib.patheffects as pe

BG     = '#0d1117'
BLUE   = '#4f8ef7'
GREEN  = '#6ee7b7'
AMBER  = '#f59e0b'
PURPLE = '#a78bfa'
TEAL   = '#2dd4bf'
RED    = '#f87171'
DIM    = '#4b5563'
TEXT   = '#e2e8f0'
ACCENT = '#58a6ff'

fig, ax = plt.subplots(figsize=(13, 10))
fig.patch.set_facecolor(BG)
ax.set_facecolor(BG)
ax.set_xlim(0, 13)
ax.set_ylim(0, 10)
ax.axis('off')

def box(x, y, w, h, col, alpha=0.18, lw=1.6, radius=0.3):
    r = FancyBboxPatch((x, y), w, h,
        boxstyle=f"round,pad=0,rounding_size={radius}",
        facecolor=col, alpha=alpha, edgecolor=col, linewidth=lw)
    ax.add_patch(r)

def label(x, y, top, bot=None, col=TEXT, top_size=13, bot_size=9):
    ax.text(x, y + (0.18 if bot else 0), top,
            fontsize=top_size, color=col, ha='center', va='center',
            fontweight='bold')
    if bot:
        ax.text(x, y - 0.22, bot,
                fontsize=bot_size, color=col, ha='center', va='center',
                alpha=0.65)

def arr(x1, y1, x2, y2, col=DIM, lw=2.0, alpha=0.7):
    ax.annotate('', xy=(x2, y2), xytext=(x1, y1),
        arrowprops=dict(arrowstyle='->', color=col, lw=lw,
                        alpha=alpha, connectionstyle='arc3,rad=0.0',
                        mutation_scale=16))

def dim_line(y):
    ax.axhline(y, color=DIM, lw=0.5, alpha=0.3, xmin=0.03, xmax=0.97)

# ═══════════════════════════════════════════════════════════
# ROW 1 — источники (4 блока)
# ═══════════════════════════════════════════════════════════
ax.text(6.5, 9.7, 'Д А Н Н Ы Е', fontsize=9, color=DIM,
        ha='center', va='center', alpha=0.8, fontweight='bold')

src = [
    (0.2,  8.55, 2.8, 0.95, BLUE,   'БИРЖА',     'MOEX · ЦБ · Росстат'),
    (3.25, 8.55, 2.8, 0.95, BLUE,   'МИРОВЫЕ',   'Brent · SP500 · Gold…'),
    (6.3,  8.55, 2.8, 0.95, AMBER,  'НОВОСТИ',   '2.5M статей · SQLite'),
    (9.35, 8.55, 3.45,0.95, PURPLE, 'ЗНАНИЯ',    'PDF · Сбер · ЦБ · Яндекс…'),
]
for x, y, w, h, col, top, bot in src:
    box(x, y, w, h, col, alpha=0.15)
    label(x + w/2, y + h/2, top, bot, col, top_size=13, bot_size=8)

# ═══════════════════════════════════════════════════════════
# Arrows: sources → storage
# ═══════════════════════════════════════════════════════════
arr(1.6,  8.55, 2.3,  7.45, BLUE,  lw=1.5, alpha=0.5)
arr(4.65, 8.55, 2.9,  7.45, BLUE,  lw=1.5, alpha=0.5)
arr(7.7,  8.55, 7.3,  7.45, AMBER, lw=1.5, alpha=0.55)
arr(11.1, 8.55, 11.1, 7.45, PURPLE,lw=1.5, alpha=0.55)

# ═══════════════════════════════════════════════════════════
# ROW 2 — хранилища (3 блока)
# ═══════════════════════════════════════════════════════════
dim_line(8.3)
ax.text(6.5, 8.2, 'ХРАНИЛИЩА', fontsize=8, color=DIM,
        ha='center', va='center', alpha=0.8)

stores = [
    (0.2,  6.55, 4.6, 0.95, BLUE,   'DuckDB',    'числа · SQL · 40мс'),
    (5.1,  6.55, 3.8, 0.95, AMBER,  'SQLite',    'новости · только чтение'),
    (9.15, 6.55, 3.65,0.95, PURPLE, 'ChromaDB',  'RAG · PDF · 65мс'),
]
for x, y, w, h, col, top, bot in stores:
    box(x, y, w, h, col, alpha=0.2, lw=1.8)
    label(x + w/2, y + h/2, top, bot, col, top_size=14, bot_size=8.5)

# news_daily arrow  (SQLite → DuckDB pre-compute)
ax.annotate('', xy=(3.5, 7.03), xytext=(5.5, 7.03),
    arrowprops=dict(arrowstyle='->', color=AMBER, lw=1.2,
                    alpha=0.55, connectionstyle='arc3,rad=-0.3'))
ax.text(4.5, 7.38, 'news_daily', fontsize=7.5, color=AMBER,
        ha='center', alpha=0.65, style='italic')

# ═══════════════════════════════════════════════════════════
# Arrows: storage → agent
# ═══════════════════════════════════════════════════════════
arr(2.5,  6.55, 4.8,  5.45, BLUE,   lw=1.8, alpha=0.65)
arr(7.0,  6.55, 6.6,  5.45, AMBER,  lw=1.4, alpha=0.5)
arr(11.0, 6.55, 8.2,  5.45, PURPLE, lw=1.8, alpha=0.65)

ax.text(7.5, 6.1, 'RAG', fontsize=9, color=PURPLE,
        ha='center', alpha=0.8, fontweight='bold')

# ═══════════════════════════════════════════════════════════
# ROW 3 — УРОБОРОС агент  (центральный, крупный)
# ═══════════════════════════════════════════════════════════
dim_line(6.3)
ax.text(6.5, 6.2, 'АГЕНТ', fontsize=8, color=DIM,
        ha='center', va='center', alpha=0.8)

box(2.0, 3.9, 9.0, 1.55, GREEN, alpha=0.14, lw=2.2, radius=0.35)
ax.text(6.5, 5.08, 'УРОБОРОС', fontsize=20, color=GREEN,
        ha='center', va='center', fontweight='bold')
ax.text(6.5, 4.52, 'гипотеза  →  SQL  →  данные  →  оценка  →  новая гипотеза',
        fontsize=10, color=TEXT, ha='center', va='center', alpha=0.8)
ax.text(6.5, 4.12, 'DeepSeek API  ·  лаги 0–90д  ·  22 темы',
        fontsize=8.5, color=DIM, ha='center', va='center', alpha=0.75)

# SQL self-repair badge
box(9.3, 4.05, 1.55, 0.75, RED, alpha=0.2, lw=1.3, radius=0.15)
ax.text(10.08, 4.43, 'REPAIR', fontsize=9, color=RED,
        ha='center', va='center', fontweight='bold')
ax.text(10.08, 4.18, '22/0', fontsize=8, color=RED,
        ha='center', va='center', alpha=0.8)

# Cycle arrow (self-loop on agent) — показывает петлю Уробороса
theta = [i * 3.14159 / 30 for i in range(0, 31)]
import math
cx, cy, rx, ry = 6.5, 4.72, 1.2, 0.35
xs = [cx + rx * math.cos(t) for t in theta]
ys = [cy + ry * math.sin(t) + 0.55 for t in theta]
ax.plot(xs[:25], ys[:25], color=GREEN, lw=1.2, alpha=0.3, linestyle='--')

# ═══════════════════════════════════════════════════════════
# Arrows: agent → outputs
# ═══════════════════════════════════════════════════════════
arr(4.2,  3.9,  2.7,  2.85, GREEN, lw=1.6, alpha=0.6)
arr(6.5,  3.9,  6.5,  2.85, GREEN, lw=1.6, alpha=0.6)
arr(8.8,  3.9,  10.3, 2.85, GREEN, lw=1.6, alpha=0.6)

# ═══════════════════════════════════════════════════════════
# ROW 4 — результаты
# ═══════════════════════════════════════════════════════════
dim_line(3.7)
ax.text(6.5, 3.58, 'РЕЗУЛЬТАТЫ', fontsize=8, color=DIM,
        ha='center', va='center', alpha=0.8)

outs = [
    (0.5,  1.7,  3.8, 1.1, AMBER,  'СИГНАЛЫ',   'signals.jsonl'),
    (4.6,  1.7,  3.8, 1.1, TEAL,   'ПАМЯТЬ',    'knowledge.md'),
    (8.7,  1.7,  3.8, 1.1, PURPLE, 'ДАТАСЕТ',   'experiments.db\n1 953 строки'),
]
for x, y, w, h, col, top, bot in outs:
    box(x, y, w, h, col, alpha=0.18, lw=1.8)
    label(x + w/2, y + h/2, top, bot, col, top_size=14, bot_size=8.5)

# ═══════════════════════════════════════════════════════════
# Footer
# ═══════════════════════════════════════════════════════════
ax.text(6.5, 1.0, 'Signal Mind — архитектура системы',
        fontsize=10, color=ACCENT, ha='center', fontweight='bold', alpha=0.9)
ax.text(6.5, 0.62, 'Python · DuckDB · SQLite · ChromaDB RAG · DeepSeek',
        fontsize=8, color=DIM, ha='center', alpha=0.65)

plt.tight_layout(pad=0.1)
plt.savefig('habr/sample/charts/architecture.png',
            dpi=150, bbox_inches='tight', facecolor=BG)
plt.close()
print('Done: habr/sample/charts/architecture.png')
