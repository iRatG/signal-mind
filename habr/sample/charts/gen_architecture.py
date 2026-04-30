"""Architecture diagram: data sources → databases → agent → output."""
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
import matplotlib.patheffects as pe

# ── palette ──────────────────────────────────────────────────────
BG      = '#0d1117'
BG2     = '#161b22'
BG3     = '#1e2535'
BORDER  = '#30363d'
BLUE    = '#4f8ef7'
GREEN   = '#6ee7b7'
AMBER   = '#f59e0b'
RED     = '#f87171'
PURPLE  = '#a78bfa'
DIM     = '#6b7280'
TEXT    = '#e2e8f0'
ACCENT  = '#58a6ff'

fig, ax = plt.subplots(figsize=(14, 9))
fig.patch.set_facecolor(BG)
ax.set_facecolor(BG)
ax.set_xlim(0, 14)
ax.set_ylim(0, 9)
ax.axis('off')

def box(x, y, w, h, color, alpha=0.15, border=None, radius=0.18):
    border = border or color
    rect = FancyBboxPatch((x, y), w, h,
        boxstyle=f"round,pad=0,rounding_size={radius}",
        facecolor=color, alpha=alpha,
        edgecolor=border, linewidth=1.2)
    ax.add_patch(rect)

def label(x, y, text, size=9, color=TEXT, bold=False, ha='center', va='center', alpha=1.0):
    weight = 'bold' if bold else 'normal'
    ax.text(x, y, text, fontsize=size, color=color, ha=ha, va=va,
            fontweight=weight, alpha=alpha,
            fontfamily='DejaVu Sans')

def arrow(x1, y1, x2, y2, color=DIM, lw=1.2, style='->', alpha=0.7):
    ax.annotate('', xy=(x2, y2), xytext=(x1, y1),
        arrowprops=dict(arrowstyle=style, color=color,
                        lw=lw, alpha=alpha,
                        connectionstyle='arc3,rad=0.0'))

# ════════════════════════════════════════════════════════════════
# ROW 1 — Data sources
# ════════════════════════════════════════════════════════════════
label(7, 8.65, 'ИСТОЧНИКИ ДАННЫХ', 8, DIM, bold=False, alpha=0.8)

sources = [
    (0.3,  7.9, 1.8, 0.55, BLUE,   'MOEX\n15 индексов'),
    (2.25, 7.9, 1.8, 0.55, BLUE,   'ЦБ РФ\nставка, курс'),
    (4.2,  7.9, 1.8, 0.55, BLUE,   'Росстат\nмакро'),
    (6.15, 7.9, 1.8, 0.55, AMBER,  'Brent·SP500\nGold·DXY…'),
    (8.1,  7.9, 2.5, 0.55, GREEN,  'HuggingFace News\n2.52M статей'),
    (10.75,7.9, 2.95,0.55, PURPLE, 'PDF ЦБ + отчётность\n17 492 чанков'),
]
for x, y, w, h, col, txt in sources:
    box(x, y, w, h, col, alpha=0.12, border=col)
    cx, cy = x + w/2, y + h/2
    label(cx, cy, txt, 7.5, col, ha='center')

# ════════════════════════════════════════════════════════════════
# Arrows: sources → databases
# ════════════════════════════════════════════════════════════════
# MOEX + CBR + Rosstat + Global → DuckDB
for sx in [1.2, 3.15, 5.1, 7.05]:
    arrow(sx, 7.9, 2.8, 6.85, BLUE, lw=1.0, alpha=0.5)

# HF News → SQLite
arrow(9.35, 7.9, 9.35, 6.85, AMBER, lw=1.0, alpha=0.5)

# PDFs → ChromaDB
arrow(12.22, 7.9, 12.22, 6.85, PURPLE, lw=1.0, alpha=0.5)

# ════════════════════════════════════════════════════════════════
# ROW 2 — Databases
# ════════════════════════════════════════════════════════════════
label(7, 7.6, 'ХРАНИЛИЩА', 8, DIM, alpha=0.7)

# DuckDB
box(0.3, 6.0, 4.8, 0.8, BLUE, alpha=0.18, border=BLUE)
label(2.7, 6.62, 'DuckDB', 10, BLUE, bold=True)
label(2.7, 6.25, 'MOEX · ЦБ · Росстат · Глобальные рынки · news_daily', 7.5, TEXT, alpha=0.75)

# SQLite
box(5.4, 6.0, 3.2, 0.8, AMBER, alpha=0.18, border=AMBER)
label(7.0, 6.62, 'SQLite', 10, AMBER, bold=True)
label(7.0, 6.25, 'hf_news.db · 9.93 GB', 7.5, TEXT, alpha=0.75)

# ChromaDB
box(8.9, 6.0, 4.8, 0.8, PURPLE, alpha=0.18, border=PURPLE)
label(11.3, 6.62, 'ChromaDB', 10, PURPLE, bold=True)
label(11.3, 6.25, '17 492 чанков · MiniLM-L12-v2', 7.5, TEXT, alpha=0.75)

# ════════════════════════════════════════════════════════════════
# Arrows: databases → agent
# ════════════════════════════════════════════════════════════════
arrow(2.7,  6.0, 5.5, 4.9, BLUE,   lw=1.3, alpha=0.6)
arrow(7.0,  6.0, 6.8, 4.9, AMBER,  lw=1.3, alpha=0.6)
arrow(11.3, 6.0, 8.5, 4.9, PURPLE, lw=1.3, alpha=0.6)

# small labels on arrows
label(3.7, 5.55, 'SQL · 40ms', 7, BLUE, alpha=0.6)
label(6.8, 5.55, 'news_daily · 25ms', 7, AMBER, alpha=0.6)
label(10.4, 5.55, 'RAG · 65ms', 7, PURPLE, alpha=0.6)

# ════════════════════════════════════════════════════════════════
# ROW 3 — Ouroboros Agent (central, prominent)
# ════════════════════════════════════════════════════════════════
box(3.5, 3.6, 7.0, 1.25, GREEN, alpha=0.12, border=GREEN, radius=0.25)
label(7.0, 4.52, 'OUROBOROS АГЕНТ', 13, GREEN, bold=True)
label(7.0, 4.12, 'гипотеза  →  SQL  →  DuckDB  →  оценка  →  новая гипотеза', 8.5, TEXT, alpha=0.8)
label(7.0, 3.78, 'DeepSeek API  ·  LAG_SWEEP [0, 7, 14, 30, 60, 90]  ·  22 темы  ·  RANDOM_JUMP 25%', 7.5, DIM, alpha=0.7)

# inner loop label
label(7.0, 4.52, '', 8, GREEN)  # placeholder spacing

# SQL self-repair badge (small box inside)
box(10.2, 3.75, 2.1, 0.6, RED, alpha=0.15, border=RED, radius=0.12)
label(11.25, 4.05, 'SQL self-repair\n8 типов · 22/0', 7, RED, alpha=0.9)

# ════════════════════════════════════════════════════════════════
# Arrows: agent → output
# ════════════════════════════════════════════════════════════════
arrow(5.0, 3.6, 3.5, 2.65, GREEN, lw=1.3, alpha=0.6)
arrow(7.0, 3.6, 7.0, 2.65, GREEN, lw=1.3, alpha=0.6)
arrow(9.0, 3.6, 10.5,2.65, GREEN, lw=1.3, alpha=0.6)

# ════════════════════════════════════════════════════════════════
# ROW 4 — Outputs
# ════════════════════════════════════════════════════════════════
label(7, 3.35, 'ВЫХОДНЫЕ ДАННЫЕ', 8, DIM, alpha=0.7)

outputs = [
    (1.0,  1.55, 4.0, 1.0, AMBER,  'signals.jsonl',    'лог всех сигналов\nсессии'),
    (5.25, 1.55, 3.5, 1.0, BLUE,   'knowledge.md',     'подтверждённые\nпаттерны'),
    (9.0,  1.55, 4.0, 1.0, PURPLE, 'experiments.db',   'fine-tuning датасет\n1 953 строки'),
]
for x, y, w, h, col, title, sub in outputs:
    box(x, y, w, h, col, alpha=0.15, border=col)
    cx = x + w/2
    label(cx, y + h*0.65, title, 9.5, col, bold=True)
    label(cx, y + h*0.28, sub, 7.5, TEXT, alpha=0.7)

# ════════════════════════════════════════════════════════════════
# Title + footer
# ════════════════════════════════════════════════════════════════
label(7, 0.9, 'Signal Mind — архитектура системы', 11, ACCENT, bold=True, alpha=0.9)
label(7, 0.55, 'Python · DuckDB · ChromaDB · SQLite · DeepSeek API', 8, DIM, alpha=0.65)

plt.tight_layout(pad=0.3)
out = 'habr/sample/charts/architecture.png'
plt.savefig(out, dpi=150, bbox_inches='tight', facecolor=BG)
plt.close()
print(f'Saved: {out}')
