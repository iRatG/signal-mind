"""Architecture diagram: data sources → databases → agent → output.
RAG = ChromaDB (PDFs only). News = SQLite → news_daily in DuckDB (SQL, not RAG).
"""
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch

BG     = '#0d1117'
BG2    = '#161b22'
BORDER = '#30363d'
BLUE   = '#4f8ef7'
GREEN  = '#6ee7b7'
AMBER  = '#f59e0b'
RED    = '#f87171'
PURPLE = '#a78bfa'
TEAL   = '#2dd4bf'
DIM    = '#6b7280'
TEXT   = '#e2e8f0'
ACCENT = '#58a6ff'

fig, ax = plt.subplots(figsize=(15, 10))
fig.patch.set_facecolor(BG)
ax.set_facecolor(BG)
ax.set_xlim(0, 15)
ax.set_ylim(0, 10)
ax.axis('off')

def box(x, y, w, h, color, alpha=0.15, border=None, lw=1.2, radius=0.2):
    b = border or color
    r = FancyBboxPatch((x, y), w, h,
        boxstyle=f"round,pad=0,rounding_size={radius}",
        facecolor=color, alpha=alpha, edgecolor=b, linewidth=lw)
    ax.add_patch(r)

def txt(x, y, s, size=9, color=TEXT, bold=False, ha='center', va='center', alpha=1.0):
    ax.text(x, y, s, fontsize=size, color=color, ha=ha, va=va,
            fontweight='bold' if bold else 'normal', alpha=alpha)

def arr(x1, y1, x2, y2, color=DIM, lw=1.3, alpha=0.65):
    ax.annotate('', xy=(x2, y2), xytext=(x1, y1),
        arrowprops=dict(arrowstyle='->', color=color, lw=lw, alpha=alpha,
                        connectionstyle='arc3,rad=0.0'))

# ─── section header ───────────────────────────────────────────────
def section(y, label):
    ax.axhline(y + 0.02, color=BORDER, lw=0.6, alpha=0.5, xmin=0.02, xmax=0.98)
    txt(7.5, y + 0.22, label, 7.5, DIM, alpha=0.75)

# ══════════════════════════════════════════════════════════════════
# ROW 1  —  источники данных
# ══════════════════════════════════════════════════════════════════
section(9.05, 'ИСТОЧНИКИ ДАННЫХ')

src = [
    # x     y     w    h    col     title               sub
    (0.2,  8.25, 2.1, 0.7, BLUE,  'MOEX',             '15 индексов · 2016–2026'),
    (2.5,  8.25, 2.1, 0.7, BLUE,  'ЦБ РФ',            'ставка · USD/RUB'),
    (4.8,  8.25, 2.1, 0.7, BLUE,  'Росстат + Global', 'макро · Brent · SP500…'),
    (7.2,  8.25, 2.8, 0.7, AMBER, 'HuggingFace News', '2.52M статей · 9.93 GB'),
    (10.2, 8.25, 4.6, 0.7, PURPLE,'PDF-отчётность',
     'ЦБ · Сбер · Яндекс · Норникель · Газпром · Райфф'),
]
for x, y, w, h, col, title, sub in src:
    box(x, y, w, h, col, alpha=0.13, border=col)
    cx = x + w/2
    txt(cx, y + h*0.67, title, 8.5, col, bold=True)
    txt(cx, y + h*0.28, sub,   7.0, TEXT, alpha=0.65)

# ══════════════════════════════════════════════════════════════════
# Arrows: sources → databases
# ══════════════════════════════════════════════════════════════════
# MOEX + CBR + Rosstat/Global → DuckDB  (синие)
for sx in [1.25, 3.55, 5.85]:
    arr(sx, 8.25, 3.0, 7.3, BLUE, lw=0.9, alpha=0.45)

# HF News → SQLite  (янтарный)
arr(8.6, 8.25, 8.6, 7.3, AMBER, lw=1.1, alpha=0.55)

# PDFs → ChromaDB  (фиолетовый)
arr(12.5, 8.25, 12.5, 7.3, PURPLE, lw=1.1, alpha=0.55)

# ══════════════════════════════════════════════════════════════════
# ROW 2  —  хранилища
# ══════════════════════════════════════════════════════════════════
section(7.7, 'ХРАНИЛИЩА')

# DuckDB
box(0.2, 6.45, 5.6, 1.0, BLUE, alpha=0.17, border=BLUE)
txt(3.0, 7.18, 'DuckDB', 11, BLUE, bold=True)
txt(3.0, 6.82, 'MOEX · ЦБ РФ · Росстат · Brent/SP500/Gold · news_daily', 7.5, TEXT, alpha=0.75)
txt(3.0, 6.58, 'SQL · 40 мс · аналитика без сервера', 7.0, DIM, alpha=0.65)

# SQLite
box(6.1,  6.45, 4.8, 1.0, AMBER, alpha=0.17, border=AMBER)
txt(8.5,  7.18, 'SQLite  hf_news.db', 11, AMBER, bold=True)
txt(8.5,  6.82, '2 520 591 статья · 9.93 GB · только чтение', 7.5, TEXT, alpha=0.75)
txt(8.5,  6.58, 'pre-computed → news_daily в DuckDB', 7.0, DIM, alpha=0.65)

# ChromaDB
box(11.1, 6.45, 3.7, 1.0, PURPLE, alpha=0.17, border=PURPLE)
txt(12.95, 7.18, 'ChromaDB  RAG', 11, PURPLE, bold=True)
txt(12.95, 6.82, '17 492 чанков · MiniLM-L12-v2', 7.5, TEXT, alpha=0.75)
txt(12.95, 6.58, 'ЦБ · Сбер · Яндекс · Норникель…', 7.0, DIM, alpha=0.65)

# ══════════════════════════════════════════════════════════════════
# Arrows: databases → agent   (с подписями)
# ══════════════════════════════════════════════════════════════════
arr(3.0,  6.45, 5.3, 5.3, BLUE,   lw=1.4, alpha=0.65)
arr(8.5,  6.45, 7.5, 5.3, AMBER,  lw=1.4, alpha=0.65)
arr(12.95,6.45, 9.7, 5.3, PURPLE, lw=1.4, alpha=0.65)

txt(3.7,  5.95, 'SQL · 40мс', 7.5, BLUE,   alpha=0.7)
txt(7.4,  5.85, 'news_daily\n25мс · SQL', 7.0, AMBER,  alpha=0.7)
# RAG label — справа
box(10.5, 5.55, 2.2, 0.55, PURPLE, alpha=0.13, border=PURPLE, lw=1.0, radius=0.12)
txt(11.6, 5.82, 'RAG · 65мс', 8, PURPLE, bold=False, alpha=0.9)
txt(11.6, 5.65, 'семантический поиск', 6.5, PURPLE, alpha=0.65)
arr(11.6, 5.55, 9.7, 5.3, PURPLE, lw=1.0, alpha=0.5)

# ══════════════════════════════════════════════════════════════════
# ROW 3  —  Ouroboros агент
# ══════════════════════════════════════════════════════════════════
section(5.6, 'OUROBOROS АГЕНТ')

box(3.0, 4.0, 9.0, 1.35, GREEN, alpha=0.12, border=GREEN, lw=1.6, radius=0.28)
txt(7.5, 5.02, 'OUROBOROS АГЕНТ', 14, GREEN, bold=True)
txt(7.5, 4.62, 'гипотеза  →  SQL  →  DuckDB  →  оценка  →  новая гипотеза', 9, TEXT, alpha=0.85)
txt(7.5, 4.25, 'DeepSeek API  ·  LAG_SWEEP [0, 7, 14, 30, 60, 90]  ·  22 темы  ·  RANDOM_JUMP 25%', 7.5, DIM, alpha=0.7)

# SQL self-repair — малый бейдж внутри
box(9.6, 4.15, 2.2, 0.72, RED, alpha=0.18, border=RED, radius=0.12)
txt(10.7, 4.56, 'SQL self-repair', 8, RED, bold=True)
txt(10.7, 4.3,  '8 типов · 22 события · 0 ошибок', 6.5, RED, alpha=0.8)

# ══════════════════════════════════════════════════════════════════
# Arrows: agent → outputs
# ══════════════════════════════════════════════════════════════════
arr(5.0, 4.0, 3.0, 2.95, GREEN, lw=1.3, alpha=0.6)
arr(7.5, 4.0, 7.5, 2.95, GREEN, lw=1.3, alpha=0.6)
arr(10.0,4.0, 12.0,2.95, GREEN, lw=1.3, alpha=0.6)

# ══════════════════════════════════════════════════════════════════
# ROW 4  —  выходные данные
# ══════════════════════════════════════════════════════════════════
section(3.2, 'ВЫХОДНЫЕ ДАННЫЕ')

outs = [
    (0.5,  1.5, 4.5, 1.3, AMBER,  'signals.jsonl',   'лог всех сигналов сессии'),
    (5.25, 1.5, 4.5, 1.3, TEAL,   'knowledge.md',    'подтверждённые паттерны'),
    (10.0, 1.5, 4.5, 1.3, PURPLE, 'experiments.db',  'fine-tuning датасет\n1 953 строки · Datasets A/B/C'),
]
for x, y, w, h, col, title, sub in outs:
    box(x, y, w, h, col, alpha=0.15, border=col)
    cx = x + w/2
    txt(cx, y + h*0.7, title, 10, col, bold=True)
    txt(cx, y + h*0.32, sub,  7.5, TEXT, alpha=0.72)

# ══════════════════════════════════════════════════════════════════
# Title + footer
# ══════════════════════════════════════════════════════════════════
txt(7.5, 0.9, 'Signal Mind — архитектура системы', 12, ACCENT, bold=True, alpha=0.95)
txt(7.5, 0.55,
    'Python · DuckDB · SQLite (новости) · ChromaDB RAG (PDF) · DeepSeek API',
    8, DIM, alpha=0.7)

plt.tight_layout(pad=0.2)
out = 'habr/sample/charts/architecture.png'
plt.savefig(out, dpi=150, bbox_inches='tight', facecolor=BG)
plt.close()
print(f'Saved: {out}')
