# Signal Mind — правила для Claude

## КРИТИЧЕСКИ ВАЖНО: защищённые файлы данных

### НИКОГДА не удалять, не перезаписывать, не очищать:

| Файл | Причина |
|------|---------|
| `db/hf_news.db` | МАСТЕР-ИСТОЧНИК новостей (1M+ статей, загрузка часы). НЕУДАЛЯЕМО. Только читать. |
| `db/signal_mind.duckdb` | Основная база: MOEX, ключевая ставка, форекс, макро. |
| `db/experiments.db` | Датасет fine-tuning — каждая строка = итерация агента. |
| `db/chroma/` | ChromaDB с 3158 чанков PDF ЦБ, встраивание займёт часы. |
| `db/signals.jsonl` | Лог всех сигналов агента. |
| `db/knowledge.md` | Накопленные знания из подтверждённых сигналов. |
| `db/sql_patterns.md` | Рабочие SQL-шаблоны. |
| `db/forbidden_patterns.md` | Задокументированные анти-паттерны. |

**Правило:** перед любой операцией с файлами в `db/` — спросить подтверждение у пользователя.  
**Правило:** `db/hf_news.db` — ТОЛЬКО ЧИТАТЬ. Никогда не удалять, не изменять, не очищать.  
**Архитектурное решение:** hf_news.db — исходные данные (source of truth). Для аналитики данные КОПИРУЮТСЯ в signal_mind.duckdb, оригинал остаётся нетронутым.

---

## КРИТИЧЕСКИ ВАЖНО: статические файлы Habr-статьи

На следующие файлы ссылается **опубликованная статья на habr.com** через хардкод raw.githubusercontent.com и htmlpreview.github.io URLs. Если файл переехал — ссылка в статье становится 404.

### НИКОГДА не переименовывать, не перемещать, не удалять:

| Файл / папка | Используется в |
|---|---|
| `habr/sample/charts/` (вся папка) | Статья Habr — все графики, диаграммы, GIF |
| `habr/sample/brent_moexfn_investigation.html` | Материалы статьи (htmlpreview) |
| `analytics/report_static.html` | Материалы статьи (htmlpreview) |
| `analytics/marathon_charts/chart1_outcomes.png` | Отчёт марафона |
| `analytics/marathon_charts/chart2_rolling.png` | Отчёт марафона |
| `analytics/marathon_charts/` (вся папка) | Все PNG графики марафона |

**Правило:** если задача затрагивает любой из этих путей — ОСТАНОВИТЬСЯ и предупредить пользователя до выполнения.

---

## Стиль работы

- Сначала думаем и планируем, код только после подтверждения пользователя.
- Не торопиться. Пользователь — экспериментатор, важна тщательность.
- Весь контекст сохранять в memory/ после каждой значимой сессии.

---

## Архитектура (кратко)

- **Агент:** `src/agent/agent.py` — Ouroboros loop, три петли обучения
- **Данные:** DuckDB (`db/signal_mind.duckdb`) + ChromaDB + SQLite новости
- **LLM:** DeepSeek API (`deepseek-chat`, env: `deep_seek_token`)
- **Запуск:** `.venv/Scripts/python -m src.agent.agent 3`

Полная архитектура: `memory/project_signal_mind.md`

---

## VPN-туннель для парсеров зарубежных источников

Текущий VPN рабочего компьютера режет соединения к западным новостным
сайтам (BBC/Guardian/Fox/AlJazeera/Euronews/France24): `ConnectionReset 10054`
ещё на TLS-хендшейке. Для обхода в проект встроен SSH SOCKS5-туннель через
выделенный сервер `37.233.83.68` (Ubuntu 24.04, root, «Sympathetic Rhea»).

- **Модуль:** `src/utils/proxy.py` — `vpn()`, `get_proxies()`, `start_vpn()`, `stop_vpn()`, `is_vpn_up()`.
- **Полная документация:** [vpn/README.md](vpn/README.md).
- **Конфиг:** ключи `vpn_*` в `.env`. SSH-ключ — в `~/.ssh/signal_mind_vpn` (не в репозитории).
- **Bootstrap (одноразово):** `.venv/Scripts/python -m vpn.bootstrap` — генерит ключ и кладёт его на сервер.

**Когда использовать:** ТОЛЬКО в парсерах зарубежных сайтов.
**Когда НЕ использовать:** DeepSeek API, локальные БД, RU-источники, HF datasets — они работают через обычное соединение.

**Шаблон интеграции в новый парсер** (см. `en_news_archive_loader.py` как образец):

```python
class HttpClient:
    def __init__(self, ..., use_vpn: bool = False):
        self.session = requests.Session()
        if use_vpn:
            from src.utils.proxy import get_proxies
            self.session.proxies = get_proxies()
```

Плюс `--vpn` флаг в argparse и `use_vpn=args.vpn` при создании клиента.

**Правило:** перед тем как добавлять VPN в новый парсер — проверить, что без него парсинг действительно падает. Если работает без VPN — не трогать.
