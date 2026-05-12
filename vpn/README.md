# VPN — SSH SOCKS5 tunnel

Маршрут для парсинга зарубежных источников (BBC, Guardian, Fox, AlJazeera,
Euronews, France24, …), который текущий VPN рабочего компьютера режет
(`ConnectionReset 10054` ещё на стадии TLS-хендшейка).

## Идея

Один процесс `ssh -D 1080 -N` открывает локальный SOCKS5-порт. Любой код
проекта, который явно ходит через `127.0.0.1:1080`, идёт через
выделенный сервер `37.233.83.68` (Ubuntu 24.04, root). Всё остальное в
системе — браузер, апдейты, прочие приложения — идёт мимо, по обычному
маршруту.

```
парсер ──► requests(proxies=socks5h://127.0.0.1:1080)
              │
              ▼
       ssh -D 1080 ──[SSH]──► 37.233.83.68 ──► bbc.com / guardian / …
```

Используется `socks5h://`, а не `socks5://`, — DNS резолвится на стороне
сервера. Никаких утечек через локальный резолвер.

## Файлы

```
vpn/
  bootstrap.py        # одноразовая установка SSH-ключа на сервер
  start.ps1           # ручной запуск туннеля + показ внешнего IP
  stop.ps1            # ручной останов
  status.ps1          # diagnostics: direct IP vs tunnel IP
  .tunnel.pid         # PID запущенного ssh.exe (gitignored)
  .gitignore
  README.md           # этот файл
src/utils/
  __init__.py
  proxy.py            # is_vpn_up / start_vpn / stop_vpn / get_proxies / vpn()
```

Конфиг — в `.env` (gitignored):

```
vpn_host=37.233.83.68
vpn_user=root
vpn_password=...        # нужен только для vpn/bootstrap.py
vpn_ssh_key=~/.ssh/signal_mind_vpn
vpn_local_port=1080
```

Приватный SSH-ключ — в `~/.ssh/signal_mind_vpn` (вне репозитория).
Публичный лежит на сервере в `/root/.ssh/authorized_keys`.

## Установка с нуля

Если ключ потерян / новый компьютер:

```powershell
.venv/Scripts/python -m vpn.bootstrap
```

Bootstrap делает:
1. `ssh-keygen -t ed25519 -f ~/.ssh/signal_mind_vpn` (Ed25519, без пассфразы).
2. Через `paramiko` коннектится с паролем из `.env` и кладёт публичный ключ в `authorized_keys`.
3. Прогоняет `ssh -o BatchMode=yes -o PasswordAuthentication=no … echo OK` — проверяет, что ключевая аутентификация работает без пароля.

После этого `vpn_password` в `.env` больше не нужен в обычной работе.
Можно очистить, но не обязательно (файл всё равно в `.gitignore`).

## Использование

### Из кода — context manager

```python
from src.utils.proxy import vpn
import requests

with vpn() as proxies:
    r = requests.get("https://www.bbc.com/news", proxies=proxies)
```

Туннель поднимается при первом вызове `vpn()` / `get_proxies()` и
переиспользуется. Закрывается автоматически при выходе Python-процесса
(`atexit`). Повторные `with vpn()` блоки не пересоздают коннект — это
просто быстрый health-check.

### Из кода — Session

```python
import requests
from src.utils.proxy import get_proxies

session = requests.Session()
session.proxies = get_proxies()  # один раз
session.get(url)                  # дальше всё через туннель
```

### Через флаг парсера

`en_news_archive_loader.py` принимает `--vpn`:

```
.venv/Scripts/python en_news_archive_loader.py \
  --from 2025-09-01 --to 2025-09-01 \
  --sources bbc,guardian --vpn
```

### Ручные кнопки

```powershell
.\vpn\start.ps1     # старт + показать tunnel IP
.\vpn\status.ps1    # direct IP vs tunnel IP
.\vpn\stop.ps1      # стоп
```

## Что НЕ заворачивается

- DeepSeek API (`agent.agent`) — работает через текущий VPN, незачем добавлять hop.
- Локальные БД (DuckDB, SQLite, Chroma) — это локально.
- HuggingFace `datasets` — через текущий VPN работает.

Заворачиваются только парсеры, явно делающие `requests` к
западным новостным сайтам. Сейчас интегрирован один —
[en_news_archive_loader.py](../en_news_archive_loader.py). Остальные
(`gdelt_loader.py`, `cc_news_parser.py`, …) — по запросу, по той же
схеме: `use_vpn: bool` параметр в HTTP-клиенте + `--vpn` CLI-флаг.

## Расширение на другой парсер

Шаблон (как в `en_news_archive_loader.py`):

```python
class HttpClient:
    def __init__(self, ..., use_vpn: bool = False) -> None:
        self.session = requests.Session()
        if use_vpn:
            from src.utils.proxy import get_proxies
            self.session.proxies = get_proxies()
        ...
```

И добавить `p.add_argument("--vpn", action="store_true", ...)` в argparse.

## Диагностика

| Симптом | Что проверить |
|---|---|
| `is_vpn_up() == False` | `ssh -i ~/.ssh/signal_mind_vpn -v root@37.233.83.68` |
| `start_vpn` падает с `ssh tunnel exited immediately` | сервер недоступен / ключ некорректен / порт 22 закрыт |
| Локальный порт 1080 уже занят | поменять `vpn_local_port` в `.env` (например, `10808`) |
| Direct IP не блокирует, а tunnel блокирует | парсер сам режет 4xx — посмотреть `User-Agent`, заголовки |
| Зомби-процесс `ssh.exe` после краша | `Get-Process ssh` + `Stop-Process -Id <pid>` или `.\vpn\stop.ps1` |

## Проверочный прогон

Базовая проверка:
```
Direct IP : 89.222.124.49        # текущая сеть
Tunnel IP : 37.233.83.68         # ожидаемый
BBC без VPN : ConnectionResetError 10054
BBC через VPN : 336 673 bytes, <title>BBC News - Breaking news ...</title>
```

Полный smoke по всем 6 EN-источникам:
```
.venv/Scripts/python -m vpn.smoke_en_sources --day 2025-09-01
```

Скрипт выдаёт per-source метрики (время, links, downloaded, matched, rej_*),
3 примера заголовков + совпавшие ключи. Не пишет в БД. Для сравнения «с/без»:
тот же запуск с `--no-vpn`.

Текущее состояние (2026-05-12, после фикса `_curl_fallback`):

| Источник | Через VPN |
|---|---|
| bbc | ✅ стабильно |
| guardian | ✅ стабильно, лучшее тематическое покрытие |
| fox | ✅ стабильно, но sitemap агрегирует ±1 день |
| aljazeera | ✅ стабильно, 100% match-rate под тематику проекта |
| euronews | ✅ стабильно через `curl --compressed` fallback |
| france24 | ⚠ intermittent (Akamai Bot Manager): на одних днях даёт статьи, на других `links=0`. Без exception'ов — пайплайн не ломает. |

## Безопасность

- `~/.ssh/signal_mind_vpn` — приватный ключ, не коммитить.
- `.env` — с паролем, уже в `.gitignore`.
- `key.txt` (исходные реквизиты сервера) — тоже в `.gitignore`.
- На сервере включить fail2ban + отключить парольный логин для root
  можно, но не обязательно (root-логин по паролю всё равно нужен
  только до первого bootstrap).
