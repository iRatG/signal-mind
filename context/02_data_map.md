# Карта данных — Signal Mind

Все данные лежат в `C:\project\signal_mind\statistic\`

---

## 1. Финансовые рынки — MOEX индексы

**Путь:** `statistic\moex\*.csv.zip`
**Формат:** ZIP-архив содержит один файл `security.csv`
**Кодировка:** cp1251 (кириллица — крякозябры в raw-режиме, нужен encoding='cp1251')
**Разделитель:** точка с запятой `;`
**Десятичный разделитель:** запятая `,`
**Первая строка:** всегда `"history"` — пропустить (skiprows=1)

### Список файлов и индексов

| Файл | Тикер | Описание | История с |
|---|---|---|---|
| IMOEX.csv.zip | IMOEX | Индекс Мосбиржи (основной) | 2026 (мало!) |
| IMOEXW.csv.zip | IMOEXW | Индекс полной доходности | 2019 |
| MOEX10.csv.zip | MOEX10 | Топ-10 акций | 2020 |
| MOEXBC.csv.zip | MOEXBC | Голубые фишки | 2020 |
| MOEXFN.csv.zip | MOEXFN | Финансовый сектор | 2016 |
| MOEXOG.csv.zip | MOEXOG | Нефть и газ | 2019 |
| RUCBTR3YNS.csv.zip | RUCBTR3YNS | ОФЗ 3 года | 2018 |
| RUCBTR5YNS.csv.zip | RUCBTR5YNS | ОФЗ 5 лет | ~2019 |
| RUPCI.csv.zip | RUPCI | Промышленные товары | 2016 |
| RUGOLD.csv.zip | RUGOLD | Золото | 2023 |
| MREDC.csv.zip | MREDC | Недвижимость МО | 2016 |
| MDIAMR.csv.zip | MDIAMR | Алмазы | 2022 |
| RUSFAR3M.csv.zip | RUSFAR3M | Ставка денег рынка 3М | 2019 |
| RUSFAR3MRT.csv.zip | RUSFAR3MRT | RUSFAR 3М total return | ~2019 |
| SUGAROTCVOL.csv.zip | SUGAROTCVOL | Сахар OTC объём | 2023 |

### Колонки
```
BOARDID | SECID | TRADEDATE | SHORTNAME | NAME | CLOSE | OPEN | HIGH | LOW |
VALUE | DURATION | YIELD | DECIMALS | CAPITALIZATION | CURRENCYID | DIVISOR |
TRADINGSESSION | VOLUME | TRADE_SESSION_DATE | RECALC_DATE
```
Нужные для анализа: `TRADEDATE, SECID, CLOSE, OPEN, HIGH, LOW, VALUE, CAPITALIZATION`

---

## 2. Курсы валют — официальный ЦБ

**Путь:** `statistic\moex\RC_F01_04_2018_T29_04_2026.xlsx`
**Формат:** XLSX (OpenXML, без shared strings — все значения inline)
**История:** 01.04.2018 — 29.04.2026

### Колонки
```
nominal | data | curs | cdx
```
- `nominal` — номинал (1, 10, 100 и т.д.)
- `data` — дата как Excel serial number (число типа 46141)
  - Конвертация: `pd.Timestamp('1899-12-30') + pd.Timedelta(days=int(N))`
- `curs` — курс в рублях (float, точка как разделитель)
- `cdx` — название валюты на русском ("Доллар США", "Евро", "Юань" и т.д.)

Также есть `RC_F22_04_2026_T29_04_2026.xlsx` — только последняя неделя, дубль.

---

## 3. Ключевая ставка ЦБ

**Путь:** `statistic\moex\cbr_hd_base_KeyRate__UniDbQuery.Posted=True&UniDbQuery.From=17.09.2013&UniDbQuery.To=29.04.2026.pdf`
**Формат:** PDF (FlateDecode, сжатый) — **не читается без pdfplumber**
**История:** 17.09.2013 — 29.04.2026

⚠️ Для парсинга нужен Python + pdfplumber. Альтернатива: скачать с cbr.ru в формате Excel.

---

## 4. Валюты и сырьё (Investing.com)

**Путь:** `statistic\moex\Прошлые данные - *.csv`
**Формат:** CSV, разделитель запятая, значения в кавычках, числа с запятой `"75,06"`
**Кодировка:** UTF-8

⚠️ **Проблема:** большинство файлов содержат только ~23 строки (~1 месяц). Нужна полная история.

| Файл | Инструмент | Строк | Нужна история |
|---|---|---|---|
| Прошлые данные - USD_RUB.csv | USD/RUB | 23 | Да, с 2016 |
| Прошлые данные - EUR_RUB.csv | EUR/RUB | 23 | Да, с 2016 |
| Прошлые данные - Фьючерс на нефть Brent.csv | Brent | 22 | Да, с 2016 |
| Прошлые данные - Фьючерс на золото.csv | Gold | 892 | Частично (с 2022) |
| Прошлые данные - US 500 Cash.csv | S&P 500 | 28 | Да, с 2016 |
| Прошлые данные - Фьючерс на индекс USD.csv | USD Index | 1117 | Частично (с 2022) |

Колонки: `Дата | Цена | Откр. | Макс. | Мин. | Объём | Изм. %`

---

## 5. Реестр заявок (биржевой стакан)

**Путь:** `statistic\moex\reestr_zayavok\orderlog20241001.zip`
**Содержимое:** `OrderLog20241001.txt` — 28 MB
**Статус:** Отложено. Это тиковые данные за один день (01.10.2024). Слишком гранулярно для макро-анализа.

---

## 6. Макроэкономика — Росстат

**Путь:** `statistic\*.xlsx` и `statistic\*.xls`
**Формат:** Excel, нестандартные заголовки Росстата
**Гранулярность:** год × регион (субъект РФ)
**Общая проблема:** объединённые ячейки, многострочные заголовки, листы "Содержание" (пропускать), сноски снизу

### Файлы

**Зарплаты:**
- `tab1-zpl_01-2026.xlsx` — средняя зарплата, 3 листа, 48 строк × 18 колонок
- `tab4-zpl_2025.xlsx` — два периода: лист "2000-2017" и лист "с 2018" (разрыв методологии!)
- `tab5-zpl_2025.xlsx` — аналогично
- `t12.xlsx` — зарплаты по видам деятельности (ОКВЭД2), федеральный срез (НЕ по регионам)

**Демография:**
- `demo32_2023.xlsx` — разводы по регионам, суперскрипты в заголовках
- `demo33_2023.xlsx` — другой демографический показатель
- `Chisl_RF_01-01-2022-01-01-2024.xls` — численность населения РФ, 2022–2024

**Жильё (2019–2025):**
- `Jil_fond_2019-2025.xls` — жилищный фонд (самый большой, 1.7 MB)
- `jil_prav_2020-2025.xls` — жилищные права
- `Jil_priv_2020-2025.xls` — приватизация жилья

**Экономика:**
- `Effect_VRP_2024.xlsx` — ВРП, 4 листа, регионы × годы
- `Fondovoorujen_2024.xlsx` — фондовооружённость, 4 листа
- `innov_1_2024.xls` — инновационная активность 2024
- `uroven_innov_2024.xls` — уровень инноваций 2024

**Образование:**
- `Pokazateli_DO.xlsx` — дошкольное образование, 10 листов (содержание + 9 таблиц), A1:AC35
- `Kadry_VO.xls` — кадры высшего образования (384 KB, много листов)
- `Vyp_inf_bez.xls` — выпуск / информатизация / безработица

---

## 7. Регуляторные PDF — ЦБ РФ

**Путь:** `statistic\pdf\`
**Формат:** PDF (Winnovative converter, FlateDecode) — нужен pdfplumber

| Файл | Тип | Период |
|---|---|---|
| kgo_2022.pdf | КГО — Ключевые выводы года ЦБ | 2022 |
| kgo_2023.pdf | КГО | 2023 |
| kgo_2024.pdf | КГО | 2024 |
| kgo_2025.pdf | КГО | 2025 |
| review_mfi_25Q4.pdf | Обзор МФО (микрофинансы) | Q4 2025 |
| pf_2025_Q4.pdf | Пенсионный фонд | Q4 2025 |
| Bbs2603r.pdf | Статистический бюллетень | — |

КГО — это главный нарративный источник. ЦБ описывает риски, тренды, выводы за год.
Для RAG: разбивать по страницам/разделам, сохранять метаданные (year, section, page).

---

## 8. Новости — HuggingFace

**Dataset:** `Brianferrell787/financial-news-multisource`
**Объём:** 57.1 млн строк
**Период:** 1990–2025
**Источники:** Bloomberg, Reuters, NYT, CNBC, Yahoo Finance, Reddit, DJIA, S&P500 headlines и др. (24 источника)
**Формат:** Parquet, поддерживает streaming

**Статус:** Доступ запрошен ✅. Нужен HuggingFace токен для загрузки.

**Стратегия загрузки:**
- Streaming + фильтр по дате: с 2016 года
- Фильтр по ключевым словам: russia, ruble, oil, brent, moex, sanctions, interest rate, central bank, inflation, gold, rouble, gazprom, sberbank, lukoil
- Ожидаемый объём после фильтрации: 500K–2M строк
- Хранение: ChromaDB (векторная БД для семантического поиска)

**Схема данных:**
```
date       | string | UTC ISO 8601 timestamp
text       | string | заголовок + тело статьи
extra_fields | string | JSON: url, publisher, author, stocks, text_type, ...
```
