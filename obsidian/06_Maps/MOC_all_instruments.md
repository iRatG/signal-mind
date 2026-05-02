---
type: moc
status: active
tags: [moc, instruments, scope]
updated: 2026-05-02
---

# MOC: All Instruments — полная карта данных

## MOEX Индексы

### Активно тестируются
- **MOEXFN** — финансы/банки → сигналы: [[signal_usd_rub_moexfn]], [[signal_brent_moexfn]], [[signal_msci_india_moexfn]]
- **MOEXOG** — нефть/газ → сигналы: [[signal_oil_news_moexog]], [[signal_moexog_usd_rub]]
- **MOEX10** — голубые фишки → сигнал: [[signal_inflation_moex10]]
- **IMOEX** — основной индекс → без подтверждённых сигналов

### Нужно исследовать (P0/P1)
- **RUCBTR3YNS** / **RUCBTR5YNS** — облигации 3Y/5Y → bond-equity spillover?
- **RUSFAR3M** — ставка денежного рынка → коррелирует с MOEXFN?
- **MREDC** — недвижимость → avg_wage_rub → MREDC?
- **RUGOLD** — золотодобыча → GOLD price → RUGOLD?
- **MOEXBC** — регионы → что движет региональными акциями?
- **MDIAMR** — дивиденды → высокая ставка → дивидендные акции?

### Экзотические (P2)
- IMOEXW, MOEXBMI, SUGAROTCVOL, RUPCI

---

## Рыночные инструменты

### Активно тестируются
- **USD_RUB** → [[signal_usd_rub_moexfn]], [[signal_sanctions_usd_rub]]
- **EUR_RUB** → [[signal_sanctions_usd_rub]] (частично)
- **BRENT** → [[signal_brent_moexfn]], [[signal_oil_news_moexog]]
- **GOLD** → активно, без подтверждённых
- **MSCI_INDIA** → [[signal_msci_india_moexfn]]

### Нужно исследовать
- **SILVER** → RUGOLD? GOLD? cross-commodity
- **ALUMINUM** → MOEXOG? IMOEX? промышленный металл
- **CHINA_H_SHARES** → BRENT через China demand
- **DJ_SOUTH_AFRICA** → IMOEX (EM correlation)
- **DXY** → USD_RUB через глобальный доллар (lag?)

---

## Новостные топики

| Топик | Текущие цели | Нетестированные цели |
|-------|-------------|---------------------|
| news_oil | MOEXOG | BRENT напрямую? MOEXFN через бюджет? |
| news_gold | — | RUGOLD (прямая!) |
| news_banking | MOEXFN | RUCBTR3YNS? |
| news_rate | MOEXFN | RUSFAR3M (прямой прокси!) |

---

## Полный скоуп → [[concept_full_data_scope]]
