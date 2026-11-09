<div align="center">

# DeepWhale

### Следи за деньгами на Hyperliquid.

Открытая платформа для отслеживания самых прибыльных кошельков Hyperliquid: позиций, сделок, PnL и поведения в лонг/шорт. Создана для аналитики и экспериментов со стратегиями следования.

[English version](README.md)

![Python](https://img.shields.io/badge/Python-3.14-3776AB?logo=python&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-3.3.2-017CEE?logo=apacheairflow&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-database-4169E1?logo=postgresql&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-planned-6E4AFF?logo=grafana&logoColor=white)
![Status](https://img.shields.io/badge/status-MVP-black)

</div>

---

## Зачем DeepWhale?

Hyperliquid — одна из немногих криптобирж, где **весь поток ордеров публичен**. Кошельки псевдонимны, но их позиции и сделки видны в сети. Это даёт редкую возможность:

- видеть, что реально делают лучшие трейдеры;
- измерять их результат, а не догадываться;
- сравнивать текущие позиции с движением цены;
- строить и проверять **стратегии следования** на реальных данных с таймстемпами.

DeepWhale превращает сырые ответы публичного API в чистую базу и аналитический слой.

> Это исследовательский проект, а не инвестиционная рекомендация.

## Что можно делать

- **Находить** топ-кошельки по месячному PnL и размеру счёта.
- **Отслеживать** открытые позиции: размер, направление, цену входа, нереализованный PnL, плечо, цену ликвидации.
- **Анализировать** сделки: реализованный PnL, объём, win rate, направление.
- **Сравнивать** поведение китов с рыночными ценами из CoinGecko.
- **Исследовать** перегруженность лонг/шорт, концентрацию кошельков и доходность стратегий следования.

## Архитектура

```mermaid
flowchart LR
    HL[Публичный API Hyperliquid] -->|лидерборд, позиции, сделки| AF[Apache Airflow]
    CG[API CoinGecko] -->|цены крипты| AF
    TD[API Twelve Data] -->|акции США| AF
    AF --> PG[(PostgreSQL)]
    PG --> AN[Аналитика / исследования]
    PG -. план .-> GF[Grafana]
```

Airflow запускает DAG-и загрузки. PostgreSQL — единственный источник правды. Grafana станет слоем визуализации.

## Пайплайны

| DAG | Расписание | Источник | Куда пишет | Зачем |
|---|---|---|---|---|
| `wallets_snapshot` | раз в день, `0 0 * * *` | лидерборд Hyperliquid | `hl_top_wallets` | выбрать топ-10 кошельков по PnL за месяц |
| `positions_snapshot` | каждые 5 минут | `clearinghouseState` | `hl_positions` | снимок открытых позиций топ-10 кошельков |
| `fills_snapshot` | каждые 5 минут | `userFillsByTime` | `hl_fills` | инкрементальная загрузка сделок топ-5 кошельков |
| `crypto_snapshot` | каждые 5 минут | CoinGecko | `coin_snapshot` | снимки BTC / ETH / BNB / GRAM |
| `stock_snapshot` | каждые 10 минут | Twelve Data | `stock_snapshot` | акции США с проверкой календаря NYSE |

Fills загружаются инкрементально: DAG берёт `max(ts)` по кошельку, вычитает 60 секунд перекрытия и отсекает дубли по `tid`. Positions — это снимки: каждый запуск пишет новое состояние каждой открытой позиции.

## Схема БД

```mermaid
erDiagram
    hl_top_wallets ||--o{ hl_positions : "address"
    hl_top_wallets ||--o{ hl_fills : "address"

    hl_top_wallets {
        text address PK
        text display_name
        numeric account_value
        numeric pnl_month
        numeric roi_month
        numeric vlm_month
    }

    hl_positions {
        timestamptz snapshot_at PK
        text address PK
        text coin PK
        numeric szi
        numeric entry_px
        numeric unrealized_pnl
        numeric leverage
        numeric liquidation_px
    }

    hl_fills {
        bigint tid PK
        text address
        text coin
        text dir
        numeric px
        numeric sz
        numeric closed_pnl
        numeric fee
        timestamptz ts
    }
```

### Таблицы

| Таблица | Гранулярность | За что отвечает |
|---|---|---|
| `hl_top_wallets` | одна строка на кошелёк | текущий месячный топ: имя, equity, PnL, ROI, объём |
| `hl_positions` | кошелёк × монета × снимок | текущий риск: размер, сторона (из `szi`), вход, uPnL, плечо, ликвидация |
| `hl_fills` | одна строка на сделку | неизменяемый лог сделок: направление, цена, размер, реализованный PnL, комиссия, время |

Модель в **3НФ**: никаких вычисляемых `side`, `rank`, `position_value` и `roe` — они считаются в запросах. У `hl_positions` и `hl_fills` есть FK на `hl_top_wallets`.

<details>
<summary><strong>SQL DDL</strong></summary>

```sql
CREATE TABLE hl_top_wallets (
    address        text PRIMARY KEY,
    display_name   text,
    account_value  numeric(30, 6),
    pnl_month      numeric(30, 6),
    roi_month      numeric(20, 10),
    vlm_month      numeric(30, 6)
);

CREATE TABLE hl_positions (
    snapshot_at     timestamptz NOT NULL,
    address         text NOT NULL
                    REFERENCES hl_top_wallets(address) ON DELETE CASCADE,
    coin            text NOT NULL,
    szi             numeric(30, 10),
    entry_px        numeric(30, 10),
    unrealized_pnl  numeric(30, 6),
    leverage        numeric(10, 2),
    liquidation_px  numeric(30, 10),
    PRIMARY KEY (snapshot_at, address, coin)
);

CREATE TABLE hl_fills (
    tid          bigint PRIMARY KEY,
    address      text NOT NULL
                 REFERENCES hl_top_wallets(address) ON DELETE CASCADE,
    coin         text NOT NULL,
    dir          text,
    px           numeric(30, 10),
    sz           numeric(30, 10),
    closed_pnl   numeric(30, 6),
    fee          numeric(30, 6),
    ts           timestamptz NOT NULL
);

CREATE INDEX hl_fills_coin_ts_idx
    ON hl_fills (coin, ts DESC);

CREATE INDEX hl_fills_address_ts_idx
    ON hl_fills (address, ts DESC);

CREATE INDEX hl_positions_coin_snapshot_idx
    ON hl_positions (coin, snapshot_at DESC);
```

</details>

Вспомогательные таблицы рыночных данных: `coins` + `coin_snapshot` для CoinGecko, `stocks` + `stock_snapshot` для акций США.

## Стек

| Слой | Инструменты |
|---|---|
| Оркестрация | Apache Airflow 3.3.2 |
| Язык | Python 3.14 |
| Хранение | PostgreSQL |
| Источники | Hyperliquid API, CoinGecko API, Twelve Data API |
| Аналитика | SQL, оконные функции, dimensional modelling |
| Визуализация | Grafana (в планах) |
| Инструменты | Git, psql, Airflow UI |

## Быстрый старт

Нужны Python 3.14, Apache Airflow 3.x с PostgreSQL-провайдером и запущенный PostgreSQL.

```bash
git clone https://github.com/NoTimur/DeepWhale.git
cd DeepWhale

python3 -m venv .venv
source .venv/bin/activate

# Создать схему аналитики: открой psql и вставь DDL из раздела «Схема БД»
psql -h localhost -U crypto_app -d crypto
```

Настройка Airflow:

1. Укажи `dags_folder` на `./airflow/dags`.
2. Создай PostgreSQL-подключение `crypto_postgres` к базе `crypto`.
3. Для стокового DAG-а создай HTTP-подключение `twelve_data`: host `https://api.twelvedata.com`, пароль — API-ключ.
4. Hyperliquid **не требует API-ключа**.

Запуск Airflow:

```bash
airflow standalone
```

Открой `http://localhost:8080`, включи DAG-и и сначала запусти `wallets_snapshot`. DAG-и позиций и сделок читают адреса из `hl_top_wallets`, поэтому кошельки должны быть загружены первыми.

> Секреты намеренно исключены из репозитория. `API.txt`, `ariflow_password.txt` и `.env` находятся в `.gitignore`.

## Дашборд

> **Скоро.**

Grafana — следующий этап. Планируемые экраны:

- лидерборд китов: PnL, ROI, объём, размер счёта;
- карточка кошелька: открытые позиции, плечо, расстояние до ликвидации, последние сделки;
- рыночный экран: цена BTC/ETH с маркерами лонг/шорт сделок;
- здоровье данных: свежесть, количество строк, упавшие DAG-и, расход лимитов API.

Скриншотов пока нет — раздел обновится, когда Grafana будет подключена.

## Roadmap

- [x] Поиск кошельков Hyperliquid и месячный рейтинг
- [x] Схема PostgreSQL в 3НФ
- [x] Инкрементальная загрузка fills с дедупликацией
- [x] Снимки позиций и оценка лонг/шорт-экспозиции
- [x] DAG-и для Hyperliquid, CoinGecko и акций США
- [ ] Дашборд Grafana
- [ ] Разметка кошельков и entity resolution
- [ ] Бэктестер стратегий следования: равные веса, risk-adjusted, правила ребалансировки
- [ ] Алерты на резкие изменения позиций
- [ ] Пагинация для кошельков с более чем 2 000 fills за окно
- [ ] Docker Compose и CI

## Ограничения

- Лидерборд Hyperliquid **opt-in** и псевдонимен. Это не все кошельки сети.
- `userFillsByTime` отдаёт максимум **2 000 сделок за ответ** и только **10 000 последних**.
- Чтобы уложиться в лимит **1 200 weight/min**, fills собираются по топ-5 кошелькам, positions — по топ-10.
- История лидерборда API не отдаётся; проект хранит собственные снимки.
- Это исследовательский инструмент, а не торговая система.

## Дисклеймер

DeepWhale — учебный и исследовательский проект. Он не даёт инвестиционных рекомендаций и не исполняет сделки. Публичные on-chain данные могут быть неполными, запаздывать или вводить в заблуждение. Прошлая доходность любого кошелька не гарантирует будущую.

---

<div align="center">

Сделано из любопытства, SQL и уважения к лимитам API.

[English version](README.md)

</div>
