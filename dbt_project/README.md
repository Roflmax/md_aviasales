# DBT Project: Aviasales DWH

DBT проект для трансформации данных о ценах на авиабилеты.

## Структура

```
dbt_project/
├── models/
│   ├── sources.yml              # Определение источника (STG)
│   ├── ods/
│   │   ├── schema.yml           # Описание + тесты ODS
│   │   └── ods_flight_prices.sql
│   └── dm/
│       ├── schema.yml           # Описание + тесты DM
│       ├── dm_best_prices_per_flight.sql
│       ├── dm_price_by_days_before_departure.sql
│       └── dm_price_by_fetch_time.sql
├── dbt_project.yml              # Конфигурация проекта
├── profiles.yml                 # Подключения к БД
├── packages.yml                 # Зависимости (elementary, dbt_utils)
└── dbt_packages/                # Установленные пакеты
```

## Модели

### Источник данных (STG)

| Таблица | Схема | Описание |
|---------|-------|----------|
| `flight_prices_raw` | stg | JSONB с сырыми данными из MongoDB |

### ODS слой

| Модель | Материализация | Описание |
|--------|----------------|----------|
| `ods_flight_prices` | incremental (delete+insert) | Распарсенные данные о билетах |

**Ключевые поля:**
- `record_id` — уникальный ID записи (PK)
- `flight_price_id` — ID билета из API
- `origin`, `destination` — маршрут (LED → SVX)
- `price` — цена в рублях
- `airline` — код авиакомпании
- `departure_at` — дата/время вылета
- `days_until_departure` — за сколько дней до вылета найден билет

### DM слой (витрины)

| Модель | Материализация | Бизнес-вопрос |
|--------|----------------|---------------|
| `dm_best_prices_per_flight` | table | Какая лучшая цена на каждый рейс? |
| `dm_price_by_days_before_departure` | incremental (append) | За сколько дней до вылета лучше покупать? |
| `dm_price_by_fetch_time` | table | В какой день/час лучше искать билеты? |

## Инкрементальные стратегии

Проект использует **2 вида** инкрементальной загрузки:

### 1. delete+insert (ODS)
```sql
{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='record_id'
    )
}}

{% if is_incremental() %}
    where loaded_at > (select max(loaded_at) from {{ this }})
{% endif %}
```

### 2. append (DM)
```sql
{{
    config(
        materialized='incremental',
        incremental_strategy='append'
    )
}}

{% if is_incremental() %}
    having max(loaded_at) > (
        select coalesce(max(calculated_at), '1970-01-01') - interval '7 days'
        from {{ this }}
    )
{% endif %}
```

## Тесты

### dbt-core тесты (4 типа)

| Тип | Пример | Количество |
|-----|--------|------------|
| `unique` | record_id, flight_price_id | 2 |
| `not_null` | price, airline, departure_at | 15 |
| `accepted_values` | airline IN ('DP','U6','5N','SU','S7') | 2 |
| `relationships` | dm.flight_price_id → ods.flight_price_id | 1 |

### Elementary тесты (6 типов аномалий)

| Тип | Описание | Применяется к |
|-----|----------|---------------|
| `volume_anomalies` | Аномалии объёма данных | ODS, все DM |
| `freshness_anomalies` | Задержка обновления | ODS |
| `column_anomalies` | Аномалии значений колонки | price, best_price |
| `dimension_anomalies` | Аномалии в измерении | airline |
| `event_freshness_anomalies` | Задержка между событиями | fetched_at → loaded_at |
| `all_columns_anomalies` | Общие аномалии всех колонок | ODS |

## Теги

```bash
# Запуск только ODS
dbt run --select tag:ods

# Запуск только DM
dbt run --select tag:dm

# Тесты elementary
dbt test --select tag:elementary
```

| Тег | Модели |
|-----|--------|
| `ods` | ods_flight_prices |
| `dm` | все DM витрины |
| `analytics` | все DM витрины |
| `pricing` | dm_price_by_* |
| `best_deals` | dm_best_prices_per_flight |

## Jinja-шаблоны

Каждая модель использует Jinja:

- `{{ config(...) }}` — конфигурация материализации
- `{{ source('stg', 'flight_prices_raw') }}` — ссылка на источник
- `{{ ref('ods_flight_prices') }}` — ссылка между моделями
- `{{ this }}` — ссылка на текущую таблицу (для incremental)
- `{% if is_incremental() %}` — условная логика

## Команды

```bash
# Установка зависимостей
dbt deps

# Запуск всех моделей
dbt run

# Запуск тестов
dbt test

# Генерация документации
dbt docs generate
dbt docs serve

# Elementary отчёт
edr report --file-path ./reports/elementary_report.html
```

## Зависимости

| Пакет | Версия | Назначение |
|-------|--------|------------|
| elementary-data/elementary | 0.17.0 | Тесты аномалий, отчёты |
| dbt-labs/dbt_utils | 1.3.0 | Утилиты (surrogate_key и др.) |

## DAG моделей

```
source.stg.flight_prices_raw
            │
            ▼
    ods_flight_prices (incremental)
            │
     ┌──────┼──────────────┐
     ▼      ▼              ▼
dm_best   dm_price_by    dm_price_by
_prices   _days_before   _fetch_time
          _departure
```
