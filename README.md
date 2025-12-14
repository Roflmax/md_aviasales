# Aviasales Price Tracker

ELT-пайплайн для анализа цен на авиабилеты LED (Санкт-Петербург) → SVX (Екатеринбург).


```

## Структура проекта

```
md_aviasales/
├── app/                        # FastAPI сервис сбора данных
│   ├── main.py                 # 3 endpoints: /health, /fetch-prices, /prices
│   ├── api.py                  # Клиент Travelpayouts API
│   ├── db.py                   # MongoDB операции (save/get)
│   ├── config.py               # Конфигурация из ENV
│   ├── requirements.txt
│   └── Dockerfile
│
├── airflow/                    # Оркестрация ELT пайплайна
│   ├── dags/
│   │   ├── fetch_prices_dag.py # Сбор данных из API (каждый час)
│   │   ├── el_pipeline.py      # EL: MongoDB → PostgreSQL STG
│   │   └── dbt_pipeline.py     # dbt run + test + elementary report
│   ├── reports/                # Elementary HTML отчёты
│   └── Dockerfile
│
├── dbt_project/                # DBT трансформации (см. dbt_project/README.md)
│   ├── models/
│   │   ├── sources.yml         # Источник: stg.flight_prices_raw
│   │   ├── ods/                # ODS слой (парсинг JSONB)
│   │   └── dm/                 # DM витрины (аналитика)
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── packages.yml            # elementary, dbt_utils
│
├── sql/                        # Инициализация БД
│   ├── init.sql                # STG таблица + индексы
│   └── 00-create-airflow-db.sh
│
├── docker-compose.yml          # 7 сервисов
├── .pre-commit-config.yaml     # Линтеры: black, isort, flake8, sqlfluff
├── .env.example                # Шаблон конфигурации
└── README.md
```

## Слои данных

| Слой | Хранилище | Описание |
|------|-----------|----------|
| RAW | MongoDB | Сырые JSON из Aviasales API |
| STG | PostgreSQL (JSONB) | Staging — append-only лог |
| ODS | PostgreSQL | Распарсенные данные, инкрементально |
| DM | PostgreSQL | 3 аналитические витрины |

## Компоненты

### app/ — FastAPI сервис

Собирает цены на билеты через Travelpayouts API и сохраняет в MongoDB.

| Endpoint | Метод | Описание |
|----------|-------|----------|
| `/health` | GET | Проверка работоспособности |
| `/fetch-prices` | POST | Запрос цен из API → MongoDB |
| `/prices` | GET | Получение сохранённых билетов |

### airflow/ — Оркестрация

3 DAG'а выполняются последовательно каждый час:

1. **fetch_flight_prices** (`:00`) — вызывает `/fetch-prices`
2. **el_flight_prices** (`:00`) — переносит данные MongoDB → PostgreSQL STG
3. **dbt_flight_prices** (`:30`) — dbt run → dbt test → edr report

### dbt_project/ — Трансформации

Подробное описание: [dbt_project/README.md](dbt_project/README.md)

- **ODS**: Парсинг JSONB, incremental (delete+insert)
- **DM**: 3 витрины с оконными функциями и агрегатами

## Запуск

### Первичная настройка

```bash
# 1. Скопируйте файлы конфигурации из шаблонов
cp .env.example .env
cp dbt_project/profiles.yml.example dbt_project/profiles.yml

# 2. Отредактируйте .env и укажите реальные значения:
# - AVIASALES_API_TOKEN (обязательно!)
# - TRAVELPAYOUTS_MARKER (обязательно!)
# - Пароли для MongoDB и PostgreSQL
# - AIRFLOW_SECRET_KEY (сгенерируйте случайную строку)
# - JUPYTER_TOKEN

# 3. Запуск всех сервисов
docker compose up -d

# 4. Проверка
docker compose ps
```

### Важно для безопасности

**Файлы с конфиденциальными данными (НЕ коммитьте в git):**
- `.env` — все пароли и API ключи
- `dbt_project/profiles.yml` — подключения к БД

**Используйте только шаблоны:**
- `.env.example` — пример конфигурации
- `dbt_project/profiles.yml.example` — пример профиля dbt

## Сервисы

| Сервис | URL | Credentials |
|--------|-----|-------------|
| Swagger API | http://localhost:8000/docs | - |
| Airflow | http://localhost:8081 | admin / admin |
| Elementary Report | http://localhost:8083 | - |
| PostgreSQL | localhost:5432 | airflow / airflow |
| MongoDB | localhost:27017 | admin / password |

## Качество данных

- **dbt-core тесты**: unique, not_null, accepted_values, relationships
- **Elementary тесты**: 6 типов аномалий (volume, freshness, column, dimension, event_freshness, all_columns)
- **Pre-commit хуки**: black, isort, flake8, yamllint, sqlfluff

## Мониторинг

Elementary Report автоматически обновляется после каждого запуска dbt:
- Статусы всех тестов
- Обнаруженные аномалии
- Тренды метрик качества
