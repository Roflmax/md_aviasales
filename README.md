# Aviasales Price Tracker

ELT-пайплайн для анализа цен на авиабилеты LED → SVX.

## Структура проекта

```
md_aviasales/
├── app/                    # FastAPI сервис (API → MongoDB)
├── airflow/
│   └── dags/
│       ├── fetch_prices_dag.py   # Сбор данных из API
│       ├── el_pipeline.py        # MongoDB → PostgreSQL
│       └── dbt_pipeline.py       # dbt трансформации
├── dbt_project/            # DBT проект
│   └── models/
│       ├── ods/            # ODS слой (парсинг JSONB)
│       └── dm/             # DM витрины (аналитика)
├── sql/                    # SQL скрипты инициализации
└── docker-compose.yml
```

## Слои данных

| Слой | Описание |
|------|----------|
| RAW | MongoDB — сырые данные из API |
| STG | PostgreSQL JSONB — staging |
| ODS | Распарсенные данные |
| DM | Аналитические витрины |

## Запуск

```bash
cp .env.example .env
docker compose up -d
```

## Сервисы

| Сервис | URL |
|--------|-----|
| Swagger | http://localhost:8000/docs |
| Airflow | http://localhost:8081 (admin/admin) |
| PostgreSQL | localhost:5432 |
| MongoDB | localhost:27017 |
