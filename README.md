# Aviasales Price Tracker

EL-пайплайн для сбора и анализа цен на авиабилеты.

## Описание

Сервис для сбора и анализа цен на авиабилеты из Aviasales API. Данные сохраняются в MongoDB и затем переносятся в PostgreSQL для аналитики.

## Архитектура

```
MongoDB  ──>  Airflow (EL)  ──>  PostgreSQL (STG)
   ↑
Python App (FastAPI)
```

## Структура проекта

```
├── docker-compose.yml
├── app/                    # FastAPI сервис (сбор данных → MongoDB)
├── airflow/
│   └── dags/
│       └── el_pipeline.py  # EL: MongoDB → PostgreSQL
└── sql/
    └── init.sql            # Схема stg.flight_prices_raw (JSONB)
```

## Быстрый старт

### 1. Клонирование и настройка

```bash
# Скопировать шаблон переменных окружения
cp .env.example .env

# Отредактировать .env и указать свой API токен
```

### 2. Запуск

```bash
docker-compose up -d
```

### 3. Сервисы

| Сервис | URL |
|--------|-----|
| FastAPI | http://localhost:8000 |
| Airflow | http://localhost:8081 (admin/admin) |
| PostgreSQL | localhost:5432 |
| MongoDB | localhost:27017 |

## API Endpoints

| Метод | URL | Описание |
|-------|-----|----------|
| GET | `/health` | Проверка состояния сервиса |
| POST | `/fetch-prices` | Загрузить цены из Aviasales API |
| GET | `/prices` | Получить сохранённые цены |
| GET | `/prices/stats` | Статистика по маршрутам |

## Swagger UI

http://localhost:8000/docs
