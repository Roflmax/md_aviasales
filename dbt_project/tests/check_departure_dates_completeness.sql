-- Проверка полноты дат вылета (7 дней вперёд)
-- Тест ПАДАЕТ если есть пропущенные даты

WITH expected_dates AS (
  -- Генерим все даты от сегодня до +7 дней
  SELECT generate_series(
    CURRENT_DATE,
    CURRENT_DATE + INTERVAL '7 days',
    INTERVAL '1 day'
  )::date AS expected_date
),

actual_dates AS (
  -- Берём уникальные даты из фактических данных
  SELECT DISTINCT DATE(departure_at) AS actual_date
  FROM {{ ref('ods_flight_prices') }}
  WHERE departure_at >= CURRENT_DATE
    AND departure_at <= CURRENT_DATE + INTERVAL '7 days'
),

missing_dates AS (
  -- Находим пропущенные даты
  SELECT
    e.expected_date,
    'Missing departure date in parsed data' AS error_message
  FROM expected_dates e
  LEFT JOIN actual_dates a ON e.expected_date = a.actual_date
  WHERE a.actual_date IS NULL
)

-- Если есть хотя бы 1 пропущенная дата → тест падает
SELECT * FROM missing_dates
