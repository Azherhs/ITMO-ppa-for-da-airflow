-- Доля учебных планов в статусе одобрено

SELECT
  "cdm"."up_wp_statuses__"."state_name" AS "state_name",
  COUNT(*) AS "count"
FROM
  "cdm"."up_wp_statuses__"
GROUP BY
  "cdm"."up_wp_statuses__"."state_name"
ORDER BY
  "cdm"."up_wp_statuses__"."state_name" ASC

-- Корректность заполнения трудоемкости
SELECT
  "source"."laboriousness_status" AS "laboriousness_status",
  "source"."count" AS "count"
FROM
  (
    SELECT
      "source"."laboriousness_status" AS "laboriousness_status",
      "source"."count" AS "count"
    FROM
      (
        -- Calculate the count of educational plans with correct and incorrect laboriousness
        SELECT
          CASE
            WHEN (
              laboriousness = 240

   AND qualification = 'бакалавриат'
            )

    OR (
              laboriousness = 120
              AND qualification = 'магистратура'
            ) THEN 'Корректно'
            ELSE 'Некорректно'
          END AS laboriousness_status,
          COUNT(*) AS count
        FROM
          cdm.up_lab

WHERE
          year = 2023

GROUP BY
          laboriousness_status
      ) AS "source"

LIMIT
      1048575
  ) AS "source"
LIMIT
  1048575

-- Заполнение аннотаций
WITH RankedData AS (
    SELECT
        *,
        CASE
            WHEN {{time_interval}} = 'Минута' THEN TO_CHAR(update_ts, 'YYYY-MM-DD HH24:MI')
            WHEN {{time_interval}} = 'Час' THEN TO_CHAR(update_ts, 'YYYY-MM-DD HH24')
            WHEN {{time_interval}} = 'День' THEN TO_CHAR(update_ts, 'YYYY-MM-DD')
            WHEN {{time_interval}} = 'Неделя' THEN TO_CHAR(update_ts, 'IYYY-IW')
        END AS time_interval,
        COUNT(*) OVER (ORDER BY update_ts) AS running_total,
        SUM(CASE WHEN wp_description IS NOT NULL AND wp_description <> '' THEN 1 ELSE 0 END) OVER (ORDER BY update_ts) AS running_completed_annotations
    FROM cdm.wp_statuses_aggregation
)
SELECT
    time_interval AS "Временной период",
    MAX(running_total) AS "Всего аннотаций",
    MAX(running_completed_annotations) AS "Заполненных аннотаций",
    MAX(running_completed_annotations) / MAX(running_total)::FLOAT AS "Доля"
FROM RankedData
GROUP BY time_interval
ORDER BY time_interval;

-- Доля предметов в статусе "одобрено" в динамике
WITH RankedData AS (
    SELECT
        *,
        CASE
            WHEN {{time_interval}} = 'Минута' THEN TO_CHAR(update_ts, 'YYYY-MM-DD HH24:MI')
            WHEN {{time_interval}} = 'Час' THEN TO_CHAR(update_ts, 'YYYY-MM-DD HH24')
            WHEN {{time_interval}} = 'День' THEN TO_CHAR(update_ts, 'YYYY-MM-DD')
            WHEN {{time_interval}} = 'Неделя' THEN TO_CHAR(update_ts, 'IYYY-IW')
        END AS time_interval,
        COUNT(*) OVER (ORDER BY update_ts) AS running_total,
        SUM(CASE WHEN cop_state = 'AC' THEN 1 ELSE 0 END) OVER (ORDER BY update_ts) AS ac_count
    FROM cdm.wp_statuses_aggregation
)
SELECT
    time_interval AS "Временной интервал",
    MAX(running_total) AS "Всего предметов",
    MAX(ac_count) AS "Со статусом одбрено",
    MAX(ac_count) / MAX(running_total)::FLOAT AS "Доля"
FROM RankedData
GROUP BY time_interval
ORDER BY time_interval;
