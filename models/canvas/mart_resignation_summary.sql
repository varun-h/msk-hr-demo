WITH source_column_selections AS (
  /* Each record represents an employee with enriched personal information and the positions they hold. This helps measure employee demographic and geographical distribution, overall retention and turnover, and compensation analysis of their employees. */
  SELECT
    employee_id,
    worker_id,
    worker_code,
    is_employed,
    first_name,
    last_name,
    business_title,
    hire_date,
    departure_date,
    days_as_worker,
    is_terminated,
    is_regrettable_termination,
    primary_termination_category,
    primary_termination_reason
  FROM {{ ref('workday', 'workday__employee_overview') }}
), filter_conditions AS (
  SELECT
    *
  FROM source_column_selections
  WHERE
    is_terminated = 'TRUE' AND departure_date >= '2017-01-01'
), formula AS (
  SELECT
    *,
    CASE
      WHEN is_regrettable_termination = TRUE
      THEN 'VOLUNTARY RESIGNATION'
      ELSE 'INVOLUNTARY RESIGNATION'
    END AS resignation_type,
    CASE WHEN is_terminated = TRUE THEN 1 ELSE 0 END AS resignation_count
  FROM filter_conditions
), mart_resignation_summary_sql AS (
  SELECT
    *
  FROM formula
)
SELECT
  *
FROM mart_resignation_summary_sql