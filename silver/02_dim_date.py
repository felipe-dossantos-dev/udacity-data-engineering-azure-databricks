# Databricks notebook source
from silver import *
from pyspark.sql.functions import explode, sequence, to_date

SILVER_TABLE_NAME = "divvy.dim_date"

spark.sql(f"DROP TABLE IF EXISTS {SILVER_TABLE_NAME};")

beginDate = "2015-01-01"
endDate = "2030-12-31"

spark.sql(
    f"select explode(sequence(to_date('{beginDate}'), to_date('{endDate}'), interval 1 day)) as date"
).createOrReplaceTempView("dates")

spark.sql(
    """
create or replace table divvy.dim_date
using delta
as select
  year(date) * 10000 + month(date) * 100 + day(date) as date_int,
  date,
  year(date) AS year,
  date_format(date, 'MMMM') as calendar_month,
  month(date) as month,
  date_format(date, 'EEEE') as calendar_day,
  dayofweek(date) as day_of_week,
  weekday(date) + 1 as day_of_week_start_monday,
  case
    when weekday(date) < 5 then 'Y'
    else 'N'
  end as is_week_day,
  dayofmonth(date) as day_of_month,
  case
    when date = last_day(date) then 'Y'
    else 'N'
  end as is_last_day_of_month,
  dayofyear(date) as day_of_year,
  weekofyear(date) as week_of_year_iso,
  quarter(date) as quarter_of_year
from
  dates
"""
)

spark.sql("optimize divvy.dim_date zorder by (date)")
