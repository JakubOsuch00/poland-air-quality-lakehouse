# poland-air-quality-lakehouse

Project that downloads air quality data from GIOŚ (archive since 2000 + live API) and weather data from Open-Meteo, processes it with PySpark and saves it as Delta tables. Everything runs locally in Docker.

Used technologies:
- PySpark 4.0 (standalone cluster)
- Delta Lake 4.0
- SeaweedFS as S3 storage
- Airflow 3.3
- Streamlit
- GitHub Actions

## Layers

Bronze - raw data (zips from the archive, xlsx sheets saved as csv, API and weather responses).

Silver - cleaned data:
- `measurements` - all pollutants, hourly and daily values
- `stations`, `sensors`, `station_code_map` - stations and mapping of old station codes
- `station_history` - SCD2 history of stations from the API
- `weather_hourly`

Gold:
- `dim_station`
- `daily_station` - daily means (a day needs at least 18 of 24 hours)
- `annual_compliance` - annual means and days over EU limits and WHO guidelines
- `monthly_city`
- `daily_pm_weather` - daily PM10/PM2.5 with weather
- `dq_results` - data quality check results

## Problems with the data

- the archive files have a different number of header rows in different years
- in 2016-2018 numbers are saved as text with a comma
- timestamps have a few milliseconds of error from Excel, so they are rounded to full hours
- archive time is always UTC+1 and the API uses summer time, both are converted to UTC
- old files use old station codes and one old code can belong to two stations, so the mapping checks which station was active in that year
- CO is in mg/m3 in the archive and in µg/m3 in the API
- manual sensors return error 400 in the API, so they are skipped
- Open-Meteo free plan has an hourly limit, so the weather download can be continued later

Wrong values (negative or too big) stay in silver with a `qa_flag` and are not used in calculations. API data does not overwrite archive data.

## How to run

You need Docker with about 8 GB RAM.

```bash
cp .env.example .env
docker compose up -d --build
```

- Airflow - http://localhost:8080
- Spark - http://localhost:8081
- Dashboard - http://localhost:8501

In Airflow run `gios_archive_backfill` with e.g. `{"years": [2018, 2023, 2024]}`, one year takes about 3-4 minutes. `weather_backfill` works the same way but is slow because of the API limit. `air_quality_daily` is the daily run. All DAGs are paused at the start.

Tests:

```bash
pip install -e ".[spark,dev]"
pytest
```

## Results

PM10, stations with at least 75% of data in a year:

| | 2005 | 2018 | 2023 |
|---|---|---|---|
| stations | 169 | 222 | 251 |
| not meeting EU limits | 98 | 161 | 4 |
| mean of annual means [µg/m³] | 32.8 | 32.3 | 20.7 |

Daily PM10 and PM2.5 in 2023 (low wind = below 10 km/h):

| heating season | low wind | PM10 | PM2.5 | days |
|---|---|---|---|---|
| no | no | 17.2 | 10.0 | 21209 |
| no | yes | 19.7 | 12.1 | 18311 |
| yes | no | 19.3 | 14.6 | 42020 |
| yes | yes | 33.0 | 26.0 | 12633 |

Correlation of daily PM10 with weather in 2023:
- boundary layer height - -0.44
- wind speed - -0.33
- temperature - -0.24

Conclusions:
- PM10 went down a lot between 2018 and 2023
- WHO guidelines are much stricter, in 2023 only about 7% of stations met them
- the worst days are in the heating season without wind
- boundary layer height has the biggest effect, when it is low the pollution stays near the ground

## Project structure

```
conf/          config
src/           ingestion and spark code
dags/          Airflow DAGs
dashboard/     Streamlit app
docker/        Dockerfiles
tests/         tests
```

## TODO

- heavy metals data
- Trino for SQL
- streaming instead of daily API calls
