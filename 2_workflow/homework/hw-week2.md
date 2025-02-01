https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2025/02-workflow-orchestration/homework.md


1. Within the execution for Yellow Taxi data for the year 2020 and month 12: what is the uncompressed file size (i.e. the output file yellow_tripdata_2020-12.csv of the extract task)?

- 364.7 MB

![alt text](image.png)


2. What is the rendered value of the variable file when the inputs taxi is set to green, year is set to 2020, and month is set to 04 during execution?

- green_tripdata_2020-04.csv


3. How many rows are there for the Yellow Taxi data for all CSV files in the year 2020?

- 24,648,499

```SQL
SELECT COUNT(*) FROM `kestra-sandbox-449521.de_zoomcamp.yellow_tripdata` 
WHERE tpep_pickup_datetime >= "2020-01-01"
AND tpep_dropoff_datetime < "2021-01-01";

-- output
24648038
```

4. How many rows are there for the Green Taxi data for all CSV files in the year 2020?

- 1,734,051

```SQL
SELECT COUNT(*) FROM `kestra-sandbox-449521.de_zoomcamp.green_tripdata` 
WHERE lpep_pickup_datetime >= "2020-01-01"
AND lpep_dropoff_datetime < "2021-01-01";

-- output
1734027
```

5. How many rows are there for the Yellow Taxi data for the March 2021 CSV file?

- 1,925,152

```SQL
SELECT COUNT(*) 
FROM `kestra-sandbox-449521.de_zoomcamp.yellow_tripdata_2021_03`;

-- OUTPUT
1,925,152
```


6. How would you configure the timezone to New York in a Schedule trigger?

- Add a timezone property set to America/New_York in the Schedule trigger configuration

```yaml
triggers:
  - id: green_schedule
    type: io.kestra.plugin.core.trigger.Schedule
    cron: "0 9 1 * *"
    timezone: "America/New_York"
    inputs:
      taxi: green
```


Learn in public: https://www.linkedin.com/posts/cairocananea_dataengineeringzoomcamp-dataengineering-mydatajourney-activity-7291549002611765249-KYUa?utm_source=share&utm_medium=member_desktop
