### Questions

Question 1. Count of records for the 2024 Yellow Taxi Data (1 point)
20,332,093


Question 2. Estimated amount of data (1 point)
0 MB for the External Table and 155.12 MB for the Materialized Table


Question 3. Why are the estimated number of Bytes different? (1 point)
BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.


Question 4. How many records have a fare_amount of 0? (1 point)
8,333


Question 5. The best strategy to make an optimized table in Big Query (1 point)
Partition by tpep_dropoff_datetime and Cluster on VendorID


Question 6. Estimated processed bytes (1 point)
310.24 MB for non-partitioned table and 26.84 MB for the partitioned table


Question 7. Where is the data for external tables stored? (1 point)
GCP Bucket


Question 8. Always clustering (1 point)
False


Question 9
Cached Results

    BigQuery caches the results of queries for a short period (usually 24 hours). If the same query (or an equivalent query) has been run recently, BigQuery will return the cached result without reprocessing the data.

    In this case, if the query SELECT COUNT(*) has been run before, BigQuery will return the cached count without scanning the table again.

### Query

```sql
# Setup
# Create a materialized table 
CREATE TABLE `de-zoomcamp-448720.ny_yellow_taxi.yellow_tripdata_materialized` AS
  SELECT * FROM `de-zoomcamp-448720.ny_yellow_taxi.yellow_tripdata`
  UNION ALL
  SELECT * FROM `de-zoomcamp-448720.ny_yellow_taxi.yellow_tripdata_2`
  UNION ALL
  SELECT * FROM `de-zoomcamp-448720.ny_yellow_taxi.yellow_tripdata_3`
  UNION ALL
  SELECT * FROM `de-zoomcamp-448720.ny_yellow_taxi.yellow_tripdata_4`
  UNION ALL
  SELECT * FROM `de-zoomcamp-448720.ny_yellow_taxi.yellow_tripdata_5`
  UNION ALL
  SELECT * FROM `de-zoomcamp-448720.ny_yellow_taxi.yellow_tripdata_6`
;


# Create an external table from all parquet files
CREATE EXTERNAL TABLE `de-zoomcamp-448720.ny_yellow_taxi.yellow_tripdata_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dezoomcamp-hw3-2025/*.parquet']
);

# Question 1
# What is count of records for the 2024 Yellow Taxi Data?
SELECT COUNT(*) FROM `de-zoomcamp-448720.ny_yellow_taxi.yellow_tripdata_external`;

# Question 2
# What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?
SELECT COUNT(DISTINCT PULocationID) AS distinct_pulocations 
FROM `de-zoomcamp-448720.ny_yellow_taxi.yellow_tripdata_external`

SELECT DISTINCT COUNT(DISTINCT PULocationID) AS distinct_pulocations 
FROM `de-zoomcamp-448720.ny_yellow_taxi.yellow_tripdata_materialized`

# Question 3
SELECT PULocationID FROM `de-zoomcamp-448720.ny_yellow_taxi.yellow_tripdata_materialized`

SELECT PULocationID, DOLocationID FROM `de-zoomcamp-448720.ny_yellow_taxi.yellow_tripdata_materialized`

# Question 4
SELECT COUNT(*) as zero_fare_records
FROM `de-zoomcamp-448720.ny_yellow_taxi.yellow_tripdata_external`
WHERE fare_amount = 0
;

# Question 5
CREATE TABLE `de-zoomcamp-448720.ny_yellow_taxi.yellow_tripdata_optimized`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
  SELECT * FROM `de-zoomcamp-448720.ny_yellow_taxi.yellow_tripdata_materialized`
;

# Question 6
SELECT DISTINCT VendorID
FROM `de-zoomcamp-448720.ny_yellow_taxi.yellow_tripdata_materialized`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15'
;

SELECT DISTINCT VendorID
FROM `de-zoomcamp-448720.ny_yellow_taxi.yellow_tripdata_optimized`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15'
;

# Question 9
SELECT COUNT(*) 
FROM `de-zoomcamp-448720.ny_yellow_taxi.yellow_tripdata_materialized`
```

