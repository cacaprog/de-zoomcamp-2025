# Data Engineering - Module 1 - Homework
https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2025/01-docker-terraform/homework.md


## Question 1

1. Run Docker
```bash
docker run -it --entrypoint bash python:3.12.8
```
2. pip version
```bash
pip --version
```

```output
pip 24.3.1 from /usr/local/lib/python3.12/site-packages/pip (python 3.12)
```

## Question 2
What's the `hostname` and `port` that pgadmin should use to connect to the postgres database?

```bash
services:
  db:
    container_name: postgres
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: 'postgres'
      POSTGRES_PASSWORD: 'postgres'
      POSTGRES_DB: 'ny_taxi'
    ports:
      - '5433:5432'
    volumes:
      - vol-pgdata:/var/lib/postgresql/data

  pgadmin:
    container_name: pgadmin
    image: dpage/pgadmin4:latest
    environment:
      PGADMIN_DEFAULT_EMAIL: "pgadmin@pgadmin.com"
      PGADMIN_DEFAULT_PASSWORD: "pgadmin"
    ports:
      - "8080:80"
    volumes:
      - vol-pgadmin_data:/var/lib/pgadmin  

volumes:
  vol-pgdata:
    name: vol-pgdata
  vol-pgadmin_data:
    name: vol-pgadmin_data
```
Answer:

- hostname: db
- port: 5432


Why?

In *Docker Compose*, services can communicate with each other **using their service name as the hostname**
The postgres service is named `db` in the compose file
While the container_name is "postgres", **services reference each other by service name**

Inside the Docker network, services communicate using the **internal container port** (right side of the port mapping)
The port mapping 5433:5432 means:
- 5433 is the external port on your host machine
- 5432 is the internal port inside the container

## Prepare Postgres

1. Start containers (postgres e pgadmin4)
```bash
docker-compose up -d
```

2. Open pgadmin on browser and connect to the PostgreSQL server
`http://localhost:8080`

Connect to the PostgreSQL Server

    Add a New Server:
        Click on the "Add New Server" button (or right-click on "Servers" in the navigation panel and select "Create > Server").

    Configure the Server Connection:

        In the General tab:
            Name: Any name, e.g., TaxiDB.

        In the Connection tab:
            Host name/address: taxidb
            Port: 5432 (the default PostgreSQL port inside the container)
            Maintenance database: ny_green_taxi (your POSTGRES_DB)
            Username: user
            Password: mysecretpass

    Save the Server:
        Click Save to connect to the database.

3. ingesting data to postgres container

```bash
    python ingest_data.py \
    --user user \
    --password mysecretpass \
    --host localhost \
    --port 5433 \
    --db ny_green_taxi \
    --table_name_1 green_taxi \
    --table_name_2 zones 
```

4. Refresh the table and check the data


## Question 3
During the period of October 1st 2019 (inclusive) and November 1st 2019 (exclusive), how many trips, respectively, happened:

    - Up to 1 mile: 104.802
    - In between 1 (exclusive) and 3 miles (inclusive): 198,924
    - In between 3 (exclusive) and 7 miles (inclusive): 109,603
    - In between 7 (exclusive) and 10 miles (inclusive): 27,678
    - Over 10 miles: 35,189

Queries
```sql
SELECT COUNT(*) as trips
FROM green_taxi
WHERE lpep_pickup_datetime >= '2019-10-01' 
  AND lpep_pickup_datetime < '2019-11-01'
  AND lpep_dropoff_datetime < '2019-11-01'
  AND trip_distance <= 1;


SELECT COUNT(*) as trips
FROM green_taxi
WHERE lpep_pickup_datetime >= '2019-10-01' 
  AND lpep_pickup_datetime < '2019-11-01'
  AND lpep_dropoff_datetime < '2019-11-01'
  AND trip_distance > 1
  AND trip_distance <= 3;


SELECT COUNT(*) as trips
FROM green_taxi
WHERE lpep_pickup_datetime >= '2019-10-01' 
  AND lpep_pickup_datetime < '2019-11-01'
  AND lpep_dropoff_datetime < '2019-11-01'
  AND trip_distance > 3
  AND trip_distance <= 7;


SELECT COUNT(*) as trips
FROM green_taxi
WHERE lpep_pickup_datetime >= '2019-10-01' 
  AND lpep_pickup_datetime < '2019-11-01'
  AND lpep_dropoff_datetime < '2019-11-01'
  AND trip_distance > 7
  AND trip_distance <= 10;


SELECT COUNT(*) as trips
FROM green_taxi
WHERE lpep_pickup_datetime >= '2019-10-01' 
  AND lpep_pickup_datetime < '2019-11-01'
  AND lpep_dropoff_datetime < '2019-11-01'
  AND trip_distance > 10;
```

## Question 4
Which was the pick up day with the longest trip distance? Use the pick up time for your calculations.

Tip: For every day, we only care about one single trip with the longest distance.

    2019-10-11: 95.75
    2019-10-24: 90.75 
    2019-10-26: 91.96
    2019-10-31: 515.89

```SQL
-- Question 4
SELECT 
    DATE(lpep_pickup_datetime) as pickup_date,
    MAX(trip_distance) as longest_distance
FROM green_taxi
GROUP BY DATE(lpep_pickup_datetime)
ORDER BY longest_distance DESC
LIMIT 10;
```

## Question 5
Which were the top pickup locations with over 13,000 in total_amount (across all trips) for 2019-10-18?

Consider only lpep_pickup_datetime when filtering by date.

   ** East Harlem North, East Harlem South, Morningside Heights - CORRECT**
    East Harlem North, Morningside Heights
    Morningside Heights, Astoria Park, East Harlem South
    Bedford, East Harlem North, Astoria Park

```SQL
SELECT 
    z."Zone" as pickup_zone,    -- Get the zone name from zones table and alias it as pickup_zone
    ROUND(SUM(t.total_amount)) as total_amount  -- Calculate the sum of all total_amount values
FROM green_taxi t    -- Main table with taxi trip data, aliased as 't'
JOIN zones z ON t."PULocationID" = z."LocationID"    -- Join with zones table to get zone names
-- matching pickup location IDs with zone locations
WHERE DATE(t.lpep_pickup_datetime) = '2019-10-18'   -- Filter only trips from October 18, 2019
GROUP BY z."Zone"      -- Group results by zone name
HAVING SUM(t.total_amount) > 13000     -- Only show zones where total earnings exceeded $13,000
ORDER BY total_amount DESC;     -- Sort by total amount in descending order
```

## Question 6

For the passengers picked up in October 2019 in the zone name "East Harlem North" which was the drop off zone that had the largest tip?

We need the name of the zone, not the ID.

    Yorkville West
**  JFK Airport - 87,3 **
    East Harlem North
    East Harlem South

```sql
SELECT 
    dropoff_zone."Zone" as dropoff_zone,
    MAX(t.tip_amount) as max_tip
FROM green_taxi t
JOIN zones pickup_zone 
    ON t."PULocationID" = pickup_zone."LocationID"
JOIN zones dropoff_zone 
    ON t."DOLocationID" = dropoff_zone."LocationID"
WHERE 
    DATE(t.lpep_pickup_datetime) >= '2019-10-01'
    AND DATE(t.lpep_pickup_datetime) < '2019-11-01'
    AND pickup_zone."Zone" = 'East Harlem North'
GROUP BY dropoff_zone."Zone"
ORDER BY max_tip DESC
LIMIT 1;
```

## Terraform

Which of the following sequences, respectively, describes the workflow for:

    Downloading the provider plugins and setting up backend,
    Generating proposed changes and auto-executing the plan
    Remove all resources managed by terraform`

Answers:

    terraform import, terraform apply -y, terraform destroy
    teraform init, terraform plan -auto-apply, terraform rm
    terraform init, terraform run -auto-approve, terraform destroy
   ** terraform init, terraform apply -auto-approve, terraform destroy **
    terraform import, terraform apply -y, terraform rm




