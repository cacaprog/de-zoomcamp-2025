For this homework we will be using the Yellow 2024-10 data from the official website:

!wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-10.parquet

### Question 1: Install Spark and PySpark

    Install Spark
    Run PySpark
    Create a local spark session
    Execute spark.version.

What's the output?
*3.3.2*

### Question 2: Yellow October 2024

- Read the October 2024 Yellow into a Spark Dataframe.
- Repartition the Dataframe to 4 partitions and save it to parquet.

What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.
*25MB*


### Question 3: Count records

How many taxi trips were there on the 15th of October?

Consider only trips that started on the 15th of October.

    85,567
    105,567
    *125,567*
    145,567

### Question 4: Longest trip

What is the length of the longest trip in the dataset in hours?

*162*


### Question 5: User Interface

Spark’s User Interface which shows the application's dashboard runs on which local port?

*4040*

## Question 6: Least frequent pickup location zone

Load the zone lookup data into a temp view in Spark:
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv

Using the zone lookup data and the Yellow October 2024 data, what is the name of the LEAST frequent pickup location Zone?

*Governor's Island/Ellis Island/Liberty Island*
