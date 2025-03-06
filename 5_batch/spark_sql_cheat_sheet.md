## **Spark SQL with PySpark Cheat Sheet**

### **1. Setting Up Spark SQL**
To use Spark SQL, you need to create a `SparkSession`, which is the entry point for working with structured data.

```python
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("SparkSQLExample") \
    .getOrCreate()
```

---

### **2. Creating DataFrames**
DataFrames are the primary abstraction in Spark SQL. They are distributed collections of data organized into named columns.

#### **From a Collection**
```python
data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)
df.show()
```

#### **From a CSV File**
```python
df = spark.read.csv("path/to/file.csv", header=True, inferSchema=True)
df.show()
```

#### **From a JSON File**
```python
df = spark.read.json("path/to/file.json")
df.show()
```

#### **From a Database**
```python
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost/db") \
    .option("dbtable", "tablename") \
    .option("user", "username") \
    .option("password", "password") \
    .load()
```

---

### **3. Basic DataFrame Operations**

#### **Viewing Data**
- **`show(n)`**: Display the first `n` rows.
  ```python
  df.show(5)
  ```
- **`printSchema()`**: Print the schema of the DataFrame.
  ```python
  df.printSchema()
  ```

#### **Selecting Columns**
- **`select()`**: Select specific columns.
  ```python
  df.select("Name", "Age").show()
  ```

#### **Filtering Rows**
- **`filter()`** or **`where()`**: Filter rows based on a condition.
  ```python
  df.filter(df["Age"] > 30).show()
  ```

#### **Adding Columns**
- **`withColumn()`**: Add or replace a column.
  ```python
  df.withColumn("AgePlus10", df["Age"] + 10).show()
  ```

#### **Renaming Columns**
- **`withColumnRenamed()`**: Rename a column.
  ```python
  df.withColumnRenamed("Age", "Years").show()
  ```

#### **Dropping Columns**
- **`drop()`**: Remove a column.
  ```python
  df.drop("Age").show()
  ```

#### **Aggregations**
- **`groupBy()`**: Group data and perform aggregations.
  ```python
  df.groupBy("Name").agg({"Age": "avg"}).show()
  ```

---

### **4. Running SQL Queries**
You can run SQL queries on DataFrames by creating a temporary view.

#### **Registering a Temporary View**
```python
df.createOrReplaceTempView("people")
```

#### **Running a SQL Query**
```python
result = spark.sql("SELECT Name, Age FROM people WHERE Age > 30")
result.show()
```

---

### **5. Joins**
Spark SQL supports various types of joins:
- **Inner Join**: `join(otherDF, on="key", how="inner")`
- **Left Join**: `join(otherDF, on="key", how="left")`
- **Right Join**: `join(otherDF, on="key", how="right")`
- **Full Outer Join**: `join(otherDF, on="key", how="outer")`

#### Example:
```python
df1 = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "Name"])
df2 = spark.createDataFrame([(1, "Engineering"), (2, "Sales")], ["id", "Dept"])

# Inner Join
df1.join(df2, on="id", how="inner").show()
```

---

### **6. User-Defined Functions (UDFs)**
You can define custom functions and use them in Spark SQL.

#### Example:
```python
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

# Define a UDF
def square(x):
    return x * x

square_udf = udf(square, IntegerType())

# Register the UDF
spark.udf.register("square_udf", square_udf)

# Use the UDF in a DataFrame
df.withColumn("AgeSquared", square_udf(df["Age"])).show()
```

---

### **7. Built-In Functions**
Spark SQL provides a wide range of built-in functions for data manipulation. Import them from `pyspark.sql.functions`.

#### Common Functions:
- **`col()`**: Reference a column.
- **`lit()`**: Create a literal value.
- **`concat()`**: Concatenate strings.
- **`sum()`**, **`avg()`**, **`min()`**, **`max()`**: Aggregation functions.
- **`when()`**, **`otherwise()`**: Conditional logic.

#### Example:
```python
from pyspark.sql.functions import col, when

df.withColumn("AgeGroup", when(col("Age") < 30, "Young").otherwise("Old")).show()
```

---

### **8. Writing Data**
You can write DataFrames to various formats.

#### **To CSV**
```python
df.write.csv("output/path", header=True)
```

#### **To JSON**
```python
df.write.json("output/path")
```

#### **To Parquet**
```python
df.write.parquet("output/path")
```

#### **To a Database**
```python
df.write \
  .format("jdbc") \
  .option("url", "jdbc:postgresql://localhost/db") \
  .option("dbtable", "tablename") \
  .option("user", "username") \
  .option("password", "password") \
  .save()
```

---

### **9. Performance Optimization**
- **Caching**: Cache frequently used DataFrames.
  ```python
  df.cache()
  ```
- **Broadcast Joins**: Use broadcast joins for small tables.
  ```python
  from pyspark.sql.functions import broadcast
  df1.join(broadcast(df2), on="key").show()
  ```
- **Partitioning**: Repartition DataFrames for better parallelism.
  ```python
  df.repartition(10)
  ```

---

### **10. Example Workflow**
```python
# Create DataFrame
data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Register as a temporary view
df.createOrReplaceTempView("people")

# Run SQL query
result = spark.sql("SELECT Name, Age FROM people WHERE Age > 30")
result.show()

# Perform DataFrame operations
df.filter(df["Age"] > 30).withColumn("AgePlus10", df["Age"] + 10).show()

# Write to CSV
df.write.csv("output/path", header=True)
```

---

### **11. Stopping the SparkSession**
Always stop the `SparkSession` when done.
```python
spark.stop()
```

## Best Practices

When working with **Spark SQL** and **PySpark**, following best practices ensures efficient, maintainable, and scalable code. Here are some key best practices to keep in mind:

---

### **1. Optimize Data Ingestion**
- **Use Schema Inference Sparingly**: While `inferSchema=True` is convenient, it can be slow for large datasets. Define schemas explicitly using `StructType` and `StructField`.
  ```python
  from pyspark.sql.types import StructType, StructField, StringType, IntegerType

  schema = StructType([
      StructField("Name", StringType(), True),
      StructField("Age", IntegerType(), True)
  ])
  df = spark.read.csv("path/to/file.csv", schema=schema, header=True)
  ```
- **Leverage Partitioned Data**: If your data is partitioned (e.g., by date), use partition discovery to read only relevant partitions.
  ```python
  df = spark.read.parquet("path/to/partitioned_data/year=2023/month=10")
  ```

---

### **2. Efficient Data Processing**
- **Avoid Shuffles**: Shuffles (data redistribution across nodes) are expensive. Minimize them by:
  - Using `broadcast` joins for small tables.
  - Using `coalesce` instead of `repartition` when reducing partitions.
- **Use Built-In Functions**: Prefer Spark SQL built-in functions over Python UDFs, as they are optimized for distributed execution.
  ```python
  from pyspark.sql.functions import col, when

  df.withColumn("Category", when(col("Age") > 30, "Old").otherwise("Young")).show()
  ```
- **Leverage Caching**: Cache intermediate DataFrames that are reused multiple times.
  ```python
  df.cache()
  ```

---

### **3. Write Efficient Queries**
- **Push Down Filters**: Apply filters as early as possible to reduce the amount of data processed.
  ```python
  df.filter(df["Age"] > 30).select("Name", "Age").show()
  ```
- **Avoid `SELECT *`**: Select only the columns you need to reduce memory and processing overhead.
  ```python
  df.select("Name", "Age").show()
  ```
- **Use SQL for Complex Queries**: For complex logic, use SQL queries instead of chaining multiple DataFrame operations.
  ```python
  df.createOrReplaceTempView("people")
  result = spark.sql("SELECT Name, Age FROM people WHERE Age > 30")
  ```

---

### **4. Memory and Resource Management**
- **Tune Executor Memory**: Allocate sufficient memory to executors to avoid out-of-memory errors.
  ```python
  spark = SparkSession.builder \
      .appName("MyApp") \
      .config("spark.executor.memory", "4g") \
      .getOrCreate()
  ```
- **Monitor Garbage Collection**: High GC overhead can slow down your application. Tune JVM settings if necessary.
- **Use Broadcast Variables**: For small datasets used in joins or lookups, use broadcast variables to reduce shuffling.
  ```python
  small_df = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "value"])
  broadcast_df = broadcast(small_df)
  df.join(broadcast_df, on="id").show()
  ```

---

### **5. Data Partitioning**
- **Repartition Wisely**: Repartition DataFrames based on the size of your data and cluster resources.
  ```python
  df = df.repartition(100)  # Repartition into 100 partitions
  ```
- **Use Partitioned Data**: When writing data, partition it by frequently queried columns (e.g., date).
  ```python
  df.write.partitionBy("year", "month").parquet("output/path")
  ```

---

### **6. Debugging and Logging**
- **Check Execution Plans**: Use `explain()` to analyze the physical and logical plans of your queries.
  ```python
  df.filter(df["Age"] > 30).explain()
  ```
- **Enable Logging**: Use Spark's logging capabilities to debug and monitor your application.
  ```python
  import logging
  logging.basicConfig(level=logging.INFO)
  ```

---

### **7. Writing Data**
- **Use Efficient File Formats**: Prefer columnar formats like Parquet or ORC for storage, as they are optimized for read/write performance.
  ```python
  df.write.parquet("output/path")
  ```
- **Avoid Small Files**: When writing data, ensure partitions are not too small. Use `coalesce` or `repartition` to control the number of output files.
  ```python
  df.coalesce(10).write.parquet("output/path")
  ```

---

### **8. Testing and Validation**
- **Unit Testing**: Use libraries like `pytest` to test your PySpark code.
  ```python
  def test_filter():
      data = [("Alice", 34), ("Bob", 45)]
      df = spark.createDataFrame(data, ["Name", "Age"])
      result = df.filter(df["Age"] > 30).collect()
      assert len(result) == 1
  ```
- **Data Validation**: Validate data quality using assertions or libraries like `Great Expectations`.

---

### **9. Documentation and Code Readability**
- **Use Meaningful Variable Names**: Choose descriptive names for DataFrames, columns, and variables.
- **Add Comments**: Document complex logic or transformations.
- **Modularize Code**: Break your code into functions or classes for better readability and reusability.

---

### **10. Cluster Configuration**
- **Dynamic Allocation**: Enable dynamic allocation to scale resources based on workload.
  ```python
  spark = SparkSession.builder \
      .appName("MyApp") \
      .config("spark.dynamicAllocation.enabled", "true") \
      .getOrCreate()
  ```
- **Specify Cores and Executors**: Configure the number of cores and executors based on your cluster size.
  ```python
  spark = SparkSession.builder \
      .appName("MyApp") \
      .config("spark.executor.cores", "4") \
      .config("spark.executor.instances", "10") \
      .getOrCreate()
  ```

---

### **11. Stop the SparkSession**
Always stop the `SparkSession` when your application is done to release resources.
```python
spark.stop()
```

---

### **Example Workflow with Best Practices**
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("BestPracticesExample") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

# Read data with explicit schema
data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Filter and cache
filtered_df = df.filter(col("Age") > 30).cache()

# Broadcast a small DataFrame
small_df = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "value"])
result = filtered_df.join(broadcast(small_df), on="id")

# Write output
result.write.parquet("output/path")

# Stop SparkSession
spark.stop()
```
