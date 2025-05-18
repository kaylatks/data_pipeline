# Data Pipeline

## This pipeline basically is read the data from local file and insert into database using spark.

### 1. Create spark session to read the data from file with data type mapping.
### 2. Create table in the connected database if not exist.
### 3. Insert the data into table respectively.
### 4. Every step will be record in the log file to track the performace.
