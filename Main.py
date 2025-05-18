import os
import json
import shutil
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,TimestampType,FloatType
import psycopg2
import logging
from dotenv import load_dotenv
from datetime import datetime

# Load .env variables
load_dotenv()

# Get logging settings from environment variables
log_level = os.getenv("LOG_LEVEL", "INFO").upper()
log_file = os.getenv("LOG_FILE", "pipeline.log")

# Generate filename with current date and time
log_dir = "log"
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
log_filename = os.path.join(log_dir,f"pipeline_{timestamp}.log")

# Set up logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    filename=log_filename,
    filemode="a"  # Append mode
)

## Function to create the table
def create_table_in_postgres(create_sql,table):
    try:
        # Connect to your PostgreSQL database
        conn = psycopg2.connect(
            dbname=os.getenv('dbname'), 
            user= os.getenv('user'), 
            password= os.getenv('password'), 
            host=os.getenv('host'), 
            port=os.getenv('port')
        )
        cur = conn.cursor()

        # Execute the CREATE TABLE SQL statement
        cur.execute(create_sql)
        conn.commit()  # Commit the changes

        # print(f"{table} created successfully!")
        logging.info(f"Table created successfully: {table}")
    except Exception as e:
        logging.error(f"Error creating table: {e}")
        # print(f"Error creating table: {e}")
    finally:
        cur.close()
        conn.close()

# Step 1: Spark Session
logging.info("Started")
jar_file_path = 'D:\Kai Sin\Python ETL\spark_temp_file'
spark = SparkSession.builder \
    .appName("load retail_db to postgresql") \
    .config("spark.jars", "D:/Kai Sin/Python ETL/Jar File/postgresql-42.5.1.jar") \
    .config("spark.local.dir", f"{jar_file_path}") \
    .getOrCreate()

# Step 2: PostgreSQL JDBC config
jdbc_url = os.getenv('jdbc_url')
db_properties = {
    "user": os.getenv('user'), 
    "password": os.getenv('password'), 
    "driver": os.getenv('driver')
}

# Step 3: Type mapping
# Map your JSON "data_type" to Spark SQL types
spark_type_map = {
    "string": StringType(),
    "integer": IntegerType(),
    "timestamp": TimestampType(),
    "float": FloatType(),
    "": StringType()
}

# Function to map PySpark types to PostgreSQL
def get_postgres_type(data_type):
    if isinstance(data_type, IntegerType):
        return "INTEGER"
    elif isinstance(data_type, StringType):
        return "TEXT"
    elif isinstance(data_type,TimestampType):
        return "TIMESTAMP"
    elif isinstance(data_type,FloatType):
        return "DECIMAL(12,2)"
    else:
        raise ValueError(f"Unsupported type: {type(data_type)}")

# Step 4: Load schema definitions
logging.info("Loading schema json file")
with open("schemas.json") as f:
    all_schemas = json.load(f)

# Step 5: Loop through files and process
data_dir = Path("D:/Kai Sin/Python ETL/data")
data_files = [f for f in data_dir.rglob("*") if f.is_file()] # get all the file recursively

for file_path in data_files:
    logging.info(f"The data file location: {file_path}")
    if not file_path.is_file():
        logging.info(f"The {file_path} do not have file inside folder")
        continue
    
    table_name = file_path.parent.name # get file name without extension

    if table_name not in all_schemas:
        logging.info(f'No schema found for {table_name}, skipping.')
        # print(f'No schema found for {table_name}, skipping.')
        continue
    
    logging.info(f"Pipline execution started for the table: {table_name}")
    # print(f"Inserting data into table: {table_name}")

    # Define schema
    columns = all_schemas[table_name]
    schema = StructType([
        StructField(col["column_name"], spark_type_map[col["data_type"]], True)
        for col in sorted(columns, key=lambda x: x["column_position"])
    ])

    # Generate SQL to create table
    sql_columns = []
    for field in schema.fields:
        col_type = get_postgres_type(field.dataType)
        sql_columns.append(f"{field.name} {col_type}")

    ## create table in raw
    create_sql = f'CREATE TABLE IF NOT EXISTS "1.raw".{table_name} ({", ".join(sql_columns)});'
    logging.info(f'''Table to be created in postgresql
    QUERY: {create_sql}''')
    # print(create_sql)

    # Step 6: Create the table in PostgreSQL
    create_table_in_postgres(create_sql,table_name)

    try:
        # Load data using Spark
        df = spark.read.option("header","false").schema(schema).csv(str(file_path))

        # Insert into PostgreSQL
        df.write \
            .mode("append") \
            .jdbc(
                url=jdbc_url,
                table=f'"1.raw".{table_name}',
                properties=db_properties
            )
        logging.info(f'Data Inserted from {file_path} into {table_name}')
        # print(f"***Data Inserted from {table_name}")
    except Exception as e:
        # print(f"##ERROR: Failed to insert {table_name}: {e}")
        logging.error("Failed to insert {table_name}: {e}")
    logging.info("Fininshed")


# Stop Spark session
spark.stop()


# Remove all spark-* temp folders
for folder in os.listdir(jar_file_path):
    # if folder.startswith("spark-"):
    folder_path = os.path.join(jar_file_path, folder)
    try:
        shutil.rmtree(folder_path)
        print(f"Deleted temp folder: {folder_path}")
    except Exception as e:
        print(f"Could not delete {folder_path}: {e}")

