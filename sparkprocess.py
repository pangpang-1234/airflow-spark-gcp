import argparse
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType
from google.cloud import storage
from google.cloud import bigquery

# Add arguments for getting variables from external environment
parser = argparse.ArgumentParser()
parser.add_argument('--crypto', help='Choose cryptocurrency (BTC, ADA, LINK)')
parser.add_argument('--bucket', help='pass bucketname')
parser.add_argument('--dataset', help='pass datasetname')

# Get variables from json config file and define to parameters
args = parser.parse_args()
CRYPTOCURRENCY = args.crypto
BUCKET_NAME = args.bucket
DATASET_NAME = args.dataset

# Capitalize string
CRYPTOCURRENCY = CRYPTOCURRENCY.upper()

# Create spark session
spark = SparkSession.builder \
  .appName('airflow') \
  .config('spark.jars.packages','com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.24.2') \
  .getOrCreate()

# List blob
blobs = storage.Client().list_blobs(BUCKET_NAME)

for blob in blobs:
  
    # file_name.append(blob.name)
    if CRYPTOCURRENCY in blob.name:
      
        # Read csv from GCS bucket
        df = spark.read.csv(f'gs://{BUCKET_NAME}/{blob.name}', header=True)

# Add column Average which find average from Open, High, Low column
marksColumns = [F.col('Open'), F.col('High'), F.col('Low')]
averageFunc = sum(x for x in marksColumns)/len(marksColumns)
df=df.withColumn('Average', averageFunc)

# Add column VolumeDivClose which divide Volume by Close
df=df.withColumn('VolumeDivClose', F.col('Volume') / F.col('Close'))

# Cast column Date to Datetype
df=df.withColumn('Date', F.col('Date').cast(DateType())) \
    .withColumn('Open', F.col('Open').cast(DoubleType())) \
    .withColumn('High', F.col('High').cast(DoubleType())) \
    .withColumn('Low', F.col('Low').cast(DoubleType())) \
    .withColumn('Close', F.col('Close').cast(DoubleType())) \
    .withColumn('Adj Close', F.col('Adj Close').cast(DoubleType())) \
    .withColumn('Volume', F.col('Volume').cast(DoubleType())) \
    .withColumn('Average', F.col('Average').cast(DoubleType())) \
    .withColumn('VolumeDivClose', F.col('VolumeDivClose').cast(DoubleType())) \

# Add column Max which find max value in each row
df=df.withColumn('Max', F.greatest(*[F.col(cl) for cl in df.columns[2:]]))

# Add column Min which find min value in each row
df=df.withColumn('Min', F.least(*[F.col(cl) for cl in df.columns[2:]]))

# Rename Adj Close to AdjClose
df=df.withColumnRenamed('Adj Close', 'AdjClose')

# Define table name
table = f'{DATASET_NAME}.{CRYPTOCURRENCY}' 

# Upload dataframe to GBQ dataset
df.write.format('bigquery') \
    .option('table', table).option("temporaryGcsBucket",BUCKET_NAME) \
    .save(table)
