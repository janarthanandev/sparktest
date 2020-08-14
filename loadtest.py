from pyspark.sql import SparkSession
import sys

warehouseLocation = 's3a://s3-phc-poc-02-sample-etl/'
spark = SparkSession.builder.appName("Test").config("spark.sql.warehouse.dir", warehouseLocation).getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("INFO")
#sc._jsc.hadoopConfiguration().set('fs.s3a.endpoint', 's3.amazonaws.com')
#data_df = spark.read.csv("s3a://s3-phc-poc-02-sample-etl/2020/08/*.gz")
data_df = spark.read.csv(sys.argv[1])
print('10 rows to display:')
data_df.show(10)


data_df.rdd.getNumPartitions()
data_df = data_df.repartition(200)
count=data_df.count()
print('Number of rows: ',count)