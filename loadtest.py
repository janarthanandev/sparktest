# from pyspark.sql import SparkSession
# import sys


# session = boto3.session.Session(profile_name="Sparks3")
# sts_connection = session.client(‘sts’)
# response = sts_connection.assume_role(RoleArn="arn:aws:iam::043825451626:role/ROLE-PHC-USW2-DATACURATION-SB-POD", RoleSessionName=’sparks3_session’,DurationSeconds=3600)
# credentials = response[‘Credentials’]


# # url = str(‘s3a://bucket/key/data.csv’)
# url = str('s3a://s3-phc-poc-02-sample-etl/data/height_weight.csv')


# spark.sparkContext._jsc.hadoopConfiguration().set(‘fs.s3a.aws.credentials.provider’, ‘org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider’)
# spark.sparkContext._jsc.hadoopConfiguration().set(‘fs.s3a.access.key’, credentials[‘AccessKeyId’])
# spark.sparkContext._jsc.hadoopConfiguration().set(‘fs.s3a.secret.key’, credentials[‘SecretAccessKey’])
# spark.sparkContext._jsc.hadoopConfiguration().set(‘fs.s3a.session.token’, credentials[‘SessionToken’])
# spark.read.csv(url).show(1)

# warehouseLocation = 's3a://s3-phc-poc-02-sample-etl/'
# spark = SparkSession.builder.appName("Test").config("spark.sql.warehouse.dir", warehouseLocation).getOrCreate()
# sc = spark.sparkContext
# sc.setLogLevel("INFO")
# sc._jsc.hadoopConfiguration().set('fs.s3a.endpoint', 's3.amazonaws.com')
# sc._jsc.hadoopConfiguration().set('fs.s3a.aws.credentials.provider',
#                                   'com.amazonaws.auth.DefaultAWSCredentialsProviderChain')
# #data_df = spark.read.csv("s3a://s3-phc-poc-02-sample-etl/2020/08/*.gz")
# data_df = spark.read.csv(sys.argv[1])
# print('10 rows to display:')
# data_df.show(10)


# data_df.rdd.getNumPartitions()
# data_df = data_df.repartition(200)
# count=data_df.count()
# print('Number of rows: ',count)



from pyspark.sql.context import SQLContext
from pyspark.sql import SparkSession
import boto3
client = boto3.client('sts')
response = client.assume_role(RoleArn="arn:aws:iam::043825451626:role/ROLE-PHC-USW2-DATACURATION-SB-POD",
RoleSessionName="MySession")
credentials = response['Credentials']
# print(credentials['AccessKeyId'])
# print(credentials['SecretAccessKey'])
# print(credentials['SessionToken'])
spark = SparkSession.builder.appName("Test").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
sc._jsc.hadoopConfiguration().set('fs.s3a.endpoint', 's3.amazonaws.com')
sc._jsc.hadoopConfiguration().set('fs.s3a.aws.credentials.provider',
'org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider')
sc._jsc.hadoopConfiguration().set('fs.s3a.access.key', credentials['AccessKeyId'])
sc._jsc.hadoopConfiguration().set('fs.s3a.secret.key', credentials['SecretAccessKey'])
sc._jsc.hadoopConfiguration().set('fs.s3a.session.token', credentials['SessionToken'])
file = str('s3a://s3-phc-poc-02-sample-etl/data/height_weight.csv')
data_df = spark.read.options(header='True',inferSchema='True').csv(file)
print('Input data:')
data_df.show(10)
data_df.createTempView("data")
file = str('s3a://s3-phc-poc-02-sample-etl/data/bmi.json')
bmi_df = spark.read.json(file).cache()
exp = bmi_df.select('*').collect()[0]
print('Metadata rule:')
bmi_df.show(10)
col = exp.__fields__
result = spark.sql("select *,case when {0}{1} then '{5}' \
                          when {0}{2} then '{6}' \
                          when {0}{3} then '{7}' \
                          when {0}{4} then '{8}' \
                          end as result from data".
          format(exp[0],exp[1],exp[2],exp[3],exp[4],col[1],col[2],col[3],col[4]))
#result.show(20)
print('Saving results as s3-phc-poc-02-sample-etl/data/result.parquet')
result.coalesce(1).write.parquet("s3a://s3-phc-poc-02-sample-etl/data/result.parquet",mode="overwrite")
print('File saved')
output= str("s3a://s3-phc-poc-02-sample-etl/data/result.parquet")
print('Verifying the output')
spark.read.parquet(output).show(20)



