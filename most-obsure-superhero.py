from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

#Creating names dataframe from Marvel-names.txt file
names = spark.read.schema(schema).option("sep", " ").csv("Marvel+Names")

lines = spark.read.text("Marvel+Graph")


connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))

#Here we are aggregating to find the minimum number of connections in connections dataframe
#And then selecting the first minimum one and taking out index 0 one
minConnectionCount=connections.agg(func.min('connections')).first()[0]

#Filtering connections dataframe having connections as minimum connection
minConnections=connections.filter(func.col('connections')==minConnectionCount)

#Joining the dataframes
minConnectionsWithNames=minConnections.join(names,'id')

print("The following characters have only "+ str(minConnectionCount) + 'connections')

minConnectionsWithNames.select('name').show()