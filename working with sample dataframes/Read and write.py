df = spark.read.format("csv").option('header', True).load("dbfs:/FileStore/Book4_1.csv")

# COMMAND ----------

df = spark.read.format("csv").option('header', True).load("dbfs:/FileStore/Book4_1.csv")

# COMMAND ----------

df = spark.read.format("csv").options(header=True, inferSchema='True', delimiter=',').load(
    "dbfs:/FileStore/Book4_1.csv")

# COMMAND ----------

df.display()

# COMMAND ----------

# df.write.format("delta").saveAsTable("AbhishekEmp")

# COMMAND ----------

df.write.csv("dbfs:/FileStore/tables_Output3")

# COMMAND ----------

df1 = spark.read.format("csv").option('header', 'true').load(
    "dbfs:/FileStore/tables_Output2/part-00000-tid-4641572560197478910-fa1e4359-4e64-4962-b73c-1fe095a738b6-39-1-c000.csv")

# COMMAND ----------

df1 = spark.read.format("csv").options(header=True, inferSchema='True', delimiter=',').load(
    "dbfs:/FileStore/tables_Output2/part-00000-tid-4641572560197478910-fa1e4359-4e64-4962-b73c-1fe095a738b6-39-1-c000.csv")

# COMMAND ----------

df1.display()

# COMMAND ----------

# how to read json file into dataframe(single and multiple files)
# how to read single line and multiline json files

# COMMAND ----------

# write dataframe into json

data = [(1, 'maheer'), (2, 'wafa')]
schema = ['id', 'name']
df = spark.createDataFrame(data, schema)

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.json('dbfs:/FileStore/tables_Output4')

# COMMAND ----------

display(spark.read.json(path='dbfs:/FileStore/tables_Output4'))

# COMMAND ----------

df.write.json('dbfs:/FileStore/tables_Output4', mode='ignore')

# COMMAND ----------

display(spark.read.json(path='dbfs:/FileStore/tables_Output4'))

# COMMAND ----------

df.write.json('dbfs:/FileStore/tables_Output4', mode='append')

# COMMAND ----------

display(spark.read.json(path='dbfs:/FileStore/tables_Output4'))

# COMMAND ----------

df.write.json('dbfs:/FileStore/tables_Output4', mode='overwrite')

# COMMAND ----------

display(spark.read.json(path='dbfs:/FileStore/tables_Output4'))

# COMMAND ----------

df.write.mode('overwrite').json('dbfs:/FileStore/tables_Output4')

# COMMAND ----------


display(spark.read.json(path='dbfs:/FileStore/tables_Output4'))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.types import *

schema = StructType().add(field='id', data_type=IntegerType()) \
    .add(field='name', data_type=StringType())

df1 = spark.read.json(path='dbfs:/FileStore/tables_Output4', schema=schema)

# COMMAND ----------

df1.printSchema()

# COMMAND ----------

# Rdd tansformations onKey value pair

# data=spark.sparkContext().parallelize([(1,2),(3,4),(3,6),(3,4)])
data = spark.sparkContext.parallelize([(1, 2), (3, 4), (3, 6), (3, 4)])

# COMMAND ----------

data.collect()

# COMMAND ----------

type(data)

# COMMAND ----------

data.countByValue()

# COMMAND ----------

data.top(2)

# COMMAND ----------

data.sortByKey().collect()

# COMMAND ----------

# lookup returns all values associates with the given key
data.lookup(3)

# COMMAND ----------

data.keys().collect()

# COMMAND ----------

data.values().collect()

# COMMAND ----------

data.mapValues(lambda a: a * a).collect()

# COMMAND ----------

# we cannot use map,reduce in key value pair(mapvalues,reduceBykey)

# COMMAND ----------

# reduceBykey()
# groupBykey()
data.reduceByKey(lambda x, y: x + y).collect()

# COMMAND ----------

data.reduceByKey(max).collect()

# COMMAND ----------

data.flatMapValues(lambda x: range(1, x)).collect()

# COMMAND ----------

data2 = spark.sparkContext.parallelize([(3, 9), (4, 15)])
data2.collect()

# COMMAND ----------

data2.join(data).collect()


def read_csv(path):
    df = spark.read.option('header', True).option('delimiter', ',').csv(path).show()


read_csv('dbfs:/FileStore/shared_uploads/sreekanthreddysri2017@gmail.com/Book3_1.csv')

# COMMAND ----------


# COMMAND ----------

#

# COMMAND ----------

#

# COMMAND ----------

df = spark.read.options(header=True, inferSchema='True', delimiter=',').csv("dbfs:/FileStore/Book3_1.csv")

# COMMAND ----------

df = spark.read.format('csv').load('dbfs:/FileStore/Book3_1.csv', header=True, inferschema='True', sep=',')

# COMMAND ----------

df.show()

# COMMAND ----------

df = spark.read.format("csv").option('header', True).option('inferschema', True).load('dbfs:/FileStore/Book3_1.csv')

# COMMAND ----------

display(df)

# COMMAND ----------

df1 = df.select("*")

# COMMAND ----------

display(df1)

# COMMAND ----------

#

# COMMAND ----------


# COMMAND ----------

#

# COMMAND ----------

df = spark.read.format("csv").option('header', True).load("dbfs:/FileStore/Book3_1.csv")

# COMMAND ----------

df.display()

# COMMAND ----------

#

# COMMAND ----------

# Create RDD from parallelize
val_dataSeq = [("Java", 20000), ("Python", 100000), ("Scala", 3000)]
val_rdd = spark.sparkContext.parallelize(val_dataSeq)

# COMMAND ----------

display(val_rdd)

# COMMAND ----------

val_rdd.collect()

# COMMAND ----------

val_rdd.count()

# COMMAND ----------

df = spark.createDataFrame(val_rdd, schema=['course', 'amount'])

# COMMAND ----------

df.show()