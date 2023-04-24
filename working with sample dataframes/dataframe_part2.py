# Databricks notebook source
type(spark)

# COMMAND ----------

help(spark.createDataFrame)

# COMMAND ----------

data=[{'id':1,'name':'maheer'},{'id':2,'name':'wafa'}]
df=spark.createDataFrame(data=data)
df.show()
#data=[{'id':1,'name':'sree'},{'id':2,'name':'re'}]
#df.spark.createDataFrame(data)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.types import *
data=[{'id':1,'name':'maheer'},{'id':2,'name':'wafa'}]
schema=StructType([StructField(name='id',dataType=IntegerType()),StructField(name='name',dataType=StringType())])
df1=spark.createDataFrame(data,schema)
df1.show()

# COMMAND ----------

df1.printSchema()

# COMMAND ----------

data1=[(1,'maheer'),(2,'wafa')]
#schema=['id','name']
schema=StructType([StructField(name='id',dataType=IntegerType()),StructField(name='name',dataType=StringType())])

df2=spark.createDataFrame(data1,schema)
df2.show()

# COMMAND ----------

df2.printSchema()

# COMMAND ----------

#using with column function
from pyspark.sql.types import *
from pyspark.sql.functions import col
data=[(1,'maheer','30000'),(2,'wafa','40000')]
columns=['id','name','salary']
#schema=StructType([StructField(name='id',dataType=IntegerType()),StructField(name='name',dataType=StringType()),StructField(name='salary',dataType=IntegerType())])

df=spark.createDataFrame(data=data,schema=columns)
df_new=df.withColumn(colName='salary',col=col('salary').cast('Integer')).\
withColumn("id",df.id.cast('Integer'))
df_new.show()

df_new.printSchema()


# COMMAND ----------

df_new.show()

# COMMAND ----------

df_new2=df_new.withColumn('salary',df_new.salary*2)
#df2=df1.withColumn('salary'.df.salary*2)

# COMMAND ----------

df_new2.show()

# COMMAND ----------

df3=df_new2.withColumn('country',lit('india'))
#df3=df_new.withColumn('country',lit('india'))

# COMMAND ----------

df3.show()

# COMMAND ----------

df4=df3.withColumn('copied_salary',df3.salary)
#df4=df3.withColumn('coped_salary'.df3.salary)

# COMMAND ----------

df4.show()

# COMMAND ----------

#dataframe with column renammed()
df5=df4.withColumnRenamed('salary','salary_amount')
#df5=df4.withColumnRenamed('salary','salary_amount')

# COMMAND ----------

df5.show()