from pyspark.sql.types import *

data = [('abc', [1, 2]), ('mno', [4, 5]), ('xyz', [7, 8])]
schema = ['id', 'numbers']
schema = StructType([ \
    StructField('id', dataType=StringType()), \
    StructField('numbers', dataType=ArrayType(IntegerType()))])

df = spark.createDataFrame(data, schema)
df.show()
df.printSchema()
display(df)

# COMMAND ----------

# fetch value from array as new column
from pyspark.sql.types import *
from pyspark.sql.functions import col
from pyspark.sql.functions import *

df.withColumn('first_number', col('numbers')[0]).show()

# COMMAND ----------

from pyspark.sql.functions import *

data = [(1, 2), (3, 4)]
schema = ['num1', 'num2']
df = spark.createDataFrame(data, schema)
df.withColumn('numbers', array(col('num1'), col('num2'))).show()

# COMMAND ----------

data = [(1, 'maheer', ['dotnet', 'azure']), (2, 'wafa', ['jawa', 'aws'])]

schema = ('id', 'name', 'skills')

df = spark.createDataFrame(data, schema)

display(df)

# COMMAND ----------

df.show()
df1 = df.withColumn('skill', explode(col('skills')))
df1.show()

# COMMAND ----------

data = [(1, 'maheer', 'dotnet,azure'), (2, 'wafa', 'jawa,aws')]

schema = ('id', 'name', 'skills')

df = spark.createDataFrame(data, schema)

display(df)

# COMMAND ----------

df.show()
df1 = df.withColumn('skillsarray', split(col('skills'), ','))
df1.show()

# COMMAND ----------

# functions on columns of dataframe
data = [(1, 'maheer', 'M', 2000), (2, 'wafa', 'M', 4000), (3, 'asi', 'f', 3000)]
schema = ['id', 'name', 'gender', 'salary']
df = spark.createDataFrame(data, schema)

# COMMAND ----------

df.show()

# COMMAND ----------

# alias function
from pyspark.sql.types import *
from pyspark.sql.functions import *

df1 = df.select(df.id.alias('emp_id'), df.name.alias('emp_name'))

# COMMAND ----------

df1.show()

# COMMAND ----------

# asc & desc
data = [(1, 'maheer', 'M', 2000), (2, 'wafa', 'M', 4000), (3, 'asi', 'f', 3000)]
schema = ['id', 'name', 'gender', 'salary']
df = spark.createDataFrame(data, schema)
df.sort(df.name.desc()).show()

# COMMAND ----------

# asc & desc
data = [(1, 'maheer', 'M', 2000), (2, 'wafa', 'M', 4000), (3, 'asi', 'f', 3000)]
schema = ['id', 'name', 'gender', 'salary']
df = spark.createDataFrame(data, schema)
df.sort(df.name.asc()).show()

# COMMAND ----------

# cast function to change the datatype
data = [(1, 'maheer', 'M', 2000), (2, 'wafa', 'M', 4000), (3, 'asi', 'f', 3000)]
schema = ['id', 'name', 'gemder', 'salary']
df = spark.createDataFrame(data, schema)

# COMMAND ----------

df1 = df.withColumn('id', col('id').cast('int'))

# COMMAND ----------

df1.printSchema()

# COMMAND ----------

data = [(1, 'maheer', 'M', 2000), (2, 'wafa', 'M', 4000), (3, 'asi', 'f', 3000)]
schema = ['id', 'name', 'gender', 'salary']
df = spark.createDataFrame(data, schema)
df2 = df.where(df.name.like('m%'))

# COMMAND ----------

df2.show()

# COMMAND ----------

data = [(1, 'maheer', 'M', 2000), (2, 'wafa', 'M', 4000), (3, 'asi', 'f', 3000)]
schema = ['id', 'name', 'gender', 'salary']
df = spark.createDataFrame(data, schema)

# COMMAND ----------

df1 = df.filter(df.gender == 'M')

# COMMAND ----------

df1.show()

# COMMAND ----------

df2 = df.where(df.gender == 'M')

# COMMAND ----------

df2.show()

# COMMAND ----------

data = [(1, 'maheer', 'M', 2000), (2, 'wafa', 'M', 4000), (2, 'wafa', 'M', 4000), (3, 'asi', 'f', 4000)]
schema = ['id', 'name', 'gender', 'salary']
df = spark.createDataFrame(data, schema)

# COMMAND ----------

df.show()

# COMMAND ----------

df.distinct().show()

# COMMAND ----------

df.dropDuplicates().show()

# COMMAND ----------

df.dropDuplicates(['salary']).show()

# COMMAND ----------

# orderby & sort
data = [(1, 'maheer', 'M', 2000, 'IT'), (2, 'wafa', 'M', 4000, 'HR'), (2, 'wafa', 'M', 4000, 'payroll'),
        (3, 'asi', 'f', 4000, 'HR')]
schema = ['id', 'name', 'gender', 'salary', 'dep']
df = spark.createDataFrame(data, schema)
df.sort(df.dep, df.id).show()

# COMMAND ----------

##orderby & sort
data = [(1, 'maheer', 'M', 2000, 'IT'), (2, 'wafa', 'M', 4000, 'HR'), (2, 'wafa', 'M', 4000, 'payroll'),
        (3, 'asi', 'f', 4000, 'HR')]
schema = ['id', 'name', 'gender', 'salary', 'dep']
df = spark.createDataFrame(data, schema)
df.sort(df.dep.desc(), df.id.desc()).show()

# COMMAND ----------

# data=[(1,'maheer','M',2000,'IT'),(2,'wafa','M',4000,'HR'),(2,'wafa','M',4000,'payroll'),(3,'asi','f',4000,'HR')]
schema = ['id', 'name', 'gender', 'salary', 'dep']
df = spark.createDataFrame(data, schema)
df.orderBy(df.dep.desc(), df.id.desc()).show()

# COMMAND ----------

data1 = [(1, 'maheer', 'M'), (2, 'wafa', 'M')]
schema1 = ['id', 'name', 'gender']
data2 = [(3, 'asin', 'M'), (4, 'sar', 'f')]
schema2 = ['id', 'name', 'gender']
df1 = spark.createDataFrame(data1, schema1)
df1.show()
df2 = spark.createDataFrame(data2, schema2)
df2.show()

# COMMAND ----------

new_df = df1.union(df2)

# COMMAND ----------

new_df.show()

# COMMAND ----------

data1 = [(1, 'maheer', 'M'), (2, 'wafa', 'M')]
schema1 = ['id', 'name', 'gender']
data2 = [(1, 'maheer', 'M'), (3, 'asin', 'M'), (4, 'sar', 'f')]
schema2 = ['id', 'name', 'gender']
df1 = spark.createDataFrame(data1, schema1)
df1.show()
df2 = spark.createDataFrame(data2, schema2)
df2.show()

# COMMAND ----------

new__df = df1.unionAll(df2)

# COMMAND ----------

new__df.show()

# COMMAND ----------

data = [(1, 'maheer', 'M', 2000, 'IT'), (2, 'wafa', 'M', 4000, 'HR'), (2, 'wafa', 'M', 4000, 'payroll'), \
        (3, 'asi', 'f', 4000, 'HR'), \
        (4, 'ass', 'f', 5000, 'iot')]
schema = ['id', 'name', 'gender', 'salary', 'dep']
df = spark.createDataFrame(data, schema)

# COMMAND ----------

df.groupBy('dep').count().show()

# COMMAND ----------

df.groupBy('dep', 'gender').min('salary').show()

# COMMAND ----------

df.groupBy('dep').max('salary').show()

# COMMAND ----------

data = [(1, 'maheer', 'M', 2000, 'IT'), (2, 'wafa', 'M', 4000, 'HR'), (2, 'wafa', 'M', 4000, 'payroll'), \
        (3, 'asi', 'f', 4000, 'HR'), \
        (4, 'ass', 'f', 5000, 'iot')]
schema = ['id', 'name', 'gender', 'salary', 'dep']
df = spark.createDataFrame(data, schema)
df.show()
df.groupBy('dep').count().show()

# COMMAND ----------

from pyspark.sql.functions import *

df.groupBy('dep').agg(count("*").alias('countofemp'), \
 \
                      min('salary').alias('min_salary')).show()

# COMMAND ----------

df1 = df.select('id', 'name')

# COMMAND ----------

df1.show()

# COMMAND ----------

df3 = df.select('*')

# COMMAND ----------

df3.show()

# COMMAND ----------

data1 = [(1, 'maheer', 2000, 2), (2, 'wafa', 3000, 1), (3, 'abcd', 1000, 4)]
schema1 = ['id', 'name', 'salary', 'dept']

data2 = [(1, 'IT'), (2, 'HR'), (3, 'payroll')]
schema2 = ['id', 'name']

empdf = spark.createDataFrame(data1, schema1)
depdf = spark.createDataFrame(data2, schema2)
empdf.show()
depdf.show()

# COMMAND ----------

df4 = empdf.join(depdf, empdf.dept == depdf.id, 'inner')

# COMMAND ----------

df4.show()

# COMMAND ----------

df5 = empdf.join(depdf, empdf.dept == depdf.id, 'left')

# COMMAND ----------

df5.show()

# COMMAND ----------

df6 = empdf.join(depdf, empdf.dept == depdf.id, 'right')

# COMMAND ----------

df6.show()

# COMMAND ----------

df7 = empdf.join(depdf, empdf.dept == depdf.id, 'right')

# COMMAND ----------

df7.show()