df2_pyspark=spark.read.option('header','true').csv('dbfs:/FileStore/Book3_1.csv',inferSchema=True)

# COMMAND ----------

df2_pyspark.show()

# COMMAND ----------

df2_pyspark.printSchema()

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
#df = df2_pyspark.select(trim(df[" Salary "]))
#df.withColumn("Salary",trim(" Salary ")).show()
#withColumn(" Salary ",col(" Salary ").cast("Integer"))
df_sel = df2_pyspark.select("Name","Age","Experience",trim(" Salary ").alias("Salary"))

# COMMAND ----------

df_sel.show()

# COMMAND ----------

df_sel.printSchema()

# COMMAND ----------

#df1=df_sel.withColumn("Salary",col("Salary").cast("Integer"))
#df1.show()

# COMMAND ----------

df_sel.printSchema()


# COMMAND ----------

df2= df_sel.where(df_sel.Salary < '50000')
df2.show()

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
#df=spark.read.option('header','true').csv('dbfs:/FileStore/Book3_1.csv',inferSchema=True)
#df_sel.select(regexp_replace(col("Salary"),",",""))

df3=df2.agg(max(df2.Salary),min(df2.Salary),avg(df2.Salary),count(df2.Salary))
# display(df3)

# COMMAND ----------

df3.show()

# COMMAND ----------

df4 = df2.withColumn("Salary", translate("Salary", ",", ""))

# COMMAND ----------

df4.show()

# COMMAND ----------

df4.printSchema()

# COMMAND ----------

df6=df4.agg(max(df4.Salary),min(df4.Salary),avg(df4.Salary),count(df4.Salary))

# COMMAND ----------

df6.show()