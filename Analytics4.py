# Databricks notebook source
from pyspark.sql.functions import col,lower,trim,lit,count,rank,dense_rank,substring
from pyspark.sql.window import Window

# COMMAND ----------

Path = sys.argv[1]

# COMMAND ----------

df_Charges = spark.read.format('csv')\
.option("inferSchema",True)\
.option("header",True)\
.load(Path+"Charges_use.csv")

df_Damages = spark.read.format('csv')\
.option("inferSchema",True)\
.option("header",True)\
.load(Path+"Damages_use.csv") 

df_Endorse = spark.read.format('csv')\
.option("inferSchema",True)\
.option("header",True)\
.load(Path+"Endorse_use.csv")

df_Primary_Person = spark.read.format('csv')\
.option("inferSchema",True)\
.option("header",True)\
.load(Path+"Primary_Person_use.csv")

df_Restrict = spark.read.format('csv')\
.option("inferSchema",True)\
.option("header",True)\
.load(Path+"Restrict_use.csv")

df_Units = spark.read.format('csv')\
.option("inferSchema",True)\
.option("header",True)\
.load(Path+"Units_use.csv")

df_units_deDup = df_Units.dropDuplicates()

# COMMAND ----------

df_units_deDup.filter( (col("UNKN_INJRY_CNT")!= 0) | (col("TOT_INJRY_CNT")!= 0) | (col("DEATH_CNT")!= 0) )\
.withColumn("Sum_OfInjuryAndDeath", (col("UNKN_INJRY_CNT") + col("TOT_INJRY_CNT") + col("DEATH_CNT")) )\
.groupBy(col("VEH_MAKE_ID")).agg(count('*').alias("Count"))\
.withColumn("Rank", dense_rank().over(Window.orderBy(col("Count").desc())))\
.filter(col("Rank").isin([i for i in range(5,16)]))\
.select(["VEH_MAKE_ID","Rank"]).display()

# COMMAND ----------


