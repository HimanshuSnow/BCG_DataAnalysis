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

 df_Primary_Person.filter(lower(trim(col("PRSN_GNDR_ID"))) == lower(lit('MALE'))).select(["CRASH_ID"]).distinct().count()
