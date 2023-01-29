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

# DBTITLE 1,Assumptions
#Taking drivers license state as state where accident took place.

# COMMAND ----------

df_Accident_Female = df_Primary_Person.filter( (lower(trim(col("PRSN_GNDR_ID"))) == lower(lit('FEMALE'))) & (col("DRVR_LIC_CLS_ID").isin('UNLICENSED','UNKNOWN','OTHER/OUT OF STATE','NA') == False)).select(["CRASH_ID","DRVR_LIC_STATE_ID"]).distinct()

# COMMAND ----------

df_Accident_Female.groupBy(["DRVR_LIC_STATE_ID"]).agg(count('*').alias("Count")).withColumn("Rank", rank().over(Window.orderBy(col("Count").desc()))).filter(col("Rank") == 1).select(col("DRVR_LIC_STATE_ID").alias("StateWithMaximumAccidentInvolvingFemale")).display()
