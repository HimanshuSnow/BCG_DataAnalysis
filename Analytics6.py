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

df_Primary_Person.alias("person").join(df_units_deDup.alias("unit"), ( (col("person.CRASH_ID") == col("unit.CRASH_ID")) & (col("person.UNIT_NBR") == col("unit.UNIT_NBR")) ), "inner")\
.select(["person.CRASH_ID","person.UNIT_NBR","person.DRVR_ZIP","unit.CONTRIB_FACTR_P1_ID","unit.CONTRIB_FACTR_2_ID","unit.CONTRIB_FACTR_1_ID"])\
.filter( ((lower(col("unit.CONTRIB_FACTR_P1_ID")).like('%alcohol%')) | (lower(col("unit.CONTRIB_FACTR_2_ID")).like('%alcohol%')) | (lower(col("unit.CONTRIB_FACTR_1_ID")).like('%alcohol%'))) & (col("person.DRVR_ZIP").isNotNull()) )\
.select(["person.CRASH_ID","person.UNIT_NBR","person.DRVR_ZIP"]).distinct()\
.groupBy(["person.DRVR_ZIP"]).agg(count('*').alias("Count"))\
.withColumn("Rank", dense_rank().over(Window.orderBy(col("Count").desc())))\
.filter(col("Rank").isin([i for i in range(1,6)]))\
.select(col("person.DRVR_ZIP").alias("Top_5_Zip_With_Most_Crashes_Due_To_Alcohol"), col("Rank"))\
.display()
