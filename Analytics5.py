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

df_Primary_Person.alias("person").join(df_units_deDup.alias("Unit"),  ( (col("person.CRASH_ID") == col("Unit.CRASH_ID")) & (col("person.UNIT_NBR") == col("Unit.UNIT_NBR")) ), "inner")\
.select(["person.CRASH_ID","person.UNIT_NBR","person.PRSN_ETHNICITY_ID","Unit.VEH_BODY_STYL_ID"])\
.groupBy(["person.PRSN_ETHNICITY_ID","Unit.VEH_BODY_STYL_ID"]).agg(count('*').alias("Count"))\
.withColumn("Rank", dense_rank().over(Window.partitionBy(col("VEH_BODY_STYL_ID")).orderBy(col("Count").desc())))\
.filter(col("Rank") == 1)\
.select(["VEH_BODY_STYL_ID","PRSN_ETHNICITY_ID","Rank"])\
.display()

# COMMAND ----------


