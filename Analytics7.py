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

df_Crash_WithDamage = df_Damages.filter( ((lower(col("DAMAGED_PROPERTY")).like('%no damage%')) | (lower(col("DAMAGED_PROPERTY")).like('none%'))) == False ).select(["CRASH_ID"]).distinct()
df_WithoutDam = df_Damages.filter( (lower(col("DAMAGED_PROPERTY")).like('%no damage%')) | (lower(col("DAMAGED_PROPERTY")).like('none%')) ).select(["CRASH_ID"]).distinct()

# COMMAND ----------

df_Crash_WithoutDamage = df_WithoutDam.alias("WD").join(df_Crash_WithDamage.alias("D"), col("WD.CRASh_ID") == col("D.CRASH_ID"), "left_anti").select(["WD.CRASH_ID"]).distinct()

# COMMAND ----------

df_Unit_WithRating = df_units_deDup.withColumn("rating1", substring("VEH_DMAG_SCL_1_ID",9,1))\
.withColumn("rating2", substring("VEH_DMAG_SCL_2_ID",9,1))

# COMMAND ----------

df_Crash_WithoutDamage.alias("WD").join(df_Unit_WithRating.alias("unit"), col("WD.CRASh_ID") == col("unit.CRASH_ID"), "inner")\
.filter( ( (col("unit.rating1")>4) | (col("unit.rating2")>4) ) & (trim(lower(col("FIN_RESP_TYPE_ID")))!= 'na') )\
.select(["unit.CRASH_ID"]).distinct().count()
