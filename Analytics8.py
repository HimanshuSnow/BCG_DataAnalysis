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

# DBTITLE 1,Speeding related offences
df_Speed_Offences = df_Charges.filter(lower(col("CHARGE")).like('%speed%')).select(["CRASH_ID","UNIT_NBR"]).distinct()

# COMMAND ----------

df_Crash_WithOffence = df_Charges.filter(col("CHARGE").isNotNull())

# COMMAND ----------

# DBTITLE 1,Top25State_WithOffence
df_Top25State_WithOffence_Licensed = df_Primary_Person.alias("Person").join(df_Crash_WithOffence.alias("Off"),  ( (col("Person.CRASH_ID") == col("Off.CRASH_ID")) & (col("Person.UNIT_NBR") == col("Off.UNIT_NBR")) ), "inner")\
.select(["Person.CRASH_ID","Person.DRVR_LIC_STATE_ID","Person.DRVR_LIC_TYPE_ID"])\
.filter(col("Person.DRVR_LIC_STATE_ID").isin(['NA','Unknown']) == False)\
.groupBy(col("Person.DRVR_LIC_STATE_ID")).agg(count('*').alias("Count"))\
.withColumn("Rank", dense_rank().over(Window.orderBy(col("Count").desc())))\
.filter((col("Rank").isin([i for i in range(1,26)])))

# COMMAND ----------

List_Top25State = df_Top25State_WithOffence_Licensed.rdd.map(lambda x:x[0]).collect()

# COMMAND ----------

# DBTITLE 1,Top10_UsedColor
df_Top10_UsedColor = df_units_deDup.groupBy(col("VEH_COLOR_ID")).agg(count('*').alias("Count"))\
.withColumn("Rank", dense_rank().over(Window.orderBy(col("Count").desc())))\
.filter(col("Rank").isin([i for i in range(1,11)]))

# COMMAND ----------

ListOfTop10Color = df_Top10_UsedColor.rdd.map(lambda x:x[0]).collect()

# COMMAND ----------

df_PreFinal = df_Primary_Person.alias("Person").join(df_units_deDup.alias("Unit"), ( (col("Person.CRASH_ID") == col("Unit.CRASH_ID")) & (col("Person.UNIT_NBR") == col("Unit.UNIT_NBR")) ), "inner")\
.filter( (col("Person.DRVR_LIC_TYPE_ID").isin(['NA','OTHER','UNKNOWN','UNLICENSED'])== False) & (lower(col("Unit.VEH_BODY_STYL_ID")).like('%car%')) & (col("Unit.VEH_COLOR_ID").isin(ListOfTop10Color)) & (col("Person.DRVR_LIC_STATE_ID").isin(List_Top25State)))\
.select(["Person.CRASH_ID","Person.UNIT_NBR","Unit.VEH_MAKE_ID"])

# COMMAND ----------

# DBTITLE 1,Final Result
df_PreFinal.alias("F").join(df_Speed_Offences.alias("Off"), ( (col("F.CRASH_ID") == col("Off.CRASH_ID")) & (col("F.UNIT_NBR") == col("Off.UNIT_NBR")) ), "inner").groupBy(col("F.VEH_MAKE_ID")).agg(count('*').alias("Count"))\
.withColumn("Rank", dense_rank().over(Window.orderBy(col("Count").desc())))\
.filter(col("Rank").isin([i for i in range(1,6)]))\
.select(["VEH_MAKE_ID","Rank"])\
.display()
