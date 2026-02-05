# Databricks notebook source
emps_df=spark.read.json("/mnt/files/jsondata/emps.json")
emps_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Creating delta tables: 2ways
# MAGIC - 1)using sql approach
# MAGIC - 2)using pyspark approach

# COMMAND ----------

# MAGIC %md
# MAGIC 1)using create stmt--->creating table in dev catalog-->demodb database
# MAGIC                                            i.e dev.demodb

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists dev.demo_db.emp1(
# MAGIC   eid int,
# MAGIC   ename string,
# MAGIC   sal int,
# MAGIC   sex string,
# MAGIC   dno int
# MAGIC ) using delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into dev.demo_db.emp1 values (101,'John',50000,'M',11),
# MAGIC (102,'Jane',60000,'F',12),(103,'Mike',70000,'M',13),(104,'Sara',80000,'F',14);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev.demo_db.emp1;

# COMMAND ----------

# MAGIC %md
# MAGIC Loading data into delta table (using DataFrame API-) -Another way of creating a Delta table

# COMMAND ----------

emps_df.write.format("delta").mode("append").saveAsTable("dev.demo_db.emp")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev.demo_db.emp;

# COMMAND ----------

# MAGIC %md
# MAGIC * II-method of creating delta table --->pyspark approach

# COMMAND ----------

# MAGIC %md
# MAGIC creating catalog ,database and table and loading data into it

# COMMAND ----------

spark.sql("create catalog if not exists dev")
spark.sql("create database if not exists dev.demo_db")

# COMMAND ----------

schema1="""eid int,
           ename string,
           sal int,
           sex string,
           dno int"""


# COMMAND ----------

empdf=spark.read.format("json").schema(schema1).load("/mnt/files/jsondata/emps.json")

# COMMAND ----------

empdf.write.format("delta").mode("append").saveAsTable("dev.demo_db.emp2")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev.demo_db.emp2;

# COMMAND ----------

# MAGIC %md
# MAGIC another approach of creating delta table
# MAGIC
# MAGIC i.e using deltalakeAPI or deltatableAPI
# MAGIC
# MAGIC using DeltaTable Builder API
# MAGIC
# MAGIC first import DeltaTable object
# MAGIC
# MAGIC from delta import DeltaTable
# MAGIC
# MAGIC here DeltaTable is the object which is used to create DeltaTable
# MAGIC using DeltatableBuilder API

# COMMAND ----------

from delta import DeltaTable  #or from delta.tables import *
# (DeltaTable.create(spark) 
#(or)DeltaTable.createOrReplace(spark) #if already exists replace and create
#(or)DeltaTable.createIfNotExists(spark) #if not exists only-->create
                                         #here we won't see spaces unlike sql
DeltaTable.createOrReplace(spark).tableName("dev.demo_db.emp3") \
           .addColumn("eid", "int") \
           .addColumn("ename", "string") \
           .addColumn("sal","int") \
           .addColumn("sex","string") \
           .addColumn("dno","int") \
           .execute()

# COMMAND ----------

spark.sql("DESCRIBE dev.demo_db.emp3").show()

# COMMAND ----------

# MAGIC %sql describe table dev.demo_db.emp3;
