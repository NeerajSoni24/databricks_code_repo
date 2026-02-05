# Databricks notebook source
# MAGIC %md
# MAGIC 1)withColumn(): we can perform the following
# MAGIC            1)for changing the column datatype
# MAGIC            2)for modifying or updating the column value
# MAGIC            3)Deriving a new column from the existing column
# MAGIC            4)Renaming a column
# MAGIC            5)dropping a dataframe column

# COMMAND ----------

data=[(101,'Miller',50000,'2020-02-21','M','pune'),
      (102,'Blake',60000,'2019-04-15','M','hyd'),
      (103,'Sony',70000,'2021-06-25','F','pune'),
      (104,'alia',80000,'2018-03-17','F','hyd')
      ]
columns=["Empid","Empname","Salary","JoinDate","Sex","City"]
df=spark.createDataFrame(data=data,schema=columns)
df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 1)for changing the column datatype
# MAGIC   for changing th column datatype,we use cast() function along with withColumn()

# COMMAND ----------

df1 = df.withColumn("Empid",df.Empid.cast("Integer"))
df1.printSchema()

# COMMAND ----------

df1 = df.withColumn("Empid",df.Empid.cast("Integer")).withColumn("Salary",df.Salary.cast("Integer"))
df1.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ii)for modifying or updating the column value
# MAGIC     i)updating with a value
# MAGIC    ii)updating based on condition

# COMMAND ----------

# MAGIC %md
# MAGIC Task:incrementing the salaries with a hike of 20%

# COMMAND ----------

from pyspark.sql.functions import col
df1 = df.withColumn("Salary",col("Salary")+col("Salary")*0.20)
df1.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ii)updating based on condition using when()
# MAGIC    -here we use withColumn() along with when()
# MAGIC     modify---->"M"------>"MALE"
# MAGIC                "F"------>"FEMALE"
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import when
df1 = df.withColumn("Sex",when(col("Sex")=="M","Male") \
                    .when(col("Sex")=="F","Female") \
                    .otherwise(col("Sex")))
df1.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 3) Adding new column:
# MAGIC    - Adding a new column with default/constant/None/Null value
# MAGIC    - Adding a new column based on another column
# MAGIC    - Adding a new column based on condition
# MAGIC
# MAGIC i)Adding a new column with constant
# MAGIC   - here we use a function called--->lit() function

# COMMAND ----------

# MAGIC %md
# MAGIC Task: adding a new column (hike_percent)

# COMMAND ----------

from pyspark.sql.functions import lit
df4=df.withColumn("Hike_percent",lit(0.30))
df4.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Note: If you want to add a NULL/None-->then use-->lit(None)

# COMMAND ----------

from pyspark.sql.functions import lit
df4=df.withColumn("Hike_percent",lit(None))
df4.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ii)Adding a new column based on another column
# MAGIC    - Generate 2 new columns--->Tax and netsal based on existing column-->Salary
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import lit
df4=df.withColumn("tax",df.Salary*0.10)
df5 = df4.withColumn("net_sal",df4.Salary-df4.tax)
df5.show()

# COMMAND ----------

# MAGIC %md
# MAGIC iv)Generating a new column based on condition
# MAGIC    - ex:Generate column Grade
# MAGIC                  - if sal>=70000----------------->Grade "A"
# MAGIC                  - if sal>=50000 and sal<70000--->Grade "B"
# MAGIC                  - else                       --->Grade "C"
# MAGIC

# COMMAND ----------

df.withColumn('GRADE',
          when((df.Salary>=70000),lit("A"))
          .when((df.Salary<70000) & (df.Salary>=50000),lit("B"))
          .otherwise(lit("C"))).show()

# COMMAND ----------

# MAGIC %md
# MAGIC lit() :lit() function is used to add a constant value  as a new column
# MAGIC
# MAGIC - i)lit() function with select()
# MAGIC

# COMMAND ----------

df2=df.select(col("Empid"),col("Empname"),col("Salary"),lit("IBM").alias("Company"))
df2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ii)lit() function with withColumn()

# COMMAND ----------

df3=df.withColumn("Loan_status",when(col("Salary")>60000,lit("Eligible")) \
    .otherwise(lit("Not Eligible")))
df3.show()

# COMMAND ----------

# MAGIC %md
# MAGIC withColumnRenamed():
# MAGIC      - using this function,we can perform the following
# MAGIC       - i)to rename a column
# MAGIC      - ii)to rename multiple columns
# MAGIC     - iii)Dynamically rename all or multiple columns
# MAGIC
# MAGIC - syntax: withColumnRenamed("oldcolname","newcolname"))
# MAGIC

# COMMAND ----------

df4=df.withColumnRenamed("Empid","Ecode")
df4.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ii)renaming multiple columns
# MAGIC    rename--->Salary to income
# MAGIC              Sex to Gender
# MAGIC

# COMMAND ----------

df5=df.withColumnRenamed("Salary","Income").withColumnRenamed("Sex","Gender")
df5.show()

# COMMAND ----------

# MAGIC %md
# MAGIC iii)to change all the columns in a DF

# COMMAND ----------

newcolumns=["col1","col2","col3","col4","col5","col6"]
df6=df5.toDF(*newcolumns)
df6.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC filter()
# MAGIC  -we can perform the following
# MAGIC 1)Filter with column condition
# MAGIC   Filter those emps whose salaries>60000

# COMMAND ----------

df7=df.filter(df.Salary>60000)
df7.show()

# COMMAND ----------

df8=df.filter(df.Sex=='M')
df8.show()

# COMMAND ----------

df2=df.filter(df.City!="hyd")
df2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC 4)Filter based on list values
# MAGIC  isin() :using this function we can filter the elements which are in list
# MAGIC          or which are not in list
# MAGIC
# MAGIC Task: If we have multiple cities,then filter only those emps who belongs to
# MAGIC       the cities hyd,pune
# MAGIC

# COMMAND ----------

list1=["hyd","pune"]
df5=df.filter(df.City.isin(list1))
df5.show()

# COMMAND ----------

list2=["hyd"]
# Task: Filter those emps other than the city "hyd"
df6=df.filter(df.City.isin(list2)==False)
df6.show()

# COMMAND ----------

# Task: Filter those emps who belongs to the city "hyd"
list2=["hyd"]
df6=df.filter(df.City.isin(list2)==True)
df6.show()


# COMMAND ----------

# Task 4: Filter based on startswith,endswith
df7=df.filter(df.Empname.startswith("M"))
df7.show()

# COMMAND ----------

df8=df.filter(df.JoinDate.startswith("2021"))
df8.show()

# COMMAND ----------

# MAGIC %md
# MAGIC distinct() :For eliminating the duplicates

# COMMAND ----------

data=[("Rahul","mrkt",30000),("Blake","sales",40000),("John","hr",50000),("Blake","sales",40000),("Rahul","mrkt",30000)]
columns=["Empname","Dept","Salary"]
df1=spark.createDataFrame(data=data,schema=columns)
df1.show()


# COMMAND ----------

df2=df1.distinct()
df2.show()

# COMMAND ----------

#I want the count of employees
print("No of Employees=",df2.count())


# COMMAND ----------

df.sort("Salary",ascending=False).show()

# COMMAND ----------

df.sort(df.Salary.desc()).show()

# COMMAND ----------

df.sort(df.Salary.desc(),df.Empname.asc()).show()

# COMMAND ----------

#sort and col functions
df.sort(col("Salary").desc(),col("Empname").asc()).show()


# COMMAND ----------


