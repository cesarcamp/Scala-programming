// DATAFRAME PROJECT
// Netflix 2011-2016 stock prices

// For Scala/Spark $ Syntax
import spark.implicits._

import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()

// Load the Netflix Stock CSV File, have Spark infer the data types.
val df = spark.read.option("header","true").option("inferSchema","true").csv("Netflix_2011_2016.csv")
// Column names
df.columns
// Schema
df.printSchema()
// First five rows
df.head(5)
// Statistical data
df.describe.show()
// New dataframe with a column called HV Ratio that
// is the ratio of the High Price versus volume of stock traded
// for a day.
val new_df = df.withColumn("HV Ratio",df("High")/df("Volume"))
new_df.columns
// Day with the Peak High in Price?
df.orderBy($"High".desc).show(1)
// Mean of close column?
df.select(mean("Close")).show()
// Max and min of the Volume column
df.select(max("Volume"),min("Volume")).show()

// How many days was the Close lower than $ 600?
df.filter($"Close"<600).count() //Spark sintaxis
// or
df.filter("Close<600").count() //sql sintaxis
// What percentage of the time was the High greater than $500 ?
(df.filter($"High">500).count()*1.0 / df.count())*100
// What is the Pearson correlation between High and Volume?
df.select(corr("High","Volume")).show()
// What is the max High per year?
val yeardf = df.withColumn("Year",year(df("Date")))
val yearmax = yeardf.select($"Year",$"High").groupBy("Year").max()
yearmax.select($"Year",$"max(High)").show()
// What is the average Close for each Calender Month?
val monthdf = df.withColumn("Month", month(df("Date")))
val monthavg = monthdf.select($"Month",$"Close").groupBy("Month").mean()
monthavg.select($"Month",$"avg(Close)").orderBy("Month").show()
