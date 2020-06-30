// import org.apache.spark.sql.SparkSession
//
// val spark = SparkSession.builder().getOrCreate()
//
// val df = spark.read.option("header", "true").option("inferSchema", "true").csv("CitiGroup2006_2008")

//print first lines
// for(row <- df.head(5)){
//     println(row)
// }

//show columns
//df.columns

//statistics as in pandas
// df.describe().show()

//select a column
//df.select("Volume").show()

//select multiple Columns
//df.select($"Date", $"Close").show()

//create new columns
//val df2 = df.withColumn("HighPlusLow",df("High")+df("Low"))

//info of the df
//df2.printSchema()

//rename a column andd show it plus show an additional column
//df2.select(df2("HighPlusLow").as("HPL"),df2("Close")).show()

/////////////
///DataFrame operations
////////////

// you can use spark sql or scala notations to perform operations, if you opt for scala
//you have to import spark.implicits
// df.printSchema()

import spark.implicits._

//SINGLE Filter
//df.filter($"Close">480).show()    SCALA
//df.filter("Close > 480").show()       SQL

// filter multiple
//df.filter($"Close" < 480 && $"High" < 480).show()      SPARK
//df.filter("Close < 480 AND  High > 480 ").show()       sql

//Transform dataframe to Array
//df.filter("Close < 480 AND High < 480").collect()       SQL

//COUN THE TOTAL RESULTS
//df.filter("Close < 480 AND High < 480").count()

// filter for specific condition    triple ===
//df.filter($"High" === 484.40).show()      SPARK
//df.filter("High = 484.40" ).show()        sql

//PEARSON CORRELATION
//df.select(corr("High","Low")).show()



import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()

// Create a DataFrame from Spark Session read csv
// Technically known as class Dataset
val df = spark.read.option("header","true").option("inferSchema","true").csv("Sales.csv")

// df.printSchema()

// df.groupBy("Company").mean().show()
// df.groupBy("Company").count().show()
// df.groupBy("Company").max().show()
// df.groupBy("Company").min().show()

// df.select(countDistinct("Sales")).show()
// df.select(sumDistinct("Sales")).show()
// df.select(variance("Sales")).show()
// df.select(stddev("Sales")).show()
// df.select(collect_set("Sales")).show()

//df.orderBy($"Sales".desc).show()


//////""" Missing Data """
///

import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()

// Grab small dataset with some missing data
val df = spark.read.option("header","true").option("inferSchema","true").csv("ContainsNull.csv")

// Show schema
//df.printSchema()

// Notice the missing values!
//df.show()

//df.na.drop().show() // drop any row with any null values
//df.na.drop(2).show() // drop any row that has less than 2 null values

//df.na.fill(100).show() // all the columns that are integers and are nulls will be filled with 100
//df.na.fill("No name").show() // fills all string columns with No Name
//df.na.fill("New Name", Array("Name")).show() // specify the columns which you want to fill


///
/////"""DATES AND TIMESTAMPS
///

// Start a simple Spark Session
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()

// Create a DataFrame from Spark Session read csv
// Technically known as class Dataset
val df = spark.read.option("header","true").option("inferSchema","true").csv("CitiGroup2006_2008")

// Show Schema
//df.printSchema()
//df.select(month(df("Date"))).show()

//df.select(year(df("Date"))).show()

val df2 = df.withColumn("Year",year(df("Date")))   //average per year
val dfavgs = df2.groupBy("Year").mean()

dfavgs.select($"Year", $"avg(Close)").show()


val df2 = df.withColumn("Year",year(df("Date")))   //average per year
val dfmins = df2.groupBy("Year").min() // minimum price

dfmins.select($"Year", $"min(Close)").show()
