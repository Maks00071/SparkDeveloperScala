package Lesson11.DataFrame.DataFrameApp

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import Lesson11.DataFrame.DataFrameApp.DataFrameCreation.fromFile


object DataFrameOperations extends SparkSessionWrapper {

  import spark.implicits._

  private val customerDF: DataFrame = fromFile()

  def basicOperations(): Unit = {
    println("\ncustomerDF.printSchema:")
    customerDF.printSchema()

    //Select
    println("\ncustomerDF.select:")
    customerDF.select("Birthdate", "Country").show(10)
    customerDF.select(col("Country")).show(10)
    customerDF.select('Country).show(10)
    customerDF.selectExpr("Birthdate as Date").show(10)
    customerDF.withColumn("Flag", lit(true)).show(10)
    customerDF.withColumnRenamed("Birthdate", "Date").show(10)

    //Filter
    println("\ncustomerDF.filter:")
    customerDF.filter("Country = 'Norway'").show(10)
    customerDF.where('Country === "Iceland").show(10)

    //Sort
    println("\ncustomerDF.sort:")
    customerDF.sort('CustomerID.desc).show(10)
    customerDF.orderBy("CustomerID").show(10)

    //Repartition
    println("\ncustomerDF.repartition:")
    println(s"Num old partitions: ${customerDF.rdd.getNumPartitions}")
    val repartitionedDF = customerDF.repartition(5, col("Country"))
    println(s"New num partitions: ${repartitionedDF.rdd.getNumPartitions}")
    println(s"Num partitions after coalesce: ${customerDF.coalesce(1).rdd.getNumPartitions}")
  }

  def functions(): Unit = {
    customerDF.select(date_format(col("Birthdate"), "yyyy-MM-dd")).show(10)
    customerDF.withColumn("Identity", array('Name, 'Username)).printSchema()
  }

  def groupBy(): Unit = {
    customerDF
      .groupBy("Country")
      .agg(count(lit(1)))
      .show(10)
  }

  def join(): Unit = {
    customerDF.printSchema()

    val retailDF = spark.read
      .format("json")
      .option("mode", "FAILFAST")
      .load("data/retail_data.json")

    retailDF.printSchema()

    customerDF.join(retailDF, "CustomerID").show(10)
  }
}






































