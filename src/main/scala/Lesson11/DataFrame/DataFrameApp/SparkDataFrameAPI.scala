package Lesson11.DataFrame.DataFrameApp


object SparkDataFrameAPI extends SparkSessionWrapper {
  def main(args: Array[String]): Unit = {

    //1 - Создание DataFrame

    println("***************************** Создание DataFrame *****************************")

    println("Create DataFrame from List:")
    DataFrameCreation.fromList().show(10)

    println("\nCreate DataFrame from RDD:")
    DataFrameCreation.fromRDD().show(10)

    println("\ncreateDataFrame:")
    DataFrameCreation.createDataFrame().show(10)

    println("\nCreate DataFrame with Schema:")
    DataFrameCreation.withSchema().show(10)

    println("\nCreate DataFrame from File:")
    DataFrameCreation.fromFile().show(10)

    //2 - Операции

    println("***************************** Операции *****************************")

    println("\nbasicOperations:")
    DataFrameOperations.basicOperations()

    println("\nfunctions:")
    DataFrameOperations.functions()

    println("\ngroupBy:")
    DataFrameOperations.groupBy()

    println("\njoin:")
    DataFrameOperations.join()
  }
}
