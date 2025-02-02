package RDDNotebook

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import scala.collection.immutable.HashSet


object RDDNoteb {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD").setMaster("local")
    val sc = new SparkContext(conf)

    println(s"spark.version = ${sc.version}")

    // Map
    val rddMap = sc.parallelize(List("a","b","c","d","e","f","g","x","y","z"), 3)
    val mappedRDD = rddMap.map(_.toUpperCase)
    println(mappedRDD.collect().mkString(","))
  }

}
