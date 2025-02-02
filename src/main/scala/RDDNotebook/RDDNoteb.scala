package RDDNotebook

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import scala.collection.immutable.HashSet


object RDDNoteb {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD").setMaster("local")
    val sc = new SparkContext(conf)

    println(s"spark.version = ${sc.version}")  // spark.version = 3.5.0

    println("------------------------")

    // map
    val rddMap = sc.parallelize(List("a","b","c","d","e","f","g","x","y","z"), 3)
    val mappedRDD = rddMap.map(_.toUpperCase)
    println(mappedRDD.collect().mkString(",")) // "A,B,C,D,E,F,G,X,Y,Z"

    println("------------------------")

    // flatmap
    val rddFlatMap = sc.parallelize(List("some text here", "other text over there", "and text", "short", "and a longer one"), 3)
    val flatmappedRDD = rddFlatMap.flatMap(_.split("\\s"))
    println(flatmappedRDD.collect().mkString("/")) // "some/text/here/other/text/over/there/and/text/short/and/a/longer/one"

    println("------------------------")

    //filter
    val rddFilter = sc.parallelize(List("a", "b", "c", "d", "e", "f", "x", "y", "z"), 3)
    val filteredRDD = rddFilter.filter(_ < "f")
    println(filteredRDD.collect().mkString("/")) // "a/b/c/d/e"

    println("------------------------")

    // mapPartitions
    val rddMapPartitions = sc.parallelize(List("a", "b", "c", "d", "e", "f", "x", "y", "z"), 3)
    val mapPartitionsRDD = rddMapPartitions.mapPartitions(p => (Array("Hello").iterator))
    println(mapPartitionsRDD.collect().mkString("/")) // "Hello/Hello/Hello"

    println("------------------------")
    println("------------------------")
  }

}
