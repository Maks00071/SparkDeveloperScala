package RDDNotebook

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import scala.collection.immutable.HashSet


object RDDNoteb {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDD").setMaster("local")
    val sc = new SparkContext(conf)

    println("------------- spark.version -------------")

    println(s"spark.version = ${sc.version}")  // spark.version = 3.5.0

    println("---------- map --------------")

    // map
    val rddMap = sc.parallelize(List("a","b","c","d","e","f","g","x","y","z"), 3)
    val mappedRDD = rddMap.map(_.toUpperCase)
    println(mappedRDD.collect().mkString(",")) // "A,B,C,D,E,F,G,X,Y,Z"

    println("----------- flatmap -------------")

    // flatmap
    val rddFlatMap = sc.parallelize(List("some text here", "other text over there", "and text", "short", "and a longer one"), 3)
    val flatmappedRDD = rddFlatMap.flatMap(_.split("\\s"))
    println(flatmappedRDD.collect().mkString("/")) // "some/text/here/other/text/over/there/and/text/short/and/a/longer/one"

    println("----------- filter -------------")

    // filter
    val rddFilter = sc.parallelize(List("a", "b", "c", "d", "e", "f", "x", "y", "z"), 3)
    val filteredRDD = rddFilter.filter(_ < "f")
    println(filteredRDD.collect().mkString("/")) // "a/b/c/d/e"

    println("----------- mapPartitions -------------")

    // mapPartitions
    val rddMapPartitions = sc.parallelize(List("a", "b", "c", "d", "e", "f", "x", "y", "z"), 3)
    val mapPartitionsRDD = rddMapPartitions.mapPartitions(p => (Array("Hello").iterator))
    println(mapPartitionsRDD.collect().mkString("/")) // "Hello/Hello/Hello"

    println("----------- mapPartitionsWithIndex -------------")

    // mapPartitionsWithIndex
    val rddMappartitionsWithIndex = sc.parallelize(List("a", "b", "c", "d", "e", "f", "x", "y", "z"), 3)
    val mapPartitionsWithRDD = rddMappartitionsWithIndex.mapPartitionsWithIndex((i,j) => Array(i).iterator)
    println(mapPartitionsWithRDD.collect().mkString("/")) // "0/1/2"

    println("----------- sample -------------")

    // sample
    val rddSample = sc.parallelize(List("a", "b", "c", "d", "e", "f", "x", "y", "z"), 3)
    val sampleRDD = rddSample.sample(false, 0.3)
    println(sampleRDD.collect().mkString("/")) // "b/c/d/x"

    println("------------ union ------------")

    // union
    val rddUnion1 = sc.parallelize(List("a", "b", "c", "d", "e", "f", "x", "y", "z"), 3)
    val rddUnion2 = sc.parallelize(List("A", "B", "C", "D", "E", "F", "X", "Y", "Z"), 3)
    val unionRDD = rddUnion1.union(rddUnion2)
    println(unionRDD.collect().mkString("/")) // "a/b/c/d/e/f/x/y/z/A/B/C/D/E/F/X/Y/Z"

    println("------------ intersection ------------")

    // intersection
    val rddIntersection1 = sc.parallelize(List("a", "b", "c", "d", "e", "f"), 3)
    val rddIntersection2 = sc.parallelize(List("d", "e", "f", "x", "y", "z"), 3)
    val intersectionRDD = rddIntersection1.intersection(rddIntersection2)
    println(intersectionRDD.collect().mkString("/")) // "f/d/e"

    println("------------ distinct ------------")

    // distinct
    val rddDistinct = sc.parallelize(List("a", "b", "c", "c", "c", "b", "b", "a"), 3)
    val distinctRDD = rddDistinct.distinct()
    try {
      println(distinctRDD.collect().mkString("/")) // "c/a/b"
    } catch {
      case ex: ClassNotFoundException => println(ex)
    }

    println("------------ groupByKey ------------")

    // groupByKey
    val rddGroupByKey = sc.parallelize(List((1,"a"),(2,"b"),(3,"c"),(1,"d"),(2,"e")), 3)
    val groupByKeyRDD = rddGroupByKey.groupByKey()
    println(groupByKeyRDD.collect().mkString("/")) // (3,CompactBuffer(c))/(1,CompactBuffer(a, d))/(2,CompactBuffer(b, e))

    println("------------ reduceByKey ------------")

    // reduceByKey
    val rddReduceByKey = sc.parallelize(List(("a", 1), ("b", 2), ("c", 3), ("a", 4), ("b", 5)), 3)
    val reduceByKeyRDD = rddReduceByKey.reduceByKey((a, b) => a + b)
    println(reduceByKeyRDD.collect().mkString("/")) // "(c,3)/(a,5)/(b,7)"

    println("------------------------")

    // aggregateByKey
    val rddAggregateByKey = sc.parallelize(List(("a", 1), ("b", 2), ("c", 3), ("a", 4), ("b", 5)), 3)
    val aggregateByKeyRDD = rddAggregateByKey.aggregateByKey(10)(_ * _, _ + _)
    println(aggregateByKeyRDD.collect().mkString("/")) // "(c,3)/(a,5)/(b,7)"

    println("------------------------")
    println("------------------------")
    println("------------------------")
    println("------------------------")
    println("------------------------")
  }
}


































