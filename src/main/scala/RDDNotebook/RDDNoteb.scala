package RDDNotebook

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import scala.collection.immutable.HashSet
import org.apache.hadoop.fs._


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

    println("----------- aggregateByKey -------------")

    // aggregateByKey
    val rddAggregateByKey = sc.parallelize(List(("a", 1), ("b", 2), ("c", 3), ("a", 4), ("b", 5)), 3)
    val aggregateByKeyRDD = rddAggregateByKey.aggregateByKey(10)(_ * _, _ + _)
    println(aggregateByKeyRDD.collect().mkString("/")) // "(c,3)/(a,5)/(b,7)"

    println("------------ sortByKey ------------")

    // sortByKey
    val rddSortByKey = sc.parallelize(List(("a", 1), ("b", 2), ("c", 3), ("a", 4), ("b", 5)), 3)
    val sortByKeyRDD = rddSortByKey.sortByKey()
    println(sortByKeyRDD.collect().mkString("/")) // "(a,1)/(a,4)/(b,2)/(b,5)/(c,3)"

    println("------------ join ------------")

    // join
    val rddJoin1 = sc.parallelize(List((1, "a"), (2, "b"), (3, "c")))
    val rddJoin2 = sc.parallelize(List((2, "B"), (1, "A"), (3, "C")))
    val joinRDD = rddJoin1.join(rddJoin2)
    println(joinRDD.collect().mkString("/")) // "(1,(a,A))/(3,(c,C))/(2,(b,B))"

    println("----------- coalesce -------------")

    // coalesce
    val rddCoalesce = sc.parallelize(List("a", "b", "c", "d", "e", "f", "g", "h", "i"), 4)
    val coalesceRDD = rddCoalesce.coalesce(2)
    println(rddCoalesce.partitions.size) // 4
    println(coalesceRDD.partitions.size) // 2

    println("----------- repartition ------------")

    // repartition
    val rddRepartition = sc.parallelize(List("a", "b", "c", "d", "e", "f", "g", "h", "i"), 2)
    val repartitionRDD = rddRepartition.repartition(4)
    println(rddRepartition.partitions.length) // 2
    println(repartitionRDD.partitions.length) // 4

    println("------------- reduce -----------")

    // reduce
    val rddReduce = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9))
    val reduceRDD = rddReduce.reduce(_ + _)
    println(reduceRDD) // 45

    println("----------- collect -------------")

    // collect
    val rddCollect = sc.parallelize(List("a", "b", "c", "d", "e", "f", "g", "h", "i"), 3)
    val collectRDD = rddCollect.collect()
    println(collectRDD.mkString("/")) // "a/b/c/d/e/f/g/h/i"

    println("----------- count -------------")

    // count
    val rddCount = sc.parallelize(List("a", "b", "c", "d", "e", "f", "g", "h", "i"), 3)
    val countRDD = rddCount.count()
    println(countRDD) // 9

    println("------------ countByKey ------------")

    // countByKey
    val rddCountByKey = sc.parallelize(List(("a", 1), ("b", 2), ("c", 3), ("a", 4), ("b", 5)), 3)
    val countByKeyRDD = rddCountByKey.countByKey()
    println(countByKeyRDD.mkString("/")) // "c -> 1/a -> 2/b -> 2"

    println("------------ first ------------")

    // first
    val rddFirst = sc.parallelize(List("a", "b", "c", "d", "e", "f", "g", "h", "i"), 3)
    val firstRDD = rddFirst.first()
    println(firstRDD) // "a"

    println("------------ take ------------")

    // take
    val rddTake = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9), 3)
    val takeRDD = rddTake.take(5)
    println(takeRDD.mkString("/")) // "1/2/3/4/5"

    println("------------- takeSample -----------")

    // takeSample
    val rddTakeSample = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9), 3)
    val takeSampleRDD = rddSample.takeSample(false, 3)
    println(takeSampleRDD.mkString("/")) // "e/b/d"

    println("------------ takeOrdered ------------")

    // takeOrdered
    val rddTakeOrdered = sc.parallelize(List(1, 9, 2, 8, 3, 7, 4, 6, 5), 3)
    val takeOrderedRDD = rddTakeOrdered.takeOrdered(3)
    println(takeOrderedRDD.mkString("/")) // 1/2/3

    println("------------ foreach ------------")

    // foreach
    val rddForeach = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9), 3)
    rddForeach.foreach(i => println(s"The next is ${i}"))
    /*
    The next is 1
    The next is 2
    The next is 3
    The next is 4
    The next is 5
    The next is 6
    The next is 7
    The next is 8
    The next is 9
     */

    println("------------- saveAs -----------")

    // saveAs
    val fs = org.apache.hadoop.fs.FileSystem.get(sc.hadoopConfiguration)
    println(fs.getClass()) // class org.apache.hadoop.fs.LocalFileSystem

    val textFile: String = "testRDD.txt"

    val rddSaveAs = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9), 2)
    //rddSaveAs.saveAsTextFile(textFile)

    println("------------------------")

}


































