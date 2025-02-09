package Lesson11.DataFrame.Notebooks

import org.apache.avro.Schema
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

import collection.JavaConverters._


object NotebooksApp {
  def main(args: Array[String]): Unit = {
    //class SparkSession extends Serializable with Closeable with Logging
    //builder - creates a "new SparkSession.Builder" instance
    //appName - sets the name of your Spark application
    //master - sets the cluster URL to connect to (use "local[*]" for local testing)
    //getOrCreate - creates a new SparkSession or returns an existing one if available
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Data Frame API")
      .getOrCreate()

    import spark.implicits._

    println(s"*************************** Spark: ${spark.version} ***************************")

    println(s"*************************** DataFrame from List ***************************")

    // << 1. Создание Data Frame >>
    // DataFrame from List - создание DataFrame из последовательности
    val data1 = Seq(("1", "Spark"), ("2", "Java"), ("3", "Scala"))
    println(data1) //List((1,Spark), (2,Java), (3,Scala))

    val df1 = spark.createDataFrame(data1)
    println(df1) // [_1: string, _2: string]
    df1.show()
    /*
    +---+-----+
    | _1|   _2|
    +---+-----+
    |  1|Spark|
    |  2| Java|
    |  3|Scala|
    +---+-----+
     */

    //переименуем столбцы
    val dfWithNames = df1.withColumnsRenamed(Map("_1" -> "StudentID", "_2" -> "Course"))
    dfWithNames.show()
    /*
    +---------+------+
    |StudentID|Course|
    +---------+------+
    |        1| Spark|
    |        2|  Java|
    |        3| Scala|
    +---------+------+
     */

    println(s"*************************** DataFRame from Schema ***************************")

    //DataFRame from Schema - создание DataFrame из схемы
    val schema1 = StructType(Array(StructField("StudentID", StringType, false),
      StructField("Course", StringType, true)))

    val schema2 = new StructType()
      .add(StructField("StudentID", StringType, false))
      .add(StructField("Course", StringType, true))

    val schema3 = StructType(
      StructField("StudentID", StringType, false) ::
        StructField("Course", StringType, true) :: Nil)

    println(schema1.equals(schema2)) //true
    println(schema2.equals(schema3)) //true


    val data2 = data1.map(s => Row(s._1,s._2)).asJava
    println(data2) // [[1,Spark], [2,Java], [3,Scala]]
    val df2 = spark.createDataFrame(data2, schema1)
    df2.show()
    /*
    +---------+------+
    |StudentID|Course|
    +---------+------+
    |        1| Spark|
    |        2|  Java|
    |        3| Scala|
    +---------+------+
     */

    println(s"*************************** DataFRame from RDD ***************************")

    // DataFrame from RDD - создание DataFrame из RDD
    val rdd1 = spark.sparkContext.parallelize(data1)
    val dfFromRDD1 = spark.createDataFrame(rdd1)
    dfFromRDD1.show()
    /*
    +---+-----+
    | _1|   _2|
    +---+-----+
    |  1|Spark|
    |  2| Java|
    |  3|Scala|
    +---+-----+
     */

    // RDD with Schema
    val rdd2 = rdd1.map(s => Row(s._1,s._2))
    val dfFromRDD2 = spark.createDataFrame(rdd2, schema1)
    dfFromRDD2.show()
    /*
    +---------+------+
    |StudentID|Course|
    +---------+------+
    |        1| Spark|
    |        2|  Java|
    |        3| Scala|
    +---------+------+
     */

    println(s"*************************** DataFRame from List with toDF ***************************")

    // toDF - создание DataFrame с помощью функции toDF
    // DataFrame from List
    val df11 = data1.toDF()
    df11.show()
    /*
    +---+-----+
    | _1|   _2|
    +---+-----+
    |  1|Spark|
    |  2| Java|
    |  3|Scala|
    +---+-----+
     */

    data1.toDF("StudentID","Course").show()
    /*
    +---------+------+
    |StudentID|Course|
    +---------+------+
    |        1| Spark|
    |        2|  Java|
    |        3| Scala|
    +---------+------+
     */

    val columns = List("StudentID","Course")
    val df12 = data1.toDF(columns: _*)
    df12.show()
    /*
    +---------+------+
    |StudentID|Course|
    +---------+------+
    |        1| Spark|
    |        2|  Java|
    |        3| Scala|
    +---------+------+
     */

    println(s"*************************** DataFRame from RDD with toDF ***************************")

    //from RDD
    val dfFromRDD11 = rdd1.toDF()
    dfFromRDD11.show()
    /*
    +---+-----+
    | _1|   _2|
    +---+-----+
    |  1|Spark|
    |  2| Java|
    |  3|Scala|
    +---+-----+
     */

    val dfFromRDD12 = rdd1.toDF(columns: _*)
    dfFromRDD12.show()
    /*
    +---------+------+
    |StudentID|Course|
    +---------+------+
    |        1| Spark|
    |        2|  Java|
    |        3| Scala|
    +---------+------+
     */

    println(s"*************************** DataFRame from file ***************************")

    //DataFrame from file
    val dfFromFile1 = spark.read.format("json").load("data/customer_data.json")
    dfFromFile1.show(5, false)
      /*
      +------------------------------------------------------+-------------------+--------------+----------+------------------------+---------------+----------------+
      |Address                                               |Birthdate          |Country       |CustomerID|Email                   |Name           |Username        |
      +------------------------------------------------------+-------------------+--------------+----------+------------------------+---------------+----------------+
      |Unit 1047 Box 4089\nDPO AA 57348                      |1994-02-20 00:46:27|United Kingdom|12346     |cooperalexis@hotmail.com|Lindsay Cowan  |valenciajennifer|
      |55711 Janet Plaza Apt. 865\nChristinachester, CT 62716|1988-06-21 00:15:34|Iceland       |12347     |timothy78@hotmail.com   |Katherine David|hillrachel      |
      |Unit 2676 Box 9352\nDPO AA 38560                      |1974-11-26 15:30:20|Finland       |12348     |tcrawford@gmail.com     |Leslie Martinez|serranobrian    |
      |2765 Powers Meadow\nHeatherfurt, CT 53165             |1977-05-06 23:57:35|Italy         |12349     |dustin37@yahoo.com      |Brad Cardenas  |charleshudson   |
      |17677 Mark Crest\nWalterberg, IA 39017                |1996-09-13 19:14:27|Norway        |12350     |amyholland@yahoo.com    |Natalie Ford   |gregoryharrison |
      +------------------------------------------------------+-------------------+--------------+----------+------------------------+---------------+----------------+
       */

    dfFromFile1.printSchema()
    /*
    root
     |-- Address: string (nullable = true)
     |-- Birthdate: string (nullable = true)
     |-- Country: string (nullable = true)
     |-- CustomerID: string (nullable = true)
     |-- Email: string (nullable = true)
     |-- Name: string (nullable = true)
     |-- Username: string (nullable = true)
     */

    val fileSchema = StructType(
        StructField("Address", StringType, true) ::
        StructField("Birthdate", StringType, true) ::
        StructField("Country", StringType, true) ::
        StructField("CustomerID", StringType, true) ::
        StructField("Email", StringType, true) ::
        StructField("Name", StringType, true) ::
        StructField("Username", StringType, true) ::
        Nil)

    val dfFromFile2 = spark.read.format("json").schema(fileSchema).load("data/customer_data.json")
    dfFromFile2.show(5, false)
    /*
    +------------------------------------------------------+-------------------+--------------+----------+------------------------+---------------+----------------+
    |Address                                               |Birthdate          |Country       |CustomerID|Email                   |Name           |Username        |
    +------------------------------------------------------+-------------------+--------------+----------+------------------------+---------------+----------------+
    |Unit 1047 Box 4089\nDPO AA 57348                      |1994-02-20 00:46:27|United Kingdom|12346     |cooperalexis@hotmail.com|Lindsay Cowan  |valenciajennifer|
    |55711 Janet Plaza Apt. 865\nChristinachester, CT 62716|1988-06-21 00:15:34|Iceland       |12347     |timothy78@hotmail.com   |Katherine David|hillrachel      |
    |Unit 2676 Box 9352\nDPO AA 38560                      |1974-11-26 15:30:20|Finland       |12348     |tcrawford@gmail.com     |Leslie Martinez|serranobrian    |
    |2765 Powers Meadow\nHeatherfurt, CT 53165             |1977-05-06 23:57:35|Italy         |12349     |dustin37@yahoo.com      |Brad Cardenas  |charleshudson   |
    |17677 Mark Crest\nWalterberg, IA 39017                |1996-09-13 19:14:27|Norway        |12350     |amyholland@yahoo.com    |Natalie Ford   |gregoryharrison |
    +------------------------------------------------------+-------------------+--------------+----------+------------------------+---------------+----------------+
     */

    println(s"*************************** Основные операции с DataFrame ***************************")

    //<< 2.Основные операции с DataFrame >>
    val customerDF = dfFromFile1
    customerDF.printSchema()
    /*
    root
     |-- Address: string (nullable = true)
     |-- Birthdate: string (nullable = true)
     |-- Country: string (nullable = true)
     |-- CustomerID: string (nullable = true)
     |-- Email: string (nullable = true)
     |-- Name: string (nullable = true)
     |-- Username: string (nullable = true)
     */

    println(customerDF.schema)
    /*
    StructType(
      StructField(Address,StringType,true),
      StructField(Birthdate,StringType,true),
      StructField(Country,StringType,true),
      StructField(CustomerID,StringType,true),
      StructField(Email,StringType,true),
      StructField(Name,StringType,true),
      StructField(Username,StringType,true)
      )
     */

    val customerSchema: StructType = customerDF.schema
    println(customerSchema)
    /*
    StructType(
      StructField(Address,StringType,true),
      StructField(Birthdate,StringType,true),
      StructField(Country,StringType,true),
      StructField(CustomerID,StringType,true),
      StructField(Email,StringType,true),
      StructField(Name,StringType,true),
      StructField(Username,StringType,true)
      )
     */

    println(customerDF.dtypes.mkString("Array(", ", ", ")"))
    /*
    Array(
      (Address,StringType),
      (Birthdate,StringType),
      (Country,StringType),
      (CustomerID,StringType),
      (Email,StringType),
      (Name,StringType),
      (Username,StringType)
      )
     */

    val h = customerDF.head()
    println(h(1)) // "1994-02-20 00:46:27"

    println(s"*************************** select ***************************")

    customerDF.select("Birthdate", "Country").show()
    /*
    +-------------------+--------------+
    |          Birthdate|       Country|
    +-------------------+--------------+
    |1994-02-20 00:46:27|United Kingdom|
    |1988-06-21 00:15:34|       Iceland|
    |1974-11-26 15:30:20|       Finland|
    |1977-05-06 23:57:35|         Italy|
    |1996-09-13 19:14:27|        Norway|
    |1969-06-21 03:39:20|        Norway|
    |1993-02-25 18:37:29|       Bahrain|
    |1993-03-13 12:37:34|         Spain|
    |1972-11-10 12:01:08|       Bahrain|
    |1973-01-13 17:17:26|      Portugal|
    |1989-11-24 17:12:54|   Switzerland|
    |1977-06-19 22:35:52|       Austria|
    |1983-09-21 05:22:18|        Cyprus|
    |1980-10-28 17:25:59|       Austria|
    |1982-09-01 09:12:57|       Belgium|
    |1979-02-03 03:42:47|       Belgium|
    |1974-12-21 13:27:20|   Unspecified|
    |1990-07-17 15:47:12|       Belgium|
    |1981-07-10 00:35:00|        Cyprus|
    |1989-12-26 00:58:01|       Denmark|
    +-------------------+--------------+
    only showing top 20 rows
     */

    customerDF.select(col("Country")).show()
    //аналогично: customerDF.select("Country").show()
    /*
    +--------------+
    |       Country|
    +--------------+
    |United Kingdom|
    |       Iceland|
    |       Finland|
    |         Italy|
    |        Norway|
    |        Norway|
    |       Bahrain|
    |         Spain|
    |       Bahrain|
    |      Portugal|
    |   Switzerland|
    |       Austria|
    |        Cyprus|
    |       Austria|
    |       Belgium|
    |       Belgium|
    |   Unspecified|
    |       Belgium|
    |        Cyprus|
    |       Denmark|
    +--------------+
    only showing top 20 rows
     */

    println(s"*************************** selectExpr ***************************")

    customerDF.selectExpr("Birthdate as Date").show()
    /*
    +-------------------+
    |               Date|
    +-------------------+
    |1994-02-20 00:46:27|
    |1988-06-21 00:15:34|
    |1974-11-26 15:30:20|
    |1977-05-06 23:57:35|
    |1996-09-13 19:14:27|
    |1969-06-21 03:39:20|
    |1993-02-25 18:37:29|
    |1993-03-13 12:37:34|
    |1972-11-10 12:01:08|
    |1973-01-13 17:17:26|
    |1989-11-24 17:12:54|
    |1977-06-19 22:35:52|
    |1983-09-21 05:22:18|
    |1980-10-28 17:25:59|
    |1982-09-01 09:12:57|
    |1979-02-03 03:42:47|
    |1974-12-21 13:27:20|
    |1990-07-17 15:47:12|
    |1981-07-10 00:35:00|
    |1989-12-26 00:58:01|
    +-------------------+
    only showing top 20 rows
     */

    customerDF.show(5, false)
    /*+------------------------------------------------------+-------------------+--------------+----------+------------------------+---------------+----------------+
    |Address                                               |Birthdate          |Country       |CustomerID|Email                   |Name           |Username        |
    +------------------------------------------------------+-------------------+--------------+----------+------------------------+---------------+----------------+
    |Unit 1047 Box 4089\nDPO AA 57348                      |1994-02-20 00:46:27|United Kingdom|12346     |cooperalexis@hotmail.com|Lindsay Cowan  |valenciajennifer|
    |55711 Janet Plaza Apt. 865\nChristinachester, CT 62716|1988-06-21 00:15:34|Iceland       |12347     |timothy78@hotmail.com   |Katherine David|hillrachel      |
    |Unit 2676 Box 9352\nDPO AA 38560                      |1974-11-26 15:30:20|Finland       |12348     |tcrawford@gmail.com     |Leslie Martinez|serranobrian    |
    |2765 Powers Meadow\nHeatherfurt, CT 53165             |1977-05-06 23:57:35|Italy         |12349     |dustin37@yahoo.com      |Brad Cardenas  |charleshudson   |
    |17677 Mark Crest\nWalterberg, IA 39017                |1996-09-13 19:14:27|Norway        |12350     |amyholland@yahoo.com    |Natalie Ford   |gregoryharrison |
    +------------------------------------------------------+-------------------+--------------+----------+------------------------+---------------+----------------+
     */

    println(s"*************************** withColumn ***************************")

    //добавление столбца "Flag"
    customerDF.withColumn("Flag", lit(true)).show(5, false)
    /*
    +------------------------------------------------------+-------------------+--------------+----------+------------------------+---------------+----------------+----+
    |Address                                               |Birthdate          |Country       |CustomerID|Email                   |Name           |Username        |Flag|
    +------------------------------------------------------+-------------------+--------------+----------+------------------------+---------------+----------------+----+
    |Unit 1047 Box 4089\nDPO AA 57348                      |1994-02-20 00:46:27|United Kingdom|12346     |cooperalexis@hotmail.com|Lindsay Cowan  |valenciajennifer|true|
    |55711 Janet Plaza Apt. 865\nChristinachester, CT 62716|1988-06-21 00:15:34|Iceland       |12347     |timothy78@hotmail.com   |Katherine David|hillrachel      |true|
    |Unit 2676 Box 9352\nDPO AA 38560                      |1974-11-26 15:30:20|Finland       |12348     |tcrawford@gmail.com     |Leslie Martinez|serranobrian    |true|
    |2765 Powers Meadow\nHeatherfurt, CT 53165             |1977-05-06 23:57:35|Italy         |12349     |dustin37@yahoo.com      |Brad Cardenas  |charleshudson   |true|
    |17677 Mark Crest\nWalterberg, IA 39017                |1996-09-13 19:14:27|Norway        |12350     |amyholland@yahoo.com    |Natalie Ford   |gregoryharrison |true|
    +------------------------------------------------------+-------------------+--------------+----------+------------------------+---------------+----------------+----+
     */

    println(s"*************************** withColumnRenamed ***************************")

    val dfRenamed = customerDF.withColumnRenamed("Birthdate", "Date")
    dfRenamed.show(5, false)
    /*
    +------------------------------------------------------+-------------------+--------------+----------+------------------------+---------------+----------------+
    |Address                                               |Date               |Country       |CustomerID|Email                   |Name           |Username        |
    +------------------------------------------------------+-------------------+--------------+----------+------------------------+---------------+----------------+
    |Unit 1047 Box 4089\nDPO AA 57348                      |1994-02-20 00:46:27|United Kingdom|12346     |cooperalexis@hotmail.com|Lindsay Cowan  |valenciajennifer|
    |55711 Janet Plaza Apt. 865\nChristinachester, CT 62716|1988-06-21 00:15:34|Iceland       |12347     |timothy78@hotmail.com   |Katherine David|hillrachel      |
    |Unit 2676 Box 9352\nDPO AA 38560                      |1974-11-26 15:30:20|Finland       |12348     |tcrawford@gmail.com     |Leslie Martinez|serranobrian    |
    |2765 Powers Meadow\nHeatherfurt, CT 53165             |1977-05-06 23:57:35|Italy         |12349     |dustin37@yahoo.com      |Brad Cardenas  |charleshudson   |
    |17677 Mark Crest\nWalterberg, IA 39017                |1996-09-13 19:14:27|Norway        |12350     |amyholland@yahoo.com    |Natalie Ford   |gregoryharrison |
    +------------------------------------------------------+-------------------+--------------+----------+------------------------+---------------+----------------+
     */

    println(customerDF.schema.mkString("Array(", ", ", ")"))
    /*
    Array(
      StructField(Address,StringType,true),
      StructField(Birthdate,StringType,true),
      StructField(Country,StringType,true),
      StructField(CustomerID,StringType,true),
      StructField(Email,StringType,true),
      StructField(Name,StringType,true),
      StructField(Username,StringType,true)
      )
     */

    println(dfRenamed.getClass) //class org.apache.spark.sql.Dataset

    println(dfRenamed.schema.mkString("Array(", ", ", ")"))
    /*
    Array(
      StructField(Address,StringType,true),
      StructField(Date,StringType,true),     -- измененное поле
      StructField(Country,StringType,true),
      StructField(CustomerID,StringType,true),
      StructField(Email,StringType,true),
      StructField(Name,StringType,true),
      StructField(Username,StringType,true)
      )
     */

    println(s"*************************** filter ***************************")

    //filter
    customerDF.filter("Country = 'Norway'").show()
    /*
    +--------------------+-------------------+-------+----------+--------------------+----------------+---------------+
    |             Address|          Birthdate|Country|CustomerID|               Email|            Name|       Username|
    +--------------------+-------------------+-------+----------+--------------------+----------------+---------------+
    |17677 Mark Crest\...|1996-09-13 19:14:27| Norway|     12350|amyholland@yahoo.com|    Natalie Ford|gregoryharrison|
    |50047 Smith Point...|1969-06-21 03:39:20| Norway|     12352| vcarter@hotmail.com|     Dana Clarke|         hmyers|
    |0297 Jacob Ranch ...|1990-06-01 14:49:52| Norway|     12381|douglaschavez@hot...|   Matthew Jones|    stephanie68|
    |3102 Hopkins Walk...|1976-06-19 08:10:24| Norway|     12430|crystalromero@hot...|       Lisa Tate|       pgilbert|
    |637 Philip Lock S...|1984-06-06 09:36:14| Norway|     12432|jessica87@hotmail...|  Cheryl Herring|mathewsnicholas|
    |546 Tyler Prairie...|1985-05-27 10:39:47| Norway|     12433|mariahmcpherson@g...|  Kaitlin Miller|         lyoung|
    |6270 Jennifer Pra...|1977-06-01 20:40:04| Norway|     12436|lynncynthia@hotma...|    Rodney Giles|       swiggins|
    |415 Megan Parkway...|1971-08-29 06:21:22| Norway|     12438|  jeff42@hotmail.com| Thomas Figueroa|  matthewharris|
    |PSC 4266, Box 099...|1971-09-03 05:42:49| Norway|     12444| richard20@gmail.com|     Meghan Wood|   salazarbilly|
    |1333 Michael Vill...|1995-03-09 03:25:02| Norway|     12752|seanrobles@gmail.com|Lauren Hernandez|    morrowhenry|
    +--------------------+-------------------+-------+----------+--------------------+----------------+---------------+
     */

    println(s"*************************** where ***************************")

    //=!= - не равно
    customerDF.where($"Country" =!= "Iceland").show(5, false)
    /*
    +--------------------------------------------------+-------------------+--------------+----------+------------------------+---------------+----------------+
    |Address                                           |Birthdate          |Country       |CustomerID|Email                   |Name           |Username        |
    +--------------------------------------------------+-------------------+--------------+----------+------------------------+---------------+----------------+
    |Unit 1047 Box 4089\nDPO AA 57348                  |1994-02-20 00:46:27|United Kingdom|12346     |cooperalexis@hotmail.com|Lindsay Cowan  |valenciajennifer|
    |Unit 2676 Box 9352\nDPO AA 38560                  |1974-11-26 15:30:20|Finland       |12348     |tcrawford@gmail.com     |Leslie Martinez|serranobrian    |
    |2765 Powers Meadow\nHeatherfurt, CT 53165         |1977-05-06 23:57:35|Italy         |12349     |dustin37@yahoo.com      |Brad Cardenas  |charleshudson   |
    |17677 Mark Crest\nWalterberg, IA 39017            |1996-09-13 19:14:27|Norway        |12350     |amyholland@yahoo.com    |Natalie Ford   |gregoryharrison |
    |50047 Smith Point Suite 162\nWilkinsstad, PA 04106|1969-06-21 03:39:20|Norway        |12352     |vcarter@hotmail.com     |Dana Clarke    |hmyers          |
    +--------------------------------------------------+-------------------+--------------+----------+------------------------+---------------+----------------+
     */

    println(s"*************************** sort ***************************")

    customerDF.sort(col("CustomerID").desc).show(10, false)
    /*
    +-----------------------------------------------------+-------------------+--------------+----------+-----------------------+--------------------+-----------------+
    |Address                                              |Birthdate          |Country       |CustomerID|Email                  |Name                |Username         |
    +-----------------------------------------------------+-------------------+--------------+----------+-----------------------+--------------------+-----------------+
    |6942 Connie Skyway\nPatrickville, WA 16551           |1973-10-24 00:52:10|United Kingdom|12989     |amber97@hotmail.com    |Brandon Contreras   |ecasey           |
    |79375 David Neck\nWest Matthewton, NJ 92863          |1971-05-04 22:20:10|United Kingdom|12988     |erica98@gmail.com      |Gabriel Romero      |qknight          |
    |00881 West Flat\nNorth Emily, IL 32130               |1997-03-05 19:20:57|United Kingdom|12987     |vkeith@yahoo.com       |Christopher Lawrence|smcintyre        |
    |499 Jonathan Streets Apt. 890\nEast Ashley, MD 76825 |1987-10-24 20:05:15|United Kingdom|12985     |fredsmith@yahoo.com    |Xavier Myers        |stricklandjeffery|
    |9505 Melissa Streets\nSouth Frankville, NJ 91189     |1975-09-22 15:21:58|United Kingdom|12984     |scottjonathan@yahoo.com|Brandy Huang        |amandawilliams   |
    |399 Fuentes Roads\nJoshuaborough, CO 64522           |1986-09-30 18:12:45|United Kingdom|12982     |cynthia31@hotmail.com  |Linda Stephens      |davidsonomar     |
    |1573 Jessica Glen\nSouth Christian, CA 15700         |1984-07-23 03:09:18|United Kingdom|12981     |esharp@hotmail.com     |Dawn Woods          |steven67         |
    |153 Tara Ridges Suite 028\nPort Anthonytown, MA 16004|1974-03-03 07:52:15|United Kingdom|12980     |jessica87@gmail.com    |Sean Brooks         |kristen26        |
    |62134 Chen Valleys\nNorth Deniseshire, WV 89532      |1990-10-09 01:29:02|United Kingdom|12977     |fmatthews@gmail.com    |Kyle Simon          |emily28          |
    |7521 Christopher Way\nNorth Anitamouth, NV 99746     |1973-10-10 23:57:51|United Kingdom|12976     |williamsheidi@yahoo.com|Hannah Rose         |eugene04         |
    +-----------------------------------------------------+-------------------+--------------+----------+-----------------------+--------------------+-----------------+
     */

    println(s"*************************** orderBy ***************************")

    customerDF.orderBy("CustomerID").show(10, false)
    /*
    +------------------------------------------------------+-------------------+--------------+----------+------------------------+-----------------+----------------+
    |Address                                               |Birthdate          |Country       |CustomerID|Email                   |Name             |Username        |
    +------------------------------------------------------+-------------------+--------------+----------+------------------------+-----------------+----------------+
    |Unit 1047 Box 4089\nDPO AA 57348                      |1994-02-20 00:46:27|United Kingdom|12346     |cooperalexis@hotmail.com|Lindsay Cowan    |valenciajennifer|
    |55711 Janet Plaza Apt. 865\nChristinachester, CT 62716|1988-06-21 00:15:34|Iceland       |12347     |timothy78@hotmail.com   |Katherine David  |hillrachel      |
    |Unit 2676 Box 9352\nDPO AA 38560                      |1974-11-26 15:30:20|Finland       |12348     |tcrawford@gmail.com     |Leslie Martinez  |serranobrian    |
    |2765 Powers Meadow\nHeatherfurt, CT 53165             |1977-05-06 23:57:35|Italy         |12349     |dustin37@yahoo.com      |Brad Cardenas    |charleshudson   |
    |17677 Mark Crest\nWalterberg, IA 39017                |1996-09-13 19:14:27|Norway        |12350     |amyholland@yahoo.com    |Natalie Ford     |gregoryharrison |
    |50047 Smith Point Suite 162\nWilkinsstad, PA 04106    |1969-06-21 03:39:20|Norway        |12352     |vcarter@hotmail.com     |Dana Clarke      |hmyers          |
    |633 Miller Turnpike\nJonathanland, OR 62874           |1993-02-25 18:37:29|Bahrain       |12353     |laura34@yahoo.com       |Gary Nichols     |andrewhamilton  |
    |38456 Rachael Causeway Apt. 735\nEvanfort, AR 33893   |1993-03-13 12:37:34|Spain         |12354     |zmelton@gmail.com       |John Parks       |matthewray      |
    |4140 Pamela Hollow Apt. 849\nEast Elizabeth, TN 29566 |1972-11-10 12:01:08|Bahrain       |12355     |scott50@yahoo.com       |Jennifer Lawrence|glopez          |
    |8681 Karen Roads Apt. 096\nLowehaven, IA 19798        |1973-01-13 17:17:26|Portugal      |12356     |josephmacias@hotmail.com|James Sanchez    |wesley20        |
    +------------------------------------------------------+-------------------+--------------+----------+------------------------+-----------------+----------------+
     */

    println(s"*************************** repartition ***************************")

    //увеличить кол-во партиций по полю
    val repartitionedDF = customerDF.repartition(5, col("Country"))
    println(s"Old num partitions: ${customerDF.rdd.getNumPartitions}") // 1
    println(s"New num partitions: ${repartitionedDF.rdd.getNumPartitions}") // 5

    println(s"*************************** coalesce ***************************")

    //уменьшение кол-во партиций
    println(s"Num partitions after coalesce: ${repartitionedDF.coalesce(1).rdd.getNumPartitions}") // 1

    println(s"*************************** functions ***************************")

    val datDF = customerDF.select($"Birthdate", date_format(col("Birthdate"), "yyyy-MM-dd").alias("bd"))
    datDF.show(10, false)
    /*
    +-------------------+----------+
    |Birthdate          |bd        |
    +-------------------+----------+
    |1994-02-20 00:46:27|1994-02-20|
    |1988-06-21 00:15:34|1988-06-21|
    |1974-11-26 15:30:20|1974-11-26|
    |1977-05-06 23:57:35|1977-05-06|
    |1996-09-13 19:14:27|1996-09-13|
    |1969-06-21 03:39:20|1969-06-21|
    |1993-02-25 18:37:29|1993-02-25|
    |1993-03-13 12:37:34|1993-03-13|
    |1972-11-10 12:01:08|1972-11-10|
    |1973-01-13 17:17:26|1973-01-13|
    +-------------------+----------+
     */

    datDF.printSchema()
    /*
    root
     |-- Birthdate: string (nullable = true)
     |-- bd: string (nullable = true)
     */

    customerDF.withColumn("Identity", array($"Name", $"Username")).printSchema()
    /*
    root
     |-- Address: string (nullable = true)
     |-- Birthdate: string (nullable = true)
     |-- Country: string (nullable = true)
     |-- CustomerID: string (nullable = true)
     |-- Email: string (nullable = true)
     |-- Name: string (nullable = true)
     |-- Username: string (nullable = true)
     |-- Identity: array (nullable = false)
     |    |-- element: string (containsNull = true)
     */

    customerDF.show(5, false)
    /*
    +------------------------------------------------------+-------------------+--------------+----------+------------------------+---------------+----------------+
    |Address                                               |Birthdate          |Country       |CustomerID|Email                   |Name           |Username        |
    +------------------------------------------------------+-------------------+--------------+----------+------------------------+---------------+----------------+
    |Unit 1047 Box 4089\nDPO AA 57348                      |1994-02-20 00:46:27|United Kingdom|12346     |cooperalexis@hotmail.com|Lindsay Cowan  |valenciajennifer|
    |55711 Janet Plaza Apt. 865\nChristinachester, CT 62716|1988-06-21 00:15:34|Iceland       |12347     |timothy78@hotmail.com   |Katherine David|hillrachel      |
    |Unit 2676 Box 9352\nDPO AA 38560                      |1974-11-26 15:30:20|Finland       |12348     |tcrawford@gmail.com     |Leslie Martinez|serranobrian    |
    |2765 Powers Meadow\nHeatherfurt, CT 53165             |1977-05-06 23:57:35|Italy         |12349     |dustin37@yahoo.com      |Brad Cardenas  |charleshudson   |
    |17677 Mark Crest\nWalterberg, IA 39017                |1996-09-13 19:14:27|Norway        |12350     |amyholland@yahoo.com    |Natalie Ford   |gregoryharrison |
    +------------------------------------------------------+-------------------+--------------+----------+------------------------+---------------+----------------+
     */

    customerDF
      .withColumn("Identity", array($"Name", $"Username"))
      .select("Name", "Username", "Identity")
      .show(5, false)
    /*
    +---------------+----------------+---------------------------------+
    |Name           |Username        |Identity                         |
    +---------------+----------------+---------------------------------+
    |Lindsay Cowan  |valenciajennifer|[Lindsay Cowan, valenciajennifer]|
    |Katherine David|hillrachel      |[Katherine David, hillrachel]    |
    |Leslie Martinez|serranobrian    |[Leslie Martinez, serranobrian]  |
    |Brad Cardenas  |charleshudson   |[Brad Cardenas, charleshudson]   |
    |Natalie Ford   |gregoryharrison |[Natalie Ford, gregoryharrison]  |
    +---------------+----------------+---------------------------------+
     */

    println(s"*************************** агрегирование ***************************")

    customerDF
      .groupBy("Country")
      .count()
      .orderBy("count")
      .show()
    /*
    +--------------------+-----+
    |             Country|count|
    +--------------------+-----+
    |           Singapore|    1|
    |                 RSA|    1|
    |             Iceland|    1|
    |        Saudi Arabia|    1|
    |United Arab Emirates|    1|
    |      Czech Republic|    1|
    |              Brazil|    1|
    |             Lebanon|    1|
    |              Greece|    2|
    |         Unspecified|    2|
    |             Bahrain|    2|
    |              Israel|    4|
    |                 USA|    4|
    |              Poland|    6|
    |              Sweden|    7|
    |              Cyprus|    7|
    |             Denmark|    8|
    |               Japan|    8|
    |           Australia|    8|
    |         Netherlands|    8|
    +--------------------+-----+
     */

    customerDF
      .groupBy($"Country", date_format(col("Birthdate"), "yyyy-MM-dd").alias("bd"))
      .agg(min("CustomerID"), max("CustomerID"))
      .orderBy("Country")
      .show()
    /*
    +---------+----------+---------------+---------------+
    |  Country|        bd|min(CustomerID)|max(CustomerID)|
    +---------+----------+---------------+---------------+
    |Australia|1966-09-17|          12422|          12422|
    |Australia|1967-11-29|          12424|          12424|
    |Australia|1985-12-30|          12386|          12386|
    |Australia|1986-08-28|          12434|          12434|
    |Australia|1987-02-26|          12393|          12393|
    |Australia|1989-08-28|          12415|          12415|
    |Australia|1993-01-31|          12431|          12431|
    |Australia|1993-04-02|          12388|          12388|
    |  Austria|1970-08-06|          12865|          12865|
    |  Austria|1973-07-16|          12818|          12818|
    |  Austria|1973-12-21|          12370|          12370|
    |  Austria|1976-06-13|          12374|          12374|
    |  Austria|1976-09-03|          12373|          12373|
    |  Austria|1977-06-19|          12358|          12358|
    |  Austria|1977-11-30|          12453|          12453|
    |  Austria|1980-10-28|          12360|          12360|
    |  Austria|1982-07-01|          12414|          12414|
    |  Austria|1983-02-24|          12429|          12429|
    |  Austria|1996-03-28|          12817|          12817|
    |  Bahrain|1972-11-10|          12355|          12355|
    +---------+----------+---------------+---------------+
     */

    println(s"*************************** операции над несколькими DataFrame ***************************")

    println(s"*************************** union ***************************")

    println(customerDF.count.getClass) //507L
    println(customerDF.union(customerDF).count()) //1014L
    println(customerDF.union(customerDF).union(customerDF).count()) //1521L

    val countUnion = (1 to 5).map(a => customerDF).reduce((x,y) => x.union(y)).count()
    println(countUnion) //2535L

    println(s"*************************** join ***************************")

    val retailDF = spark.read.format("json").load("data/retail_data.json")
    customerDF.printSchema()
    /*
    root
     |-- Address: string (nullable = true)
     |-- Birthdate: string (nullable = true)
     |-- Country: string (nullable = true)
     |-- CustomerID: string (nullable = true)
     |-- Email: string (nullable = true)
     |-- Name: string (nullable = true)
     |-- Username: string (nullable = true)
     */

    retailDF.printSchema()
    /*
    root
     |-- CustomerID: string (nullable = true)
     |-- Description: string (nullable = true)
     |-- InvoiceDate: string (nullable = true)
     |-- InvoiceNo: string (nullable = true)
     |-- Quantity: string (nullable = true)
     |-- StockCode: string (nullable = true)
     |-- UnitPrice: string (nullable = true)
     */

    println((customerDF.dtypes.map(_._1)).toSet.intersect((retailDF.dtypes.map(_._1)).toSet)) // Set(CustomerID)

    customerDF.join(retailDF, Seq("CustomerID")).show(10)
    /*
    +----------+--------------------+-------------------+--------------+--------------------+---------------+----------------+--------------------+---------------+---------+--------+---------+---------+
    |CustomerID|             Address|          Birthdate|       Country|               Email|           Name|        Username|         Description|    InvoiceDate|InvoiceNo|Quantity|StockCode|UnitPrice|
    +----------+--------------------+-------------------+--------------+--------------------+---------------+----------------+--------------------+---------------+---------+--------+---------+---------+
    |     12346|Unit 1047 Box 408...|1994-02-20 00:46:27|United Kingdom|cooperalexis@hotm...|  Lindsay Cowan|valenciajennifer|MEDIUM CERAMIC TO...|1/18/2011 10:01|   541431|   74215|    23166|     1.04|
    |     12346|Unit 1047 Box 408...|1994-02-20 00:46:27|United Kingdom|cooperalexis@hotm...|  Lindsay Cowan|valenciajennifer|MEDIUM CERAMIC TO...|1/18/2011 10:17|  C541433|  -74215|    23166|     1.04|
    |     12347|55711 Janet Plaza...|1988-06-21 00:15:34|       Iceland|timothy78@hotmail...|Katherine David|      hillrachel|BLACK CANDELABRA ...|12/7/2010 14:57|   537626|      12|    85116|      2.1|
    |     12347|55711 Janet Plaza...|1988-06-21 00:15:34|       Iceland|timothy78@hotmail...|Katherine David|      hillrachel|AIRLINE BAG VINTA...|12/7/2010 14:57|   537626|       4|    22375|     4.25|
    |     12347|55711 Janet Plaza...|1988-06-21 00:15:34|       Iceland|timothy78@hotmail...|Katherine David|      hillrachel|COLOUR GLASS. STA...|12/7/2010 14:57|   537626|      12|    71477|     3.25|
    |     12347|55711 Janet Plaza...|1988-06-21 00:15:34|       Iceland|timothy78@hotmail...|Katherine David|      hillrachel|MINI PAINT SET VI...|12/7/2010 14:57|   537626|      36|    22492|     0.65|
    |     12347|55711 Janet Plaza...|1988-06-21 00:15:34|       Iceland|timothy78@hotmail...|Katherine David|      hillrachel|CLEAR DRAWER KNOB...|12/7/2010 14:57|   537626|      12|    22771|     1.25|
    |     12347|55711 Janet Plaza...|1988-06-21 00:15:34|       Iceland|timothy78@hotmail...|Katherine David|      hillrachel|PINK DRAWER KNOB ...|12/7/2010 14:57|   537626|      12|    22772|     1.25|
    |     12347|55711 Janet Plaza...|1988-06-21 00:15:34|       Iceland|timothy78@hotmail...|Katherine David|      hillrachel|GREEN DRAWER KNOB...|12/7/2010 14:57|   537626|      12|    22773|     1.25|
    |     12347|55711 Janet Plaza...|1988-06-21 00:15:34|       Iceland|timothy78@hotmail...|Katherine David|      hillrachel|RED DRAWER KNOB A...|12/7/2010 14:57|   537626|      12|    22774|     1.25|
    +----------+--------------------+-------------------+--------------+--------------------+---------------+----------------+--------------------+---------------+---------+--------+---------+---------+
     */

    val newRetailDF = retailDF.withColumnRenamed("CustomerID", "rightCustomerID")
    val customerRetailDF = customerDF
      .join(newRetailDF, customerDF("CustomerID") === newRetailDF("rightCustomerID"), "left")

    customerRetailDF.show(10)
    /*
    +--------------------+-------------------+--------------+----------+--------------------+---------------+----------------+---------------+--------------------+---------------+---------+--------+---------+---------+
    |             Address|          Birthdate|       Country|CustomerID|               Email|           Name|        Username|rightCustomerID|         Description|    InvoiceDate|InvoiceNo|Quantity|StockCode|UnitPrice|
    +--------------------+-------------------+--------------+----------+--------------------+---------------+----------------+---------------+--------------------+---------------+---------+--------+---------+---------+
    |Unit 1047 Box 408...|1994-02-20 00:46:27|United Kingdom|     12346|cooperalexis@hotm...|  Lindsay Cowan|valenciajennifer|          12346|MEDIUM CERAMIC TO...|1/18/2011 10:17|  C541433|  -74215|    23166|     1.04|
    |Unit 1047 Box 408...|1994-02-20 00:46:27|United Kingdom|     12346|cooperalexis@hotm...|  Lindsay Cowan|valenciajennifer|          12346|MEDIUM CERAMIC TO...|1/18/2011 10:01|   541431|   74215|    23166|     1.04|
    |55711 Janet Plaza...|1988-06-21 00:15:34|       Iceland|     12347|timothy78@hotmail...|Katherine David|      hillrachel|          12347|MINI PLAYING CARD...|12/7/2011 15:52|   581180|      20|    23508|     0.42|
    |55711 Janet Plaza...|1988-06-21 00:15:34|       Iceland|     12347|timothy78@hotmail...|Katherine David|      hillrachel|          12347|MINI PLAYING CARD...|12/7/2011 15:52|   581180|      20|    23506|     0.42|
    |55711 Janet Plaza...|1988-06-21 00:15:34|       Iceland|     12347|timothy78@hotmail...|Katherine David|      hillrachel|          12347|CHRISTMAS TABLE S...|12/7/2011 15:52|   581180|      16|    23271|     0.83|
    |55711 Janet Plaza...|1988-06-21 00:15:34|       Iceland|     12347|timothy78@hotmail...|Katherine David|      hillrachel|          12347|PINK GOOSE FEATHE...|12/7/2011 15:52|   581180|      12|    21265|     1.95|
    |55711 Janet Plaza...|1988-06-21 00:15:34|       Iceland|     12347|timothy78@hotmail...|Katherine David|      hillrachel|          12347|WOODLAND CHARLOTT...|12/7/2011 15:52|   581180|      10|    20719|     0.85|
    |55711 Janet Plaza...|1988-06-21 00:15:34|       Iceland|     12347|timothy78@hotmail...|Katherine David|      hillrachel|          12347|  RABBIT NIGHT LIGHT|12/7/2011 15:52|   581180|      24|    23084|     1.79|
    |55711 Janet Plaza...|1988-06-21 00:15:34|       Iceland|     12347|timothy78@hotmail...|Katherine David|      hillrachel|          12347|RED TOADSTOOL LED...|12/7/2011 15:52|   581180|      24|    21731|     1.65|
    |55711 Janet Plaza...|1988-06-21 00:15:34|       Iceland|     12347|timothy78@hotmail...|Katherine David|      hillrachel|          12347|PINK NEW BAROQUEC...|12/7/2011 15:52|   581180|      24|   84625A|     0.85|
    +--------------------+-------------------+--------------+----------+--------------------+---------------+----------------+---------------+--------------------+---------------+---------+--------+---------+---------+
     */

    customerRetailDF.select("CustomerID").show(10)
    /*
    +----------+
    |CustomerID|
    +----------+
    |     12346|
    |     12346|
    |     12347|
    |     12347|
    |     12347|
    |     12347|
    |     12347|
    |     12347|
    |     12347|
    |     12347|
    +----------+
     */

    spark.stop()

  }
}















































