package ru.bigdata

import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import scala.collection.immutable.HashSet
import org.apache.hadoop.fs._
import org.apache.hadoop.conf.Configuration
import org.apache.avro.file._
import org.apache.avro.generic._
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import scala.collection.immutable._
import java.time.LocalTime
import java.time.LocalDate



object LoadAvro {
  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val loadAvro = new LoadAvro(0)
    //loadAvro
  }
}


class LoadAvro(increment: Int) extends getVariables {
   // variables
  val loadPart = getValue("loadPart")

  val srcSchema = getValue("srcSchema")
  val srcTable = getValue("srcTable")

  val stgSchema = getValue("stgSchema")
  val stgTable = getValue("stgTable")

  val tgtSchema = getValue("tgtSchema")
  val tgtTable = getValue("tgtTable")

  val stgDir = getValue("stgDir")
  val tgtDir = getValue("tgtDir")

  val conf = new SparkConf().setAppName("LoadAvro").setMaster("local")
  val sc = new SparkContext(conf)

  val pathToFile = new Path("data/sampleAvro.avro")




    if (increment == 1) {
      println("****************************** Loading  delta ******************************")
      println()
      println(s"****************************** Spark Version ${sc.version} ******************************")
      println()
      println(s"srcSchema: ${srcSchema}; srcTable: ${srcTable}")
      println(s"stgSchema: ${stgSchema}; stgTable: ${stgTable}")
      println(s"tgtSchema: ${tgtSchema}; tgtTable: ${tgtTable} ")
      println(s"stgDir: ${stgDir}; tgtDir: ${tgtDir}")

    } else {
      println("****************************** Loading archive ******************************")
      println()
      println(s"****************************** Spark Version ${sc.version} ******************************")
      println()
      println(s"loadPart: ${loadPart}")
      println(s"srcSchema: ${srcSchema}; srcTable: ${srcTable}")
      println(s"stgSchema: ${stgSchema}; stgTable: ${stgTable}")
      println(s"tgtSchema: ${tgtSchema}; tgtTable: ${tgtTable} ")
      println(s"stgDir: ${stgDir}; tgtDir: ${tgtDir}")

      //println(fullAvroTypeName(pathToFile))

    }

  /**
   * Функция обработки
   */
  val getStringFromAvroRow = (row: GenericRecord, name: String) => {
    Option(row.get(name)).map(_.toString()).orNull
  }

  /**
   * Функция обработки
   */
  val getMapFromAvroRow = (row: GenericRecord, fieldsSeq: Seq[String]) => {
    var fieldsMap = Map.empty[String, String]
    fieldsSeq.foreach(p => fieldsMap += p.toString -> Option(row.get(p.toString)).map(_.toString()).orNull)
    fieldsMap
  }

  // << 1 >>

  /**
   * Функция принимает массив байт (Array[Byte]) и возвращает type of avro-schema
   *
   * @param data сериализованный набор данных (массив байт)
   */
  def fullAvroTypeName(data: Array[Byte]): Unit = {
    if (data == null) {
      return None
    } else {
      val is = new ByteArrayInputStream(data)
      var avroFileStream: DataFileStream[GenericRecord] = null

      try {
        // открываем потоковый доступ к данным. Конструктор DataFileStream<D>(InputStream in, DatumReader<D> reader)
        // public class GenericDatumReader<D> extends Object implements DatumReader<D> - класс для десериализации
        avroFileStream = new DataFileStream[GenericRecord](is, new GenericDatumReader[GenericRecord]())

        // возвращаем объект org.apache.avro.Schema -> return a schema of
        // (record, enum, array of values, map, union of other schemas, fixed sized binary object,
        // unicode string, sequence of bytes, 32-bit signed int, 64-bit signed long, 32-bit IEEE single-float,
        // 64-bit IEEE double-float, boolean) or null
        Some(avroFileStream.getSchema().getFullName())
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        if (avroFileStream != null) {
          avroFileStream.close()
        }
      }
    }
  }

  // << 2 >>

  /**
   * Функция принимает массив байт (Array[Byte]) и возвращает нименование схемы avro в фомате String
   *
   * @param data сериализованный набор данных (массив байт)
   */
  def parseAvroSchema(data: Array[Byte]): Unit = {
    if (data == null) {
      return None
    } else {
      val is = new ByteArrayInputStream(data)
      var avroFileStream: DataFileStream[GenericRecord] = null

      try {
        // открываем потоковый доступ к данным. Конструктор DataFileStream<D>(InputStream in, DatumReader<D> reader)
        // public class GenericDatumReader<D> extends Object implements DatumReader<D> - класс для десериализации
        avroFileStream = new DataFileStream[GenericRecord](is, new GenericDatumReader[GenericRecord]())
        // возвращаем объект org.apache.avro.Schema -> return String
        Some(avroFileStream.getSchema().toString())
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        if (avroFileStream != null) {
          avroFileStream.close()
        }
      }
    }
  }

  // << 3 >>

  /**
   * Функция принимает массив байт (Array[Byte]) и возвращает объект List[String]
   *
   * @param data сериализованный набор данных (массив байт)
   */
  def parseAvroFields(data: Array[Byte]): Unit = {
    if (data == null) {
      return None
    } else {
      val is = new ByteArrayInputStream(data)
      var avroFileStream: DataFileStream[GenericRecord] = null

      try {
        // открываем потоковый доступ к данным. Конструктор DataFileStream<D>(InputStream in, DatumReader<D> reader)
        // public class GenericDatumReader<D> extends Object implements DatumReader<D> - класс для десериализации
        avroFileStream = new DataFileStream[GenericRecord](is, new GenericDatumReader[GenericRecord]())
        // возвращаем объект org.apache.avro.Schema -> List<Schema.Fields> -> return String (список строк List(...,....,...))
        val listOfFields = avroFileStream.getSchema().getFields().toString()
        Some(listOfFields.split(", ").map(_.split("\\s+")(0)).toList) // return -> List[String]
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        if (avroFileStream != null) {
          avroFileStream.close()
        }
      }
    }
  }

  // << 4 >>

  /**
   * Функция принимает массив байт (Array[Byte]),
   * наименование поля для парсинга и возвращает List[String]
   *
   * @param data      сериализованный набор данных (массив байт)
   * @param fieldName поле для парсинга
   */
  def getCustomAvroField(data: Array[Byte], fieldName: String): Unit = {
    if (data == null) {
      return None
    } else {
      val is = new ByteArrayInputStream(data)
      var avroFileStream: DataFileStream[GenericRecord] = null

      try {
        // открываем потоковый доступ к данным. Конструктор DataFileStream<D>(InputStream in, DatumReader<D> reader)
        // public class GenericDatumReader<D> extends Object implements DatumReader<D> - класс для десериализации
        avroFileStream = new DataFileStream[GenericRecord](is, new GenericDatumReader[GenericRecord]())

        // получаем из DataFileStream iterator и обрабатываем каждый элемент объекта fieldName
        // функцией (getStringFromAvroRow() -> return String) -> return List[String]
        Some(scala.collection.JavaConverters.asScalaIterator(avroFileStream.iterator()).map(row => {
          Map(fieldName -> getStringFromAvroRow(row, fieldName))
        }).toList)
      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        if (avroFileStream != null) {
          avroFileStream.close()
        }
      }
    }
  }

  // << 5 >>

  /**
   * Функция принимает массив байт (Array[Byte]) и возвращает
   * объект Map[String, String]
   *
   * @param data
   */
  def getCustomAvroMaps(data: Array[Byte]): Unit = {
    if (data == null) {
      return None
    } else {
      val is = new ByteArrayInputStream(data)
      var avroFileStream: DataFileStream[GenericRecord] = null

      try {
        // открываем потоковый доступ к данным. Конструктор DataFileStream<D>(InputStream in, DatumReader<D> reader)
        // public class GenericDatumReader<D> extends Object implements DatumReader<D> - класс для десериализации
        avroFileStream = new DataFileStream[GenericRecord](is, new GenericDatumReader[GenericRecord]())
        val listOfFields = avroFileStream.getSchema().getFields().toString.drop(1).dropRight(1)
        val fieldsSeq = listOfFields.split(", ").map(_.split("\\s+")(0)).toList

        Some(scala.collection.JavaConverters.asScalaIterator(avroFileStream.iterator).map(row => {
          getMapFromAvroRow(row, fieldsSeq)
        }).toList(0))

      } catch {
        case e: Exception => e.printStackTrace()
      } finally {
        if (avroFileStream != null) {
          avroFileStream.close()
        }
      }
    }
  }
}


















































