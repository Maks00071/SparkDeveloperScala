package ru.bigdata

import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.EncoderFactory
import org.apache.avro.reflect.AvroSchema

import java.io.ByteArrayOutputStream

object AvroToByte {
  def main(args: Array[String]): Unit = {

  }
}

class AvroToByte {

//  val schema = AvroSchema[AvroToByte]
//  val record = new AvroToByte()
//
//
//  def serializeSubmapRecord(record: AvroToByte): Array[Byte] = {
//    val out = new ByteArrayOutputStream()
//    val encoder = EncoderFactory.get.binaryEncoder(out, null)
//    val writer = new GenericDatumWriter[GenericRecord](out)
//    val r = new GenericData.Record(AvroToByte);
//    r.put("my_number", 1);
//    writer.write(r, encoder)
//    encoder.flush
//    out.close
//    out.toByteArray
//  }
}
