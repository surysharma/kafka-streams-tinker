package com.thebigscale.serdes

import java.io.{ByteArrayInputStream, ObjectInputStream}
import java.util

import org.apache.kafka.common.serialization.Deserializer

class KafkaBytesDeserializer[T] extends Deserializer[T]{
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()

  override def deserialize(topic: String, data: Array[Byte]): T = {
    val objIn = new ObjectInputStream(new ByteArrayInputStream(data))
    val obj = objIn.readObject().asInstanceOf[T]
    objIn.close
    obj
  }

  override def close(): Unit = ()
}
