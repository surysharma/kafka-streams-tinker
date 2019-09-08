package com.thebigscale.app

import java.util.Properties

import com.thebigscale.common.Types.BYTE_ARRAY
import com.thebigscale.conf.KafkaConf
import com.thebigscale.domain.Person
import com.thebigscale.serdes.KafkaBytesSerializer
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}

object ProducerApp extends App {

  val  props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")

  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "com.thebigscale.serdes.PersonSerializer")

  val producer = new KafkaProducer[String, BYTE_ARRAY](props)

  val person = new Person(4, "user4", 27)
  val personSerializer = new KafkaBytesSerializer[Person]()
  val bytePerson: BYTE_ARRAY = personSerializer.serialize("", person)

  val record = new ProducerRecord[String, BYTE_ARRAY](KafkaConf.INPUT_TOPIC, "key1", bytePerson)
  producer.send(record, new Callback {
    override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
      if (exception != null ) {
        println("Exception thrown by producer: " + exception)
      } else {
        println("Record sent successfully: " + metadata)
      }
    }
  })
  Thread.sleep(5000)
}

