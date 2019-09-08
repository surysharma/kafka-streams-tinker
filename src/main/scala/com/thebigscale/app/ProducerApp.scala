package com.thebigscale.app

import java.util.Properties

import com.thebigscale.conf.KafkaConf
import com.thebigscale.domain.Person
import org.apache.kafka.clients.producer._

object ProducerApp extends App {

  val  props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[com.thebigscale.serdes.KafkaBytesSerializer[Person]])

  val producer = new KafkaProducer[String, Person](props)

  val person = new Person(4, "user4", 27)

  val record = new ProducerRecord[String, Person](KafkaConf.INPUT_TOPIC, "key1", person)
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

