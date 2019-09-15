package com.thebigscale.common

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}

trait ProducerConf[T] {

  val producer: () => KafkaProducer[String, T] = () => {
    val  props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[com.thebigscale.serdes.KafkaBytesSerializer[T]])

    val producer = new KafkaProducer[String, T](props)
    producer
  }

}
