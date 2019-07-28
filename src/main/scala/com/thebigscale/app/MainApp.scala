package com.thebigscale.app

import java.time.Duration
import java.util.Properties

import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

object MainApp extends App {

  val properties = new Properties()
  properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "MainApp-consumer-group")
  properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")

  val builder = new StreamsBuilder()

  val testStream: KStream[String, String] = builder.stream[String, String]("test_input_topic") //This need implicitConversions and Serdes.

  testStream
    .mapValues(value => value.toUpperCase)
    .peek((key, value) => println(s"Key=$key, value=$value ..."))
    .to("test_output_topic")


  val streams: KafkaStreams = new KafkaStreams(builder.build(), properties)
  streams.start()

  sys.addShutdownHook {
    streams.close(Duration.ofMinutes(1))
  }

}
