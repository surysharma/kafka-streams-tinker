package com.thebigscale.serdesexample

import java.time.Duration
import java.util.Properties

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}
import org.apache.kafka.streams.scala.kstream.{Consumed, KStream}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

object StringSerdesExampleApp extends App {

  /*
  1- Create properties
   */
  val properties = new Properties()
  properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "MainApp-consumer-group")
  properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")


  /*
  2- Create serdes object
   */
  val stringSerde: Serde[String]  = Serdes.String

  /*
  3- Create a Streams Builder
   */
  val builder = new StreamsBuilder()

  /*
  4- Create a KStream object from the streams builder.
   */
  val testStream: KStream[String, String] = builder.stream[String, String]("test_input_topic")(Consumed.`with`(stringSerde, stringSerde)) //This need implicitConversions and Serdes.

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
