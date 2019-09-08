package com.thebigscale.serdesexample

import java.time.Duration
import java.util.Properties

import com.thebigscale.conf.KafkaConf._
import com.thebigscale.domain.Person
import com.thebigscale.serdes._
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{Consumed, Produced}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

object CustomSerdesExampleApp extends App {

  /*ø
  1- Create properties
   */
  val properties = new Properties()
  properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "MainApp-consumer-group")
  properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")

  /*
  2- Create a Streams Builder
   */
  val builder = new StreamsBuilder()
  val personSerde: Serde[Person] = Serdes.serdeFrom[Person](new KafkaBytesSerializer[Person], new KafkaBytesDeserializer[Person])
    builder
    .stream[String, Person](INPUT_TOPIC)(Consumed.`with`(Serdes.String(), personSerde))
    .map[String, Person]((k,p) => (k, Person(p.id, p.name.toUpperCase(), p.age)))
    .peek((k, p) => println("Key" + k + " Person: " + p))
    .to(OUTPUT_TOPIC)(Produced.`with`(Serdes.String(), personSerde))


  val streams: KafkaStreams = new KafkaStreams(builder.build(), properties)
  streams.start()

  sys.addShutdownHook {
    println("Shutting down now...")
    streams.close(Duration.ofMinutes(1))
  }
}
