package com.thebigscale.serdesexample

import java.time.Duration
import java.util.Properties

import com.thebigscale.conf.KafkaConf._
import com.thebigscale.domain.Person
import com.thebigscale.serdes._
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{Consumed, KStream, Produced}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

object CustomSerdesExampleApp extends App {

  /*
  1- Create properties
   */
  val properties = new Properties()
  properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "MainApp-consumer-group")
  properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")

  /*
  2- Create Serdes
  */
  implicit val personSerde: Serde[Person] = Serdes.serdeFrom[Person](new KafkaBytesSerializer[Person], new KafkaBytesDeserializer[Person])
  implicit val consumed: Consumed[String, Person] = Consumed.`with`(Serdes.String(), personSerde)
  implicit val produced: Produced[String, Person] = Produced.`with`(Serdes.String(), personSerde)

  /*
  3- Create a Streams Builder
   */
  val builder = new StreamsBuilder()

  /*
  4- Create input processor and chain the processors
   */
   val personProcessor: KStream[String, Person] = builder.stream[String, Person](INPUT_TOPIC)

  val upperCasePersonProcessor: KStream[String, Person] = personProcessor.map[String, Person]((k,p) => (k, Person(p.id, p.name.toUpperCase(), p.age)))

  val peekPersonProcessor: KStream[String, Person] = upperCasePersonProcessor.peek((k, p) => println("Key" + k + " Person: " + p))

  /*
  5- Sink the processors to the sink topic
   */
  peekPersonProcessor.to(OUTPUT_TOPIC)


  /*
  5- Start the stream
   */
  val streams: KafkaStreams = new KafkaStreams(builder.build(), properties)
  streams.start()

  /*
  6- Shut down the stream
   */
  sys.addShutdownHook {
    println("Shutting down now...")
    streams.close(Duration.ofMinutes(1))
  }
}
