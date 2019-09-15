package com.thebigscale.zmart.consumers

import java.time.Duration
import java.util.Properties

import com.thebigscale.conf.KafkaConf
import com.thebigscale.serdes.{KafkaBytesDeserializer, KafkaBytesSerializer}
import com.thebigscale.zmart.domain.Purchase
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.kstream.Printed
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{Consumed, KStream, Produced}

object ShoppingApp extends App {

  /*
  1- Create properties
 */
  val properties = new Properties()
  properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "ShoppingApp-consumer-group")
  properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")

  implicit val personSerde: Serde[Purchase] = Serdes.serdeFrom[Purchase](new KafkaBytesSerializer[Purchase], new KafkaBytesDeserializer[Purchase])
  implicit val consumed: Consumed[String, Purchase] = Consumed `with` (Serdes.String, personSerde)
  implicit val produced: Produced[String, Purchase] = Produced `with` (Serdes.String, personSerde)


  val builder = new StreamsBuilder()

  val purchaseStream: KStream[String, Purchase] = builder.stream(KafkaConf.PURCHASE_TOPIC)
  val maskedStream: KStream[String, Purchase] = purchaseStream.mapValues(purchase => Purchase(purchase.id, purchase.cardNo + "xxxxxx"))

  maskedStream.print(Printed.toSysOut[String, Purchase].withLabel("Patterns: "))

  maskedStream.to(KafkaConf.PATTERNS_TOPIC)

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
