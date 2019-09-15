package com.thebigscale.common

import java.util.Properties

import com.thebigscale.conf.KafkaConf
import com.thebigscale.zmart.domain.Purchase
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import org.scalatest.FunSuite

class PurchaseProducer extends FunSuite with ProducerConf[Purchase]{

  test("Send a Purchase message") {

    val producerProps = new Properties()

    val record = new ProducerRecord[String, Purchase](KafkaConf.PURCHASE_TOPIC, "key1", Purchase("456", "43353-434343-554554"))

   producer().send(record, (metadata: RecordMetadata, exception: Exception) => {
     if (exception != null){
       println("Exception occurred while sending the record:" +  exception)
     }else {
       println(s"Record sent successfully, metadata.topic()=${metadata.topic()}, metadata.offset()=${metadata.offset()}, metadata.partition()=${metadata.partition()}")
     }
   })

  }

}
