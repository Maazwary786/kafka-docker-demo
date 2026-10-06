package com.shipment

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties
import scala.util.Random

object ShipmentProducer {
  case class Shipment(id: String, location: String, status: String, latitude: Double, longitude: Double)

  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    val topic = "shipment-events"
    val random = new Random()
    val locations = Seq("Delhi", "Jaipur", "Ahmedabad", "Mumbai", "Pune", "Hyderabad", "Bengaluru")
    val statuses = Seq("PICKED_UP", "IN_TRANSIT", "OUT_FOR_DELIVERY", "DELIVERED")

    println(s"Starting shipment producer -> $topic")

    try {
      while (true) {
        val shipmentId = f"SHP-${1000 + random.nextInt(9000)}"
        val location = locations(random.nextInt(locations.length))
        val status = statuses(random.nextInt(statuses.length))
        val lat = 18.0 + random.nextDouble() * 12.0
        val lon = 72.0 + random.nextDouble() * 12.0
        val timestamp = System.currentTimeMillis()
        val json =
          s"""{"shipment_id":"$shipmentId","location":"$location","status":"$status","latitude":$lat,"longitude":$lon,"event_time":$timestamp}"""

        producer.send(new ProducerRecord[String, String](topic, shipmentId, json))
        println(json)
        Thread.sleep(2000)
      }
    } finally {
      producer.close()
    }
  }
}
