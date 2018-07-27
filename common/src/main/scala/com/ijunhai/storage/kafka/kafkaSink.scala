package com.ijunhai.storage.kafka

import java.util
import java.util.Properties
import kafka.common.KafkaException
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}

class KafkaSink(createProducer: () => KafkaProducer[String, String]) extends Serializable {
  lazy val producer = createProducer()

  def send(topic: String, value: String): util.concurrent.Future[RecordMetadata] = {
    producer.send(new ProducerRecord(topic, value))
  }

  def syncSend(topic: String, value: String): Boolean = {
    var offset = -1L
    while (offset < 0) {
      try {
        offset = producer.send(new ProducerRecord(topic, value)).get().offset()
      } catch {
        case ex: KafkaException =>
      }
    }
    true
  }
}

object KafkaSink extends Serializable {
  def apply(brokers: String): KafkaSink = {
    val f = () => {
      val props = new Properties()
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, if (brokers == null || brokers == "") "ukafka-iezd30-kafka1:9092,ukafka-iezd30-kafka2:9092,ukafka-iezd30-kafka3:9092" else brokers)
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
      val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)
      sys.addShutdownHook {
        producer.close()
      }
      producer
    }
    new KafkaSink(f)
  }
}