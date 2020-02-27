package sink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig

/**
  * 只写出value
  */
object FlinkSinKafka {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream = environment.socketTextStream("192.168.247.200",8888)

    val resultStream = dataStream.flatMap(_.split("\\s+"))
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    val prop = new Properties()
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hive1:9092")
    prop.put(ProducerConfig.ACKS_CONFIG, "all")
    prop.put(ProducerConfig.RETRIES_CONFIG, "3")
    prop.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "2000")

    val producer = new FlinkKafkaProducer[String]("st3", new SimpleStringSchema(),prop)
    resultStream.map(t=>t._1 + "->" + t._2)
        .addSink(producer)
    environment.execute()
  }
}
