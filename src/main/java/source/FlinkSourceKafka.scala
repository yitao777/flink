package source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.scala._
import org.apache.kafka.clients.consumer.ConsumerConfig
object FlinkSourceKafka {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    val prop = new Properties()
    prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hive1:9092")
    prop.put(ConsumerConfig.GROUP_ID_CONFIG,"g1")
    //只获取value
    val dataStream = environment
                      .addSource(new FlinkKafkaConsumer[String]("flink",
                      new SimpleStringSchema(),prop))

    dataStream
      .print()

    environment.execute("kafka source")
  }
}
