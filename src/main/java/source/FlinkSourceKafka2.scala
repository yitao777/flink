package source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema
object FlinkSourceKafka2 {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    val prop = new Properties()
    prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hive1:9092")
    prop.put(ConsumerConfig.GROUP_ID_CONFIG,"g1")

    val dataStream = environment
      // 假如：获取key value offset partition
      .addSource(new FlinkKafkaConsumer[(String,String,Long,Int)]("flink",
        new KafkaDeserializationSchema[(String,String,Long,Int)] {


          override def isEndOfStream(t: (String, String, Long, Int)): Boolean = false

          override def deserialize(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]]): (String, String, Long, Int) = {
            val key = new String(consumerRecord.key())
            val value = new String(consumerRecord.value())
            val offset = consumerRecord.offset()
            val partition = consumerRecord.partition()
            (key, value, offset, partition)
          }

          override def getProducedType: TypeInformation[(String, String, Long, Int)] = {
            createTypeInformation[(String, String, Long, Int)]
          }
        },prop))

    dataStream
      .print()

    environment.execute("kafka source")
  }
}
