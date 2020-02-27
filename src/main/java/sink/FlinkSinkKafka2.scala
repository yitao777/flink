package sink

import java.util.Properties


import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig

/**
  * 写出key 和value
  */
object FlinkSinkKafka2 {
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

//    val producer = new FlinkKafkaProducer[(String,Int)]("st3",
//      new KeyedSerializationSchema[(String,Int)] {
//        override def serializeKey(t: (String, Int)): Array[Byte] = t._1.getBytes()
//
//        override def serializeValue(t: (String, Int)): Array[Byte] = t._2.toString.getBytes()
//
//        override def getTargetTopic(t: (String, Int)): String = "st3"
//      },
//      prop)
//
//    resultStream.addSink(producer)

    resultStream
      .addSink(new FlinkKafkaProducer[(String, Int)](
        "st3",
        new KeyedSerializationSchema[(String, Int)] {
          /**
            * 序列化record key方法
            *
            * @param element
            * @return
            */
          override def serializeKey(element: (String, Int)): Array[Byte] = element._1.getBytes()

          override def serializeValue(element: (String, Int)): Array[Byte] = element._2.toString.getBytes()

          override def getTargetTopic(element: (String, Int)): String = "st3"
        }, prop))

    environment.execute()
  }
}
