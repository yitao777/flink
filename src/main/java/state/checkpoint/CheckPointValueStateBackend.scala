package state.checkpoint

import java.util.Properties

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema
import org.apache.kafka.clients.consumer.ConsumerConfig
object CheckPointValueStateBackend {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置状态后端为RocksDB  ,注释掉即可在本地运行,方便测试
    env.setStateBackend(new RocksDBStateBackend("hdfs://192.168.247.200:9000/170/flink3"))

//    激活检查点
    env.enableCheckpointing(1000) //间隔时间
    //配置检查点
    var config = env.getCheckpointConfig
    config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)//CheckpointingMode默认是CheckpointingMode.EXACTLY_ONCE，也可以指定为CheckpointingMode.AT_LEAST_ONCE
    config.setFailOnCheckpointingErrors(false)
    //用于指定checkpoint coordinator上一个checkpoint完成之后最小等多久可以出发另一个checkpoint，当指定这个参数时，maxConcurrentCheckpoints的值为1
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    config.setMaxConcurrentCheckpoints(1)

    //配置检查点定期持久化存储在外部系统中
    //ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION:作业取消时保留外部检查点。请注意，在这种情况下，你必须手动清除取消后的检查点状态
    //ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION: 作业取消时删除外部检查点。检查点状态只有在作业失败时才可用
    config.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    //KafkaSource属性
    val prop = new Properties()
    prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.247.20:9092")

    val dataStream = env.addSource(new FlinkKafkaConsumer[String]("st3",
      new SimpleStringSchema(), prop))
   val resultStream = dataStream.flatMap(_.split("\\s+"))
      .map((_,1))
      .keyBy(0)
      //自定义状态管理,ValueState
      .map(new RichMapFunction[(String,Int), (String, Int)] {
        var state:ValueState[Int] = _

        override def open(parameters: Configuration) = {
          val vsd = new ValueStateDescriptor[Int]("valueState",classOf[Int])
          state = getRuntimeContext.getState(vsd)
        }

        override def map(in: (String, Int)): (String, Int) = {
          val word = in._1
          val currentCount = in._2
          state.update(state.value() + currentCount)
          (word, state.value())
        }
      })
    resultStream.map(t=>t._1 + "->" + t._2)
        .addSink(new FlinkKafkaProducer[String]("flink",new SimpleStringSchema(),prop))
//      .writeAsText("file:///D://result",WriteMode.OVERWRITE)
//        .writeAsText("hdfs://192.168.247.200:9000/result3",WriteMode.OVERWRITE)
//        .print()
    env.execute()
  }
}
