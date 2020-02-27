package state.managedkey

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
object FlinkValueState {
  def main(args: Array[String]): Unit = {
    //1. flink程序运行的环境  自适应运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //2. 构建数据源对象
    val dataStream = env.socketTextStream("192.168.247.200", 8888)

    //3. 转换处理操作
    dataStream
      .flatMap(_.split("\\s+"))
      .map((_, 1L))
      // 分组
      // 0 --> tuple2 word
      .keyBy(0)
      .map(new RichMapFunction[(String,Long),(String,Long)] {

        var state:ValueState[Long] = _
        override def open(param: Configuration): Unit = {
            val vsd = new ValueStateDescriptor[(Long)]("valueCount",createTypeInformation[Long],0L)
            state = getRuntimeContext.getState[Long](vsd)
        }

        override def map(in: (String, Long)): (String, Long) = {
          state.update(state.value() + in._2)
          (in._1, state.value())
        }
      }).print()

    env.execute()
  }
}
