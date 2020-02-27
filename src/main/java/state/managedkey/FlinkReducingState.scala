package state.managedkey

import org.apache.flink.api.common.functions.{ReduceFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ReducingState, ReducingStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
object FlinkReducingState {
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
      .map(new RichMapFunction[(String, Long),(String, Long)] {
        var state:ReducingState[Long] = _

        override def open(parameters: Configuration) = {
           val rsd =  new ReducingStateDescriptor[Long]("ReducingStateCount",
                new ReduceFunction[Long]() {
                  override def reduce(t: Long, t1: Long): Long = t + t1
                },
             classOf[Long])
             state =  getRuntimeContext.getReducingState(rsd)
        }
        override def map(in: (String, Long)): (String, Long) = {
          val word = in._1
          val currentCount = in._2
          state.add(currentCount)
          (word, state.get())
        }
      }).print()


    env.execute()
  }
}
