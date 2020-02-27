package state

import org.apache.flink.api.common.functions.{AggregateFunction, RichMapFunction}
import org.apache.flink.api.common.state.{AggregatingState, AggregatingStateDescriptor, MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object FlinkAggregatingState {


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

        var state:AggregatingState[Long, Long] = _

        override def open(parameters: Configuration)={
             // 泛型1：In  泛型2: 累加器 泛型3：Out
             val asd =  new AggregatingStateDescriptor[Long, Long, Long]("AggregatingStateCount",
               new AggregateFunction[Long,Long,Long] {
                 override def createAccumulator(): Long = 0L //初始值

                 override def add(in: Long, acc: Long): Long = in + acc

                 override def getResult(acc: Long): Long = acc

                 override def merge(acc: Long, acc1: Long): Long = acc + acc1
               },
               classOf[Long])

          state = getRuntimeContext.getAggregatingState(asd)
        }

        override def map(in: (String, Long)): (String, Long) = {
          val word  = in._1
          val currentCount = in._2
          state.add(currentCount)
          (word, state.get())
        }
      }).print()

    env.execute()

  }
}
