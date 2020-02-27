package state.managedkey

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}

object FlinkMapState {
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
        var state:MapState[String,Long] = _

        override def  open(parameters: Configuration)={
          //        val msd = new MapStateDescriptor[String, Long]("MapStateCount",
          //          createTypeInformation[String],
          //          createTypeInformation[Long],
          //        )
          val msd = new MapStateDescriptor[String,Long]("MapStateCount",
            classOf[String], classOf[Long])
          state = getRuntimeContext.getMapState(msd)

        }

        override def map(in: (String, Long)): (String, Long) = {
          val word  = in._1
          val currentCount = in._2
          state.put(word, state.get(word) + currentCount)

          (word,state.get(word))
        }
      }).print()

    env.execute()

  }
}
