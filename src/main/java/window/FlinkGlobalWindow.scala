package window

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{GlobalWindows, SlidingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger

object FlinkGlobalWindow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream = env.socketTextStream("192.168.247.200",8888)

    dataStream.flatMap(_.split("\\s+"))
      .map((_,1))
      .keyBy(0) //先分组，再划分窗口
      .window(GlobalWindows.create())//先创建全局窗口，再添加触发器
      .trigger(CountTrigger.of(3))//相同的单词出现3次触发计算
      .reduce((t1,t2) => (t1._1, t1._2 + t2._2))
      .print()

    env.execute("global window word count")
  }
}
