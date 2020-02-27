package window

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.{ProcessingTimeSessionWindows, SessionWindowTimeGapExtractor, SlidingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
object FlinkSessionWindow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream = env.socketTextStream("192.168.247.200",8888)

    dataStream.flatMap(_.split("\\s+"))
      .map((_,1))
      .keyBy(0) //先分组，再划分窗口
//      .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))//静态活跃间隙，固定时间间隔
      //// 动态活跃间隙，可以定义不同的条件定义不同的时间间隔
      .window(ProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor[(String, Int)] {

      override def extract(element: (String, Int)): Long = {
        if (element._1.contains("a")) {
          5000   //间隔5000毫秒
        } else {
          1000
        }
      }

      }))
      .reduce((t1,t2) => (t1._1, t1._2 + t2._2))
      .print()

    env.execute("session window word count")
  }
}
