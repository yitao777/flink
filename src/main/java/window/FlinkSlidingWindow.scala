package window

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.{SlidingProcessingTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._

object FlinkSlidingWindow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream = env.socketTextStream("192.168.247.200",8888)

    dataStream.flatMap(_.split("\\s+"))
      .map((_,1))
      .keyBy(0) //先分组，再划分窗口
      .window(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(5)))
      .reduce((t1,t2) => (t1._1, t1._2 + t2._2))
      .print()

    env.execute("sliding window word count")
  }
}
