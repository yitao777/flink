package window.function

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object FlinkReduceFunction {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream = env.socketTextStream("192.168.247.200",8888)

    dataStream.flatMap(_.split("\\s+"))
      .map((_,1))
      .keyBy(0) //先分组，再划分窗口
      .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))

      .reduce(new ReduceFunction[(String,Int)] {
        override def reduce(t1: (String, Int), t2: (String, Int)): (String, Int) = {
          (t1._1, t1._2 + t2._2)
        }
      })
      .print()

    env.execute()
  }
}
