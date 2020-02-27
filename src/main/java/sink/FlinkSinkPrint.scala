package sink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
object FlinkSinkPrint {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream = env.socketTextStream("192.168.247.200",8888)

    dataStream.flatMap(_.split("\\s+"))
        .map((_,1))
        .keyBy(0)
        .sum(1)
        .print()
    env.execute()

  }
}
