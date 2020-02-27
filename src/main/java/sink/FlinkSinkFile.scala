package sink

import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
object FlinkSinkFile {
   def main(args: Array[String]): Unit = {
      val environment = StreamExecutionEnvironment.getExecutionEnvironment
      val dataStream = environment.socketTextStream("192.168.247.200",8888)

      val resultStream = dataStream.flatMap(_.split("\\s+"))
        .map((_, 1))
        .keyBy(0)
        .sum(1)
      resultStream.writeAsText("hdfs://192.168.247.200:9000/data3")
       //往本地写文件会缓存在内存中，超出才会溢写，csv文件缓存更大，NO_OVERWRITE模式目录不能已存在，OVERWRITE模式才可以
//      resultStream.writeAsText("file:///D://result",WriteMode.OVERWRITE)//默认NO_OVERWRITE
//      resultStream.writeAsCsv("file:///D://result2",WriteMode.NO_OVERWRITE)
//      resultStream.writeAsCsv("file:///D://result",WriteMode.OVERWRITE)
      environment.execute()

   }
}
