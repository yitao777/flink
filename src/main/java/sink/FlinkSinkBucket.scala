package sink

import java.time.ZoneId

import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.fs.bucketing.{BucketingSink, DateTimeBucketer}
object FlinkSinkBucket {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream = environment.socketTextStream("192.168.247.200",8888)

    val resultStream = dataStream.flatMap(_.split("\\s+"))
      .map((_, 1))
      .keyBy(0)
      .sum(1)



       val bucket = new BucketingSink[(String,Int)]("file:///D://data")
       bucket.setBucketer(
         new DateTimeBucketer[(String, Int)]("yyyy-MM-dd-HH:mm",ZoneId.of("Asia/Shanghai"))
       )
       bucket.setBatchSize(1024 * 10) //单位：字节  超过10K字节就分桶
      bucket.setBatchRolloverInterval(10 * 1000) // 10s 间隔超过10s就分桶。 两个条件满足其一即可
    resultStream.addSink(bucket)
    environment.execute()
  }
}
