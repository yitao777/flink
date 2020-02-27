package state.managedkey


import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
object FlinkListState {
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

        var state:ListState[Long] = _
        override def open(param: Configuration): Unit = {
//          val lsd = new ListStateDescriptor[(Long)]("ListStateCount",classOf[Long])
          val lsd = new ListStateDescriptor[(Long)]("ListStateCount",createTypeInformation[Long])
          state = getRuntimeContext.getListState(lsd)
        }

        override def map(in: (String, Long)): (String, Long) = {
//          state.update(state.value() + in._2)
             val word = in._1
             val currentCount = in._2
             state.add(currentCount)
             var  sum = 0L
             val iter = state.get().iterator()
            while (iter.hasNext){
                sum += iter.next()
            }

            (word, sum)
        }
      }).print()

    env.execute()
  }
}
