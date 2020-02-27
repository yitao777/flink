package state.managedoperator

import java.util.Collections

import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object FlinkListCheckpointed {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val dataStream = env.addSource(new CounterSource())

    dataStream.print()

    env.execute()
  }


  class CounterSource extends RichParallelSourceFunction[scala.Long]
      with ListCheckpointed[java.lang.Long] {

    @volatile
    private var isRunning = true

    private var counter = 0L

    //写出
    override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
      val lock = ctx.getCheckpointLock

      while (isRunning) { //死循环
        Thread.sleep(1000) //每隔一秒执行一次
        // output and state update are atomic
        lock.synchronized({
          ctx.collect(counter) //下发给下一个算子，counter+1
          counter += 1
          println(counter)
        })
      }
    }

    //不中断
    override def cancel(): Unit = isRunning = false

    //恢复
    override def restoreState(state: java.util.List[java.lang.Long]): Unit ={
          val iter = state.iterator()
          while (iter.hasNext) {
            counter = iter.next()
          }
    }

    //更新
    override def snapshotState(checkpointId: Long, timestamp: Long): java.util.List[java.lang.Long] =
      Collections.singletonList(counter)

  }
}
