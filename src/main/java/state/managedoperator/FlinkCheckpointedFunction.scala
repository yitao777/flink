package state.managedoperator

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable.ListBuffer
object FlinkCheckpointedFunction {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val dataStream = env.socketTextStream("192.168.247.200",8888)

    val resultStream = dataStream.flatMap(_.split("\\s+"))
      .map((_, 1))
      .keyBy(0)
      .sum(1)
    resultStream
        .addSink(new BufferingSink(3))
        .setParallelism(1)  //上述两步不能写反否则无预期结果

    env.execute()
  }

  class BufferingSink(threshold :Int) extends RichSinkFunction[(String, Int)] with CheckpointedFunction {
    //状态数据
    var checkpointedState:ListState[(String, Int)] = _
    //已缓存的待写出元素
    val bufferedElements = ListBuffer[(String, Int)]()
    //写出，传给下一个算子
    override def invoke(value: (String, Int), context: SinkFunction.Context[_]) = {
        bufferedElements += value
      if (bufferedElements.size == threshold) {
         for (element <- bufferedElements) {
           println(element)
         }
        bufferedElements.clear()
      }
    }

    //checkpoint时调用
    override def snapshotState(functionSnapshotContext: FunctionSnapshotContext): Unit = {
      checkpointedState.clear() //清空当前/旧的状态
      bufferedElements.foreach(element => {  //遍历、更新状态
        checkpointedState.add(element)
      })
    }

    //初始化或状态恢复时调用
    override def initializeState(context: FunctionInitializationContext): Unit = {
        val lsd =  new ListStateDescriptor[(String, Int)](
            "buffered-elements",classOf[(String, Int)])
        //初始化
        val checkpointedState = context.getOperatorStateStore.getListState(lsd)
        if(context.isRestored){ //状态恢复时
          //2.11不能用foreach 只能用for循环和iterator迭代器
          val iter = checkpointedState.get().iterator()
          while(iter.hasNext) {
            val element = iter.next()
            bufferedElements += element
          }
        }
    }


  }
}
