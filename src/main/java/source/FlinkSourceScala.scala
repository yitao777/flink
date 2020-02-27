package source

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.LongValueSequenceIterator
object FlinkSourceScala {
   def main(args: Array[String]): Unit = {
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      val dataStream = getStream5(env)

//      dataStream.flatMap(_.split("\\s+"))
      ////        .map((_,1))
      ////        .keyBy(t=>t._1)   //keyBy(0)
      ////        .sum(1)
      ////        .print()
      dataStream.print()
      env.execute("word count")
   }

   // scala list
   def  getStream(environment: StreamExecutionEnvironment)={
      environment.fromCollection(List("hello spark","hello flink"))
   }
   // scala seq
   def getStream2(environment: StreamExecutionEnvironment)={
      environment.fromCollection(Seq("hello spark","hello flink"))
   }

   // 并行集合
   def getStream3(executionEnvironment: StreamExecutionEnvironment)={
      executionEnvironment.fromParallelCollection(new LongValueSequenceIterator(1,10))
   }

   // 数值Range  1 to 20
   def getStream4(executionEnvironment: StreamExecutionEnvironment)={
      executionEnvironment.generateSequence(1,20)
   }

   // pojo
   def getStream5(executionEnvironment: StreamExecutionEnvironment)={
      executionEnvironment.fromElements(User(1,"zs"),User(2,"lisi"))
   }

   case class User(id:Int, name:String)
}
