package state.managedkey


import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state._
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

/**
  * manager keyed state
  * manager： 使用flink提供状态管理数据类型和数据结构
  * keyed：KeyedStream状态管理操作
  */
object WordCountWithKeyedState {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val dataStream = env.socketTextStream("localhost", 9999)

    dataStream
      .flatMap(_.split("\\s"))
      .map((_, 1))
      .keyBy(t2 => t2._1)

    /*  //--------------------------------------------MapState<K,V>-----------------------------------------------
      .map(new RichMapFunction[(String, Int), (String, Int)] {
      // 状态类型
      var state: MapState[String, Int] = _

      override def open(parameters: Configuration): Unit = {
        val msd = new MapStateDescriptor[String, Int]("wordcount", classOf[String], classOf[Int])
        // ttl state 有效期的Keyed State
        val ttlConfig = StateTtlConfig
          .newBuilder(Time.seconds(5))
          .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
          //.setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
          .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
          // 全量快照时进行过期状态数据的清理
          .cleanupFullSnapshot()
          .build()
        msd.enableTimeToLive(ttlConfig)

        state = getRuntimeContext.getMapState[String, Int](msd)
      }

      override def map(value: (String, Int)): (String, Int) = {
        // 状态数据更新
        val word = value._1
        val currentCount = value._2
        // 状态更新
        if (state.contains(word)) {
          val historyCount = state.get(word)
          state.put(word, historyCount + currentCount)
        } else {
          state.put(word, currentCount)
        }
        (word, state.get(word))
      }
    })
      .print()*/
    //--------------------------------------------MapState<K,V>-----------------------------------------------

    //--------------------------------------------ReducingState<T>-----------------------------------------------
    /*
    .map(new RichMapFunction[(String, Int), (String, Int)] {
      // 状态类型 状态数据以列表的形式维护管理
      var state: ReducingState[Int] = _

      override def open(parameters: Configuration): Unit = {
        val rsd = new ReducingStateDescriptor[Int](
          "reducingValue",
          new ReduceFunction[Int] {
            override def reduce(value1: Int, value2: Int): Int = value1 + value2
          },
          classOf[Int])
        state = getRuntimeContext.getReducingState[Int](rsd)
      }

      override def map(value: (String, Int)): (String, Int) = {
        // 状态数据更新
        val word = value._1
        val currentCount = value._2
        // 状态更新
        state.add(currentCount)
        // 如何获取最新的状态数据
        (word, state.get())
      }
    })
    .print()
     */
    //--------------------------------------------ReducingState<T>-----------------------------------------------

    //--------------------------------------------ListState[T]-----------------------------------------------
    .map(new RichMapFunction[(String, Int), (String, Int)] {
      // 状态类型 状态数据以列表的形式维护管理
      var state: ListState[Int] = _

      override def open(parameters: Configuration): Unit = {
        val lsd = new ListStateDescriptor[Int]("valueList", classOf[Int])
        state = getRuntimeContext.getListState[Int](lsd)
      }

      override def map(value: (String, Int)): (String, Int) = {
        // 状态数据更新
        val word = value._1
        val currentCount = value._2
        // 状态更新
        state.add(currentCount)
        // 如何获取最新的状态数据
        val iter = state.get().iterator()
        var sum = 0
        while (iter.hasNext) {
          sum += iter.next()
        }
        (word, sum)
      }
    })
    .print()
    //--------------------------------------------ListState[T]-----------------------------------------------

    //--------------------------------------------ValueState[T]-----------------------------------------------
    // KeyedStream 实现有状态计算
    // 有状态计算，相同的单词次数累加
    // In  Out
    /*
    .map(new RichMapFunction[(String, Int), (String, Int)] {
      // 状态类型
      var state: ValueState[Int] = _

      /**
       * 初始化获取历史状态
       *
       * @param parameters
       */
      override def open(parameters: Configuration): Unit = {
        val vsd = new ValueStateDescriptor[Int]("valueCount", classOf[Int], 0)
        // 恢复历史状态数据
        state = getRuntimeContext().getState[Int](vsd)
      }

      /**
       * 状态更新
       *
       * @param value
       * @return
       */
      override def map(value: (String, Int)): (String, Int) = {
        val word = value._1
        val currentCount = value._2
        // 历史状态 + 当前次数 = 最新状态
        state.update(state.value() + currentCount)
        (word, state.value())
      }
    })
    .print()
     */
    //-------------------------------------------------------------------------------------------


    env.execute()
  }
}
