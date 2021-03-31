package com.zjl.api

import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object StateTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStateBackend(new RocksDBStateBackend("",true))

    //容错机制相关配置
    //checkpoint
    env.enableCheckpointing(1000L)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(60000L)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500L) //会覆盖setMaxConcurrentCheckpoints设置，为1
    env.getCheckpointConfig.setPreferCheckpointForRecovery(true)
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(3)

    // 重启策略
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5,10000L))

    //读取数据
    val inputStream: DataStream[String] = env.socketTextStream("localhost", 7777)

    val dataStream: DataStream[SensorReading] = inputStream.map(line => {
      val arr: Array[String] = line.split(",")
      SensorReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
    })

    val warningStream=dataStream
      .keyBy(_.id)  //以id分组
//      .flatMap(new TempChangeWarning(10.0))
      .flatMapWithState[(String,Double,Double),Double](
        {
          case(inputData:SensorReading,None)=>(List.empty,Some(inputData.temperature))
          case (inputData:SensorReading,lastTemp)=>{
            val diff=(inputData.temperature-lastTemp.get).abs
            if(diff>10.0){
              (List((inputData.id,lastTemp.get,inputData.temperature)),Some(inputData.temperature))
            }else{
              (List.empty,Some(inputData.temperature))
            }
          }
        }
      )
    warningStream.print()
    inputStream.print("input:")

    print("start...")
    env.execute("state test job")


//    new MyStateOperator
  }
}

//自定义RichFlatMapFunction,实现温度跳变检测报警功能
class TempChangeWarning(threshold: Double) extends RichFlatMapFunction[SensorReading,(String,Double,Double)]{
  //定义状态，保存上一个温度值
  private var lastTempState:ValueState[Double]=_
  val defaultTemp:Double = -273.15

  //加入一个标识位状态，用来表示是否出现过当前传感器数据
  private var isOccurState:ValueState[Boolean]=_

  override def open(parameters: Configuration): Unit = {
    lastTempState=getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp",classOf[Double],defaultTemp))
    isOccurState=getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-occur",classOf[Boolean]))
  }
  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
    //从状态中获取上次温度值
    val lastTemp=lastTempState.value()

    //跟当前温度做比较，如果大于阈值，输出报警信息
    val diff=(value.temperature-lastTemp).abs
//    if(diff>threshold && lastTemp != defaultTemp){
    if(isOccurState.value()  &&  diff>threshold ){
      out.collect((value.id,lastTemp,value.temperature))
    }

    //更新状态
    lastTempState.update(value.temperature)
    isOccurState.update(true)

  }
}

class MyStateOperator extends RichMapFunction[SensorReading,String]{
  //var myState:ValueState[Int]=_
  //不加lazy报错
   lazy val myState:ValueState[Int]=getRuntimeContext.getState[Int](new ValueStateDescriptor[Int]("myInt",classOf[Int]))

  override def open(parameters: Configuration): Unit = {
    //myState=getRuntimeContext.getState[Int](new ValueStateDescriptor[Int]("myInt",classOf[Int]))
  }

  override def map(value: SensorReading): String = {
    //底层会区分不同的key
    myState.value()
    myState.update(10)
    ""
  }
}