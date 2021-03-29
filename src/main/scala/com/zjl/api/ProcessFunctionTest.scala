package com.zjl.api

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import java.util

object ProcessFunctionTest {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //读取数据
    val inputStream: DataStream[String] = env.socketTextStream("localhost", 7777)

    val dataStream: DataStream[SensorReading] = inputStream.map(line => {
      val arr: Array[String] = line.split(",")
      SensorReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
    })

    // process function 示例
//    val processedStream: DataStream[Int] = dataStream
//      .keyBy("id")
//      .process(new MyKeyedProcess)
//
//    processedStream.getSideOutput(new OutputTag[Int]("side")).print("side")

    //  需求： 检测10秒钟内温度是否连续上升，如果上升，那么报警
    val warningStream=dataStream
      .keyBy("id")
      .process( new TempIncreaseWarning(10000L))
    warningStream.print("warning:")


    env.execute("process function test")
  }
}
// 自定义Keyed Process Function，实现10秒内温度连续上升报警检测
class TempIncreaseWarning(interval:Long) extends KeyedProcessFunction[Tuple,SensorReading,String]{

  //首先，定义状态
  lazy val lastTempState:ValueState[Double]=getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp",classOf[Double]))

  lazy val curTimerTsState:ValueState[Long]=getRuntimeContext.getState(new ValueStateDescriptor[Long]("current-timer-ts",classOf[Long]))
  override def processElement(i: SensorReading, context: KeyedProcessFunction[Tuple, SensorReading, String]#Context, collector: Collector[String]): Unit = {

    //取出状态
    val lastTemp=lastTempState.value()
    val curTimerTs=curTimerTsState.value()

    lastTempState.update(i.temperature)

    //如果温度上升，并且没有定时器，那么注册一个10秒后的定时器
    if(i.temperature>lastTemp && curTimerTs == 0){
      val ts = context.timerService().currentProcessingTime()+interval
      context.timerService().registerProcessingTimeTimer(ts)
      //更新timerTs状态
      curTimerTsState.update(ts)
    } else if(i.temperature < lastTemp){
      //如果温度下降，那么就删除定时器
      context.timerService().deleteProcessingTimeTimer(curTimerTs)
      //清空状态
      curTimerTsState.clear()
    }

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    //定时器触发，说明10秒内没有温度下降，报警
    out.collect(s"传感器${ctx.getCurrentKey}的温度值已经连续 ${interval/1000}秒上升了")
    //清空定时器时间戳状态
    curTimerTsState.clear()

  }
}


//自定义Keyed Process Function
class MyKeyedProcess extends KeyedProcessFunction[Tuple,SensorReading,Int]{
  var myState:ListState[Int]=_

  override def open(parameters: Configuration): Unit = {

    myState=getRuntimeContext.getListState(new ListStateDescriptor[Int]("my-state",classOf[Int]))
  }
  override def processElement(i: SensorReading, context: KeyedProcessFunction[Tuple, SensorReading, Int]#Context, collector: Collector[Int]): Unit = {

    myState.get()
    myState.add(10)
    myState.update(new util.ArrayList[Int]())

    //侧输出流
    context.output(new OutputTag[Int]("side"),10)
    //获取当前键
    context.getCurrentKey
    //获取时间相关
    context.timestamp()
    context.timerService().currentWatermark()
    context.timerService().registerEventTimeTimer(context.timerService().currentWatermark()+10)
  }

  //定时器触发时的操作定义
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, SensorReading, Int]#OnTimerContext, out: Collector[Int]): Unit = {
    ctx.output(new OutputTag[Int]("side"),15)
    out.collect(1)
    println("timer occur")
  }
}
