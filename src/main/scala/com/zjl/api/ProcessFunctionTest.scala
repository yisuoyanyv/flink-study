package com.zjl.api

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
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

    val processedStream: DataStream[Int] = dataStream
      .keyBy("id")
      .process(new MyKeyedProcess)

    processedStream.getSideOutput(new OutputTag[Int]("side")).print("side")


    env.execute("process function test")
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
