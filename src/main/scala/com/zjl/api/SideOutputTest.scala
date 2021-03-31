package com.zjl.api

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SideOutputTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //读取数据
    val inputStream: DataStream[String] = env.socketTextStream("localhost", 8888)

    val dataStream: DataStream[SensorReading] = inputStream.map(line => {
      val arr: Array[String] = line.split(",")
      SensorReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
    })


    //用侧输出流实现分流操作，定义主流为高温流
    val highTempStream=dataStream
      .process(new SplitStreamOperation(30.0))
    highTempStream.print("high")
    val lowTempStream=highTempStream.getSideOutput(new OutputTag[(String,Double,Long)]("low"))
    lowTempStream.print("low")

    env.execute("side Out put test")
  }
}


//自定义 ProcessFunction ,实现高低温的分流操作
class SplitStreamOperation(threshold: Double) extends ProcessFunction[SensorReading,SensorReading]{
  override def processElement(i: SensorReading, context: ProcessFunction[SensorReading, SensorReading]#Context, collector: Collector[SensorReading]): Unit = {
    if(i.temperature>threshold){
      // 如果大于阈值，直接输出到主流（高温流）
      collector.collect(i)

    }else{
      //如果小于等于，输出到侧输出流（低温流）
      context.output(new OutputTag[(String,Double,Long)]("low"),(i.id,i.temperature,i.timestamp))
    }

  }
}