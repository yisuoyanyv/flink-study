package com.zjl.api

import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingProcessingTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
object WindowTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //读取数据
    val filePath="F:\\workspace\\flink-study\\src\\main\\resources\\sensor.txt"
//    val inputStream: DataStream[String] = env.readTextFile(filePath)
    val inputStream:DataStream[String]=env.socketTextStream("localhost",7777)
    val dataStream: DataStream[SensorReading] = inputStream.map(line => {
      val arr: Array[String] = line.split(",")
      SensorReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
    })

    //开窗集合操作、
    val aggStream=dataStream
      .keyBy(_.id)
      //窗口分配器
      .timeWindow(Time.seconds(10))  //10秒大小的滚动窗口
//      .window(EventTimeSessionWindows.withGap(Time.seconds(1)))  //会话窗口
//      .window(TumblingEventTimeWindows.of(Time.hours(1),Time.minutes(10))) //带10分钟偏移量的1小时滚动窗口
//      .window(SlidingProcessingTimeWindows.of(Time.hours(1),Time.minutes(10)))  //1小时窗口，10分钟滑动一次
//      .countWindow(10,2) //滑动计数窗口
      //      .minBy("temperature")
      //窗口函数
//      .reduce((curState,newData)=>SensorReading(newData.id,newData.timestamp+1,curState.temperature.max(newData.temperature)))
      .reduce(new MyMaxTemp())


    aggStream.print()

    env.execute("Window  API test")



  }




}


//自定义取窗口最大温度值的聚合函数
class MyMaxTemp() extends ReduceFunction[SensorReading]{
  override def reduce(t: SensorReading, t1: SensorReading): SensorReading = {
    SensorReading(t.id,t1.timestamp+1,t.temperature.max(t1.temperature))
  }
}

//自定义一个求评价温度的聚合函数
class MyAvgTemp() extends AggregateFunction[SensorReading,(String,Double,Int),(String,Double)] {
  override def createAccumulator(): (String,Double, Int) = ("",0.0,0)

  override def add(value: SensorReading, accumulator: (String,Double, Int)): (String,Double, Int) = (value.id,accumulator._2+value.temperature,accumulator._3+1)

  override def getResult(accumulator: (String,Double, Int)): (String, Double) =
    (accumulator._1,accumulator._2/accumulator._3)

  override def merge(a: (String, Double, Int), b: (String, Double, Int)): (String, Double, Int) = (a._1,a._2+b._2,a._3+b._3)

}