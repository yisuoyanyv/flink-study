package com.zjl.api

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
//      .timeWindow(Time.seconds(10))  //10秒大小的滚动窗口
//      .window(EventTimeSessionWindows.withGap(Time.seconds(1)))  //会话窗口
//      .window(TumblingEventTimeWindows.of(Time.hours(1),Time.minutes(10))) //带10分钟偏移量的1小时滚动窗口
//      .window(SlidingProcessingTimeWindows.of(Time.hours(1),Time.minutes(10)))  //1小时窗口，10分钟滑动一次
      .countWindow(10,2) //滑动计数窗口
      .minBy("temperature")


    aggStream.print()

    env.execute("Window  API test")



  }

}
