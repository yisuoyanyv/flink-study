package com.zjl.api.com.zjl.api.sink_Test

import com.zjl.api.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object FileSink {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(1)
    //读取数据
    val filePath="F:\\workspace\\flink-study\\src\\main\\resources\\sensor.txt"
    val inputStream: DataStream[String] = env.readTextFile(filePath)


    // 1.基本转换

    val dataStream: DataStream[SensorReading] = inputStream.map(line => {
      val arr: Array[String] = line.split(",")
      SensorReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
    }
    )

    //写入文件
   // dataStream.writeAsCsv("F:\\workspace\\flink-study\\src\\main\\resources\\out.txt")
    dataStream.addSink(StreamingFileSink.forRowFormat(
      //路径改为hdfs路径，即可输出到hdfs
      new Path("F:\\workspace\\flink-study\\src\\main\\resources\\out.txt"),
      new SimpleStringEncoder[SensorReading]("UTF-8")
    ).build())

    env.execute("sink File")
  }
}
