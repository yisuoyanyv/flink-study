package com.zjl.api.com.zjl.api.sink_Test

import com.zjl.api.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

import java.util

object ESSinkTest {
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
    //写入es

    //定义HttpHosts
    val httpHosts=new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("localhost",9200))

    //定义一个ESFunction

    dataStream.addSink(new ElasticsearchSink.Builder[SensorReading](httpHosts,new MyEsSinkFunction).build())
    env.execute("sink File")
  }
}

//定义一个EsSinkFunction
class MyEsSinkFunction extends ElasticsearchSinkFunction[SensorReading]{
  override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
    // 提取数据 包装source
    val dataSource =new util.HashMap[String,String]()
    dataSource.put("id",t.id)
    dataSource.put("temp",t.temperature.toString)
    dataSource.put("ts",t.timestamp.toString)

    //创建index request
    val indexRequest=Requests.indexRequest()
      .index("sensor")
      .`type`("temperature")
      .source(dataSource)

    // 利用 requestIndexer 发送http请求
    requestIndexer.add(indexRequest)

    println(t+" saved")
  }
}