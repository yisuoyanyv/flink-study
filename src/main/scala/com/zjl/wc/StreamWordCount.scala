package com.zjl.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}

/**
 * 流处理WordCount
 */
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    //1.创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //env.disableOperatorChaining()//不合并操作链
    //    env.setParallelism(4)
    //2.接收socke文本流   通过nc -lk 7777 命令，在win10的ubuntu上启动一个服务
    val paramTool:ParameterTool=ParameterTool.fromArgs(args)
    val host:String =paramTool.get("host")
    val port:Int=paramTool.getInt("port")
    val inputDataStream: DataStream[String] = env.socketTextStream(host, port)
    
    //3.对DataStream进行转换处理
    val resultDataStream:DataStream[(String,Int)]=inputDataStream
      .flatMap(_.split(" ")).slotSharingGroup("1")
      .map((_,1)).disableChaining()
      .keyBy(0)
      .sum(1)

    // 4.打印输出
    resultDataStream.print()

    // 5.启动执行任务
    env.execute("stream word count")
    
  }

}
