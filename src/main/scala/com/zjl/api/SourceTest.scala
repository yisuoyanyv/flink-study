package com.zjl.api

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import java.util.Properties
import scala.util.Random

/**
 * 创建输入数据的样例类
 */
case class SensorReading(id:String, timestamp:Long, temperature:Double)

object SourceTest {
  def main(args: Array[String]): Unit = {
    //创建流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 1.从集合读取数据
    val sensorList=List(
      SensorReading("sensor_1",1547718199,35.8),
      SensorReading("sensor_2",1547718201,15.4),
      SensorReading("sensor_3",1547718300,36.8),
      SensorReading("sensor_4",1547718400,35.8),
      SensorReading("sensor_5",1547718201,18.4),
      SensorReading("sensor_6",1547718300,37.8),
      SensorReading("sensor_3",1547718400,25.8),
    )
    val inputStream1:DataStream[SensorReading]=env.fromCollection(sensorList)

//    inputStream1.print("stream1").setParallelism(1)

    val value: DataStream[Any] = env.fromElements(0, 0.99, "hello")


    // 2.从文件读取数据

    val filePath="F:\\workspace\\flink-study\\src\\main\\resources\\sensor.txt"
    val inputStream2: DataStream[String] = env.readTextFile(filePath)
//    inputStream2.print("stream2")


    // 3. 从kafka读取数据
    // env.socketTextStream("localhost",7777)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers","hadoop102:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val inputStream3=env.addSource(new FlinkKafkaConsumer011[String]("sensor",new SimpleStringSchema(),properties))
//    inputStream3.print("stream3")



    // 4.自定义source
    val inputStream4=env.addSource(new MySensorSource())

    inputStream4.print("stream4").setParallelism(1)
    //执行
    env.execute("source test")



  }


}


//自定义的SourceFunction
class MySensorSource() extends SourceFunction[SensorReading]{
  // 定义一个标志位，用来指示是否正常生成数据
  var running:Boolean = true
  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    //定义一个随机数发生器
    val rand = new Random()

    //随机初始化10个传感器的温度值，之后再此基础上随机波动
    var curTempList = 1.to(10).map(
      i => ("sensor_" + i, 60 + rand.nextGaussian() * 20)
    )

    //无限循环，生成随机的传感器数据
    while (running) {
      //在之前温度基础上，随机波动一点，改变温度值
      curTempList = curTempList.map(
        curTempTuple => (curTempTuple._1, curTempTuple._2 + rand.nextGaussian())
      )

      //获取当前的时间戳
      val curTs = System.currentTimeMillis()
      //将数据包装成样例类，用sourceContext输出
      curTempList.foreach(
        curTempTuple=>sourceContext.collect(SensorReading(curTempTuple._1,curTs,curTempTuple._2))
      )
      //间隔1s
      Thread.sleep(1000)
    }
  }
  override def cancel(): Unit = running=false
}