package com.zjl.api.com.zjl.api.sink_Test

import com.zjl.api.SensorReading
import org.apache.flink.api.common.serialization.{SerializationSchema, SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

import java.util.Properties

object KafkaDataPipelineTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

    // 从kafka读取数据
    // env.socketTextStream("localhost",7777)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers","hadoop102:9092")
    properties.setProperty("group.id", "sensor")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val inputStream=env.addSource(new FlinkKafkaConsumer011[String]("sensor",new SimpleStringSchema(),properties))

    //基本转换
    val dataStream: DataStream[SensorReading] = inputStream.map(line => {
      val arr = line.split(",")
      SensorReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
    }
    )

    //写入kafka

    //假如写入的类型不为String类型，而是元组类型，则需自定义序列化
//    dataStream.map(data=>(data.id,data.temperature))
//      .addSink(new FlinkKafkaProducer011[(String, Double)]("hadoop102:9092","sinktest",new SerializationSchema[(String, Double)] {
//        override def serialize(t: (String, Double)): Array[Byte] = s"id:${t._1} temp:${t._2}".toArray.map(_.toByte)
//      }))

    dataStream.map(data=>data.toString)
      .addSink(new FlinkKafkaProducer011[String]("hadoop102:9092","sinktest",new SimpleStringSchema()))


    env.execute("kafka pipeline job")


  }
}
