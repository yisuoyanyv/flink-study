package com.zjl.api.com.zjl.api.sink_Test

import com.zjl.api.SensorReading
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
object RedisSinkTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //    env.setParallelism(1)
    //读取数据
    val filePath="F:\\workspace\\flink-study\\src\\main\\resources\\sensor.txt"
    val inputStream: DataStream[String] = env.readTextFile(filePath)


    // 基本转换

    val dataStream: DataStream[SensorReading] = inputStream.map(line => {
      val arr: Array[String] = line.split(",")
      SensorReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
    }
    )

    //写入Redis

    //定义jedis连接
    val config= new FlinkJedisPoolConfig.Builder()
      .setHost("hadoop102")
      .setPort(6379)
      .build()

    dataStream.addSink(new RedisSink[SensorReading](config,new MyRedisMapper()))

    env.execute("Redis Sink")
  }
}

//定义一个写入redis的Mapper类
class MyRedisMapper() extends RedisMapper[SensorReading]{
  //写入redis的命令，保存成 Hash表 hset 表名 field value
  override def getCommandDescription: RedisCommandDescription =
     new RedisCommandDescription(RedisCommand.HSET,"sensor")

  override def getKeyFromData(t: SensorReading): String = t.id

  override def getValueFromData(t: SensorReading): String = t.temperature.toString
}