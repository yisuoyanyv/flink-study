package com.zjl.api.tableapi.udftest

import com.zjl.api.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

object ScalarFunctionTest {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val tableEnv=StreamTableEnvironment.create(env)


    //读取数据
    //    val inputStream:DataStream[String]= env.socketTextStream("localhost", 7777)

    val inputStream=env.readTextFile("F:\\workspace\\flink-study\\src\\main\\resources\\sensor.txt")
    val dataStream: DataStream[SensorReading] = inputStream.map(line => {
      val arr: Array[String] = line.split(",")
      SensorReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.milliseconds(500)) {
        override def extractTimestamp(t: SensorReading): Long = t.timestamp * 1000L
      })


    //将流转成Table，同时设定时间字段
    val  sensorTable=tableEnv.fromDataStream(dataStream,'id,'temperature as 'temp,'timestamp as 'ts,'pt.proctime)
    // 1. Table API 中使用
    // 创建UDF实例，然后直接使用
    val hashCode = new HashCode(5)
    val resultTable = sensorTable
      .select( 'id,'ts,'temp,hashCode('id))

    resultTable.toAppendStream[Row].print("res")

    // 2. SQL 中使用
    // 在环境中注册UDF
    tableEnv.createTemporaryView("sensor", sensorTable)
    tableEnv.registerFunction("hashCode",hashCode)
    val resultSqlTable = tableEnv.sqlQuery("select id,ts,temp,hashCode(id) from sensor")
    resultSqlTable.toAppendStream[Row].print("sql")

    env.execute("scalar function test job")

  }

}

// 自定义一个标量函数
class HashCode(factor : Int ) extends ScalarFunction {
  def eval(value : String) :Int={
    value.hashCode * factor
  }
}