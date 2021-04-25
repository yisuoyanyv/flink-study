package com.zjl.api.tableapi

import com.zjl.api.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{Over, Tumble}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object TimeAndWindowTest {
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
    // 1.处理时间
//    val  sensorTable=tableEnv.fromDataStream(dataStream,'id,'temperature as 'temp,'timestamp as 'ts,'pt.proctime)
    // 2.事件时间
    val sensorTable=tableEnv.fromDataStream(dataStream,'id,'timestamp.rowtime as 'ts,'temperature as 'temp)
//    sensorTable.printSchema()
//    sensorTable.toAppendStream[Row].print()

    //3. 窗口聚合
    // 3.1 分组窗口
    // 3.1.1 Table API
    val resultTable=sensorTable
      .window(Tumble over 10.seconds on 'ts as 'tw)
      .groupBy('id,'tw)
      .select('id,'id.count as 'cnt,'tw.end)
    //转换成流打印输出
    resultTable.toAppendStream[Row].print()

    // 3.1.2 SQL实现
    tableEnv.createTemporaryView("sensor",sensorTable)
    val resultSqlTable=tableEnv.sqlQuery(
      """
        |select id,count(id) as cnt,tumble_end(ts,interval '10' second)
        |from sensor
        |group by id,tumble(ts,interval '10' second)
        |""".stripMargin
    )

    resultSqlTable.toAppendStream[Row].print("sql")


    // 3.2 开窗函数
    // 3.2.1 Table  API
    val overResultTable=sensorTable
      .window(Over partitionBy 'id orderBy 'ts preceding 2.rows as 'ow)
      .select('id,'ts,'id.count over 'ow,'temp.avg over 'ow)

    overResultTable.toAppendStream[Row].print("over")
    //3.2.2 SQL
    val overResultSQLTable=tableEnv
      .sqlQuery(
        """
          |select id,ts,count(id) over ow,avg(temp) over ow
          |from sensor
          |window ow as (
          |  partition by id
          |  order by ts
          |  rows between 2 preceding and current row
          |)
          |""".stripMargin)
    overResultSQLTable.toAppendStream[Row].print("over-sql")
    env.execute("time and window test job")

//    val sinkDDL:String=
//      """
//        |cerate table dataTable(
//        |  id varchar(20) not null,
//        |  ts bigint,
//        |  temperature double,
//        |  pt AS PROCTIME()
//        |) with(
//        |  'connector.type' = 'filesystem',
//        |  'connector.path' = '/sensor.txt',
//        |  'format.type' = 'csv'
//        |)
//        |""".stripMargin
//    tableEnv.sqlUpdate(sinkDDL)

//      val sinkDDL:String=
//        """
//          |cerate table dataTable(
//          |  id varchar(20) not null,
//          |  ts bigint,
//          |  temperature double,
//          |  rt AS TO_TIMESTAMP( FROM_UNIXTIME(ts)),
//          |  watermark for rt as rt - interval '1' second
//          |
//          |) with(
//          |  'connector.type' = 'filesystem',
//          |  'connector.path' = '/sensor.txt',
//          |  'format.type' = 'csv'
//          |)
//          |""".stripMargin
//      tableEnv.sqlUpdate(sinkDDL)


  }
}
