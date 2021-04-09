package com.zjl.api.tableapi

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors._

object TableApiTest {
  def main(args: Array[String]): Unit = {
    // 1.表执行环境
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
//    val tableEnv=StreamTableEnvironment.create(env)

//    // 1.1老版本的流处理环境
//    val oldStreamSettings=EnvironmentSettings.newInstance()
//      .useOldPlanner()
//      .inStreamingMode()
//      .build()
//    val odlStreamTableEnv=StreamTableEnvironment.create(env,oldStreamSettings)
//
//    // 1.2 老版本批处理环境
//    val batchEnv=ExecutionEnvironment.getExecutionEnvironment
//    val oldBatchTableEnv=BatchTableEnvironment.create(batchEnv)

    // 1.3 blink流处理环境
    val blinkStreamSetting=EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val blinkStreamTableEnv=StreamTableEnvironment.create(env,blinkStreamSetting)

//    // 1.4 blink批处理环境
//    val blinkBatchSettings=EnvironmentSettings.newInstance()
//      .useBlinkPlanner()
//      .inBatchMode()
//      .build()
//    val blinkBatchTableEnv=TableEnvironment.create(blinkBatchSettings)

    // 2.创建表
    // 从文件中读取数据
    val filePath="F:\\workspace\\flink-study\\src\\main\\resources\\sensor.txt"

    blinkStreamTableEnv.connect(
      new FileSystem().path(filePath))//定义到文件系统给的连接
      .withFormat(new Csv())    //定义以csv格式进行数据格式化
      .withSchema(new Schema()
        .field("id",DataTypes.STRING())
        .field("timestamp",DataTypes.BIGINT())
        .field("temperature",DataTypes.DOUBLE())
      )   //定义表结构
      .createTemporaryTable("inputTable")  //创建临时表
    val sensorTable: Table = blinkStreamTableEnv.from("inputTable")
//    sensorTable.toAppendStream[(String,Long,Double)].print(" input")

    //连接到kafka
    blinkStreamTableEnv.connect( new Kafka()
      .version("0.11")
      .topic("sensor")
      .property("zookeeper.conncet","hadoop102:2181")
      .property("bootstrap.servers","hadoop102:9092")
    )
      .withFormat( new Csv())
      .withSchema(new Schema()
          .field("id",DataTypes.STRING())
          .field("timestamp",DataTypes.BIGINT())
          .field("temperature",DataTypes.DOUBLE())
      )
      .createTemporaryTable("kafkaInputTable")


    //3.表的查询
    // 3.1 Table API
    // 3.1.1 简单查询
    val resultTable = sensorTable
      .select('id, 'temperature)
//      .where('id === "sensor_1")

    // 3.1.2 聚合查询
    val aggResultTable =  sensorTable
      .groupBy('id) //按照id分组
      .select('id,'id.count as 'count)


    // 3.2 SQL
    // 3.2.1 简单查询
    val resultSqlTable = blinkStreamTableEnv.sqlQuery(
      """
        |select id,temperature
        |from inputTable
        |where id = 'sensor_1'
        |""".stripMargin
    )
    // 3.2.2 聚合查询
    val aggResultSqlTable = blinkStreamTableEnv.sqlQuery(
      """
        |select id,count(id) as cnt
        |from inputTable
        |group by id
        |
        |""".stripMargin
    )



//    resultTable.toAppendStream[(String,Double)].print("res")
//    resultSqlTable.toAppendStream[(String,Double)].print("sql")
    aggResultTable.printSchema()
    aggResultTable.toRetractStream[(String,Long)].print("res")
    aggResultSqlTable.toRetractStream[(String,Long)].print("sql")


    // 4.输出
    // 输出到文件
    val outputPath="F:\\workspace\\flink-study\\src\\main\\resources\\output.txt"

    blinkStreamTableEnv.connect(
      new FileSystem().path(outputPath))//定义到文件系统给的连接
      .withFormat(new Csv())    //定义以csv格式进行数据格式化
      .withSchema(new Schema()
        .field("id",DataTypes.STRING())
        .field("temperature",DataTypes.DOUBLE())
      )   //定义表结构
      .createTemporaryTable("outputTable")  //创建临时表

    resultTable.insertInto("outputTable")


    env.execute("api test")

  }
}
