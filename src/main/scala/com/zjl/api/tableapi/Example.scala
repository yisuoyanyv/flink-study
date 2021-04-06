package com.zjl.api.tableapi

import com.zjl.api.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._


//TODO 该示例启动报错

/**
 * Caused by: org.apache.flink.table.api.NoMatchingTableFactoryException: Could not find a suitable table factory for 'org.apache.flink.table.delegation.ExecutorFactory' in
the classpath.

Reason: No factory supports the additional filters.

The following properties are requested:
class-name=org.apache.flink.table.executor.StreamExecutorFactory
streaming-mode=true

 */
object Example {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


    //读取数据
    val inputStream:DataStream[String]= env.socketTextStream("localhost", 7777)

    val dataStream: DataStream[SensorReading] = inputStream.map(line => {
      val arr: Array[String] = line.split(",")
      SensorReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
    })

    //基于env创建一个表执行环境
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val tableEnv=StreamTableEnvironment.create(env,settings)

    //基于一条流创建一张表，流中的样例类的字段就对应表的列
    val dataTable: Table = tableEnv.fromDataStream(dataStream)

    //1.调用table API ,进行表的转换操作
    val resultTable: Table = dataTable
      .select("id,temperature")
      .filter("id=='sensor_1'")

    resultTable.printSchema()
    val resultStream: DataStream[(String, Double)] = resultTable.toAppendStream[(String, Double)]
    resultStream.print("res")

    //2.直接写sql,得到转换结果
    tableEnv.createTemporaryView("dataTable",dataTable)
    val sql="select id,temperature from dataTable where id='sensor_1'"
    val resultSqlTable: Table = tableEnv.sqlQuery(sql)
    val resultSqlStream: DataStream[(String, Double)] = resultSqlTable.toAppendStream[(String, Double)]
    resultSqlStream.print("Sql")



    env.execute("table api")
  }

}
