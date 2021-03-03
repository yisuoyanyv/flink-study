package com.zjl.api.com.zjl.api.sink_Test

import com.zjl.api.SensorReading
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

import java.sql.{Connection, DriverManager, PreparedStatement}

/*
jdbc 的支持从 flink 2.11开始，官方提供sink支持
 */
object JdbcSinkTest {
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

    //写入 mySql
   dataStream.addSink(new MyJdbcSink())

    env.execute("mysql Sink job")
  }

}

//自定义实现SinkFunction
class MyJdbcSink() extends RichSinkFunction[SensorReading]{
  //定义sql连接、预编译语句
  var conn:Connection =_
  var insertStmt:PreparedStatement=_
  var updateStmt:PreparedStatement=_

  override def open(parameters: Configuration): Unit = {
    //创建连接，并实现预编译语句
    conn=DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test","root","000000")
    insertStmt=conn.prepareStatement("insert into sensor(id,temperature) values (?,?) ")
    updateStmt=conn.prepareStatement("update sensor set temperature=? where id =?  ")


  }

  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    //直接执行更新语句，如果没有更新就插入
    updateStmt.setDouble(1,value.temperature)
    updateStmt.setString(2,value.id)
    updateStmt.execute()
    if(updateStmt.getUpdateCount==0){
      insertStmt.setString(1,value.id)
      insertStmt.setDouble(2,value.temperature)
      insertStmt.execute()
    }

  }

  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()

  }




}