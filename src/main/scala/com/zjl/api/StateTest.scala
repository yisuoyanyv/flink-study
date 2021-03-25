package com.zjl.api

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

object StateTest {
  def main(args: Array[String]): Unit = {
    new MyStateOperator
  }
}


class MyStateOperator extends RichMapFunction[SensorReading,String]{
  //var myState:ValueState[Int]=_
  //不加lazy报错
   lazy val myState:ValueState[Int]=getRuntimeContext.getState[Int](new ValueStateDescriptor[Int]("myInt",classOf[Int]))

  override def open(parameters: Configuration): Unit = {
    //myState=getRuntimeContext.getState[Int](new ValueStateDescriptor[Int]("myInt",classOf[Int]))
  }

  override def map(value: SensorReading): String = {
    myState.value()
    myState.update(10)
    ""
  }
}