package com.zjl.wc

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, createTypeInformation}

//批处理 word count
object WordCount {
  def main(args: Array[String]): Unit = {
    // 1.创建一个批处理执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    // 2.从文件中读取数据
    val inputPath="F:\\workspace\\flink-study\\src\\main\\resources\\hello.txt"

    val inputDataSet: DataSet[String] = env.readTextFile(inputPath)

    // 3.空格分词，然后map成计数的二元组进行聚合
    val resultDataSet:DataSet[(String,Int)]=inputDataSet
      .flatMap(_.split(" ")) //空格分割，打散成word
      .map((_,1))//map成 （word,1)
      .groupBy(0)  //以第一个元素进行分组
      .sum(1)  //对元组中第二个元素求和

    //4.打印输出
    resultDataSet.print()
  }
}
