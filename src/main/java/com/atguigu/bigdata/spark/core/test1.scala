package com.atguigu.bigdata.spark.core

object test1 {


  def main(args: Array[String]): Unit = {
    import org.apache.spark.{SparkConf, SparkContext}
    System.setProperty("hadoop.home.dir", "D:\\work\\hadoop-common-2.2.0-bin-master")

    val conf = new SparkConf().setAppName("SlidingWindowAggregation").setMaster("local")
    val sc = new SparkContext(conf)

    // 创建一个包含要聚合的数据的RDD
    val data = sc.parallelize(Seq(("A", 1), ("A", 2), ("A", 3), ("B", 4), ("B", 5), ("B", 6)))

    // 定义窗口大小和滑动步长
    val windowSize = 3
    val slidingStep = 1

    // 对数据进行分组和聚合
    val result = data.groupBy(_._1)
      .flatMap { case (key, values) =>
        values.toList.sliding(windowSize, slidingStep).map(window => (key, window.map(_._2).sum))
      }

    // 打印结果
    result.foreach(println)
  }
}
