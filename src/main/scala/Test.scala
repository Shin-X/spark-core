import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Test {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\work\\hadoop-common-2.2.0-bin-master")
    val spark: SparkSession = SparkSession.builder().master("local").appName("test").getOrCreate()


    val rdd2 = spark.sparkContext.parallelize(Seq(
      ("1点", "Son", 2),
      ("2点", "Jane", 3),
      ("3点", "Bob", 4),
      ("3点", "Bob", 5),
      ("3点", "cob", 6),
      ("3点", "cob", 6),
      ("3点", "cob", 6),
      ("3点", "cob", 17),
      ("3点", "dob", 18),
      ("5点", "dob", 18),
      ("3点", "dob", 18),
      ("2点", "Son", 18),
      ("3点", "Jane", 18),
      ("4点", "Bob", 18)
    ))


    val rdd = spark.sparkContext.parallelize(Seq(
      ("1点", 1),
      ("2点", 3),
      ("3点", 4),
      ("3点", 5),
      ("3点", 6),
      ("3点", 6),
      ("3点", 6),
      ("3点", 17),
      ("3点", 18),
      ("5点", 18),
      ("3点", 18),
      ("2点", 18),
      ("3点", 18),
      ("4点", 18)
    ))
    // x._1 小时 x._2 url


   //rdd2.map(x => ((x._1, x._2), 1)).groupByKey().mapValues(x => x.sum).map(x => (x._1._1, x._1._2, x._2)).groupBy(x => x._1).mapValues(values => values.toList.sorted.takeRight(10)).foreach(println)

    //rdd2.map(x => ((x._1, x._2), 1)).groupByKey().mapValues(x=>x.sum).map(x=>(x._1._1,x._1._2,x._2)).groupBy(x=>x._1).mapValues(values => values.toList.sortBy(x=>x._3).takeRight(2)).foreach(println)

    //val value: RDD[((String, String), Int)] = rdd2.map(x => (x._1, x._2, 1)).groupBy(x => (x._1, x._2)).mapValues(x => x.map(x => x._3)).mapValues(x => x.sum)

    val rdd1: RDD[(String, Int)] = rdd.map(x => (x._1, 1)).groupByKey().mapValues(x => x.reduce(_ + _))

    rdd2.map(x => ((x._1, x._2), 1)).groupByKey().mapValues(x => x.sum).map(x => (x._1._1, x._1._2, x._2)).foreach(println)

    rdd1.foreach(println)
    println("__________")
    val value: RDD[(String, Iterable[(String, Int)])] = rdd1.map(x=>(x._1+"_flag",x._2)).groupBy(_._1.split("_")(1))
    value.foreach(println)

     value.flatMap { case (key, values) =>
      values.toList.sortBy(x => x._1)
        .sliding(2).map(window =>
        (window.map(_._1.split("_")(0)), window.map(_._2).sum)
      )

    }.foreach(println)

    //values.toList.sliding(2).map(window => (key, window.map(_._2).sum))


    //val groupedRdd = rdd2.map(x => ((x._1, x._2), 1)).groupBy(_._1) // 按照第一个元素进行分组
    // 统计每个组的大小和求和
    // val value: RDD[((Int, String), (Int, Int))] = groupedRdd.mapValues(values => (values.size, values.map(_._2).sum))


  }
}
