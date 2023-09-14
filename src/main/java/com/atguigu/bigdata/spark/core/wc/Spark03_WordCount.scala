package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, RowFactory}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark03_WordCount {
    def main(args: Array[String]): Unit = {
        System.setProperty("hadoop.home.dir", "D:\\work\\hadoop-common-2.2.0-bin-master")

        val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparConf)

        //wordcount91011(sc)

      /*  val arr: Array[(String, Int, String)] = Array(("aa", 12, "./home"), ("bb", 2, "./home"), ("bb", 2, "./home"))

        val row: Row = Row.fromSeq(arr)
        val row2: Row = RowFactory.create(arr)
        println(row)
        println(row2)

        val tuple =((20201022,5060180989186180L,"[12, 15)"),288556)
        val tuple1: ((Int, Long, String), (Int, Long, String)) = ((20201022, 5060180989186180L, "name"), (20201022, 5060180989186180L, "name2"))

        val array: Array[Any] = tuple1.productIterator.toArray

        array.foreach(println)

        val rdd: RDD[(String, Int, String)] = sc.parallelize(arr)
                rdd.collect.foreach(println)
*/
        ws(sc)




        sc.stop()

    }

    // groupBy
    def wordcount1(sc : SparkContext): Unit = {

        val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))



        val words: RDD[String] = rdd.flatMap(_.split(" "))
        //指定key
        val group: RDD[(String, Iterable[String])] = words.groupBy(x=>x)
        //mapValues 只对value做处理
        val wordCount: RDD[(String, Int)] = group.mapValues(iter=>iter.size)
    }

    // groupByKey
    def wordcount2(sc : SparkContext): Unit = {
        val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words = rdd.flatMap(_.split(" "))
        val wordOne = words.map((_,1))
        //(key,value)
        val group: RDD[(String, Iterable[Int])] = wordOne.groupByKey()
        //只对 value 做处理
        val wordCount: RDD[(String, Int)] = group.mapValues(iter=>iter.size)
    }

    // reduceByKey
    def wordcount3(sc : SparkContext): Unit = {
        val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words = rdd.flatMap(_.split(" "))
        val wordOne = words.map((_,1))
        val wordCount: RDD[(String, Int)] = wordOne.reduceByKey(_+_)

        // reduceByKey((x, y) => x + y)
    }

    // aggregateByKey
    def wordcount4(sc : SparkContext): Unit = {
        val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words = rdd.flatMap(_.split(" "))
        val wordOne = words.map((_,1))
        val wordCount: RDD[(String, Int)] = wordOne.aggregateByKey(0)(_+_, _+_)
    }

    def ws(sc:SparkContext):Unit={
        val rdd1: RDD[(String, Int)] = sc.parallelize(List(
            "spark", "hadoop", "hive", "spark",
            "spark", "flink", "hive", "spark",
            "kafka", "kafka", "kafka", "kafka",
            "hadoop", "flink", "hive", "flink"
        ),5).map((_, 1))
        rdd1.foreach(println)

        rdd1.aggregateByKey(0)(math.max,_+_)
          .mapPartitionsWithIndex((index,iter)=>{
              iter.map((index,_))
          }).foreach(println)
    }

    // foldByKey
    def wordcount5(sc : SparkContext): Unit = {
        val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words = rdd.flatMap(_.split(" "))
        val wordOne = words.map((_,1))
        val wordCount: RDD[(String, Int)] = wordOne.foldByKey(0)(_+_)
    }

    // combineByKey
    def wordcount6(sc : SparkContext): Unit = {
        val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words = rdd.flatMap(_.split(" "))
        val wordOne = words.map((_,1))
        val wordCount: RDD[(String, Int)] = wordOne.combineByKey(
            v=>v,
            (x:Int, y) => x + y,
            (x:Int, y:Int) => x + y
        )
    }

    // countByKey
    def wordcount7(sc : SparkContext): Unit = {
        val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words = rdd.flatMap(_.split(" "))
        val wordOne = words.map((_,1))
        val wordCount: collection.Map[String, Long] = wordOne.countByKey()
    }

    // countByValue
    def wordcount8(sc : SparkContext): Unit = {
        val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words: RDD[String] = rdd.flatMap(_.split(" "))


        val wordCount: collection.Map[String, Long] = words.countByValue()
        wordCount.foreach(println)
    }

    // reduce, aggregate, fold
    def wordcount91011(sc : SparkContext): Unit = {
        val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
        val words = rdd.flatMap(_.split(" "))

        // 【（word, count）,(word, count)】
        // word => Map[(word,1)]
        val mapWord = words.map(
            word => {
                mutable.Map[String, Long]((word,1))
            }
        )

       val wordCount = mapWord.reduce(
            (map1, map2) => {
                map2.foreach{
                    case (word, count) => {
                        val newCount = map1.getOrElse(word, 0L) + count
                        map1.update(word, newCount)
                    }
                }
                map1
            }
        )

        println(wordCount)
    }

}
