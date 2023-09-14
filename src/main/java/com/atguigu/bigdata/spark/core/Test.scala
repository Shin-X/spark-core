package com.atguigu.bigdata.spark.core


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.{max, row_number, sum}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}



object Test {

    def main(args: Array[String]): Unit = {

        System.setProperty("hadoop.home.dir", "D:\\work\\hadoop-common-2.2.0-bin-master")

        val spark = SparkSession.builder()
          .appName("RDD to DataFrame")
          .master("local")
          .getOrCreate()
        import spark.implicits._
       // spark.sparkContext.setCheckpointDir("")

        // RDD <=> DataFrame
        val rdd = spark.sparkContext.makeRDD(List((1, "zhangsan", 30), (2, "lisi", 40),(2, "zhangsan", 50)))

        val df: DataFrame = rdd.toDF("id", "name", "age")




    /*    val rddd = spark.sparkContext.parallelize(Seq(
            (1, "John", 25),
            (2, "Jane", 30),
            (3, "Bob", 35),
            (2, "John", 55),
            (3, "Jane", 40),
            (4, "Bob", 75)
        ))
        val schema = StructType(Seq(
            StructField("id", IntegerType, nullable = false),
            StructField("name", StringType, nullable = false),
            StructField("age", IntegerType, nullable = false)
        ))

        val df = spark.createDataFrame(rdd.map(row => Row.fromTuple(row)), schema)
*/


        df.groupBy("id","name").max("age").show(8)
        val windowSpec: WindowSpec = Window.orderBy("id").partitionBy("name")

       // df.withColumn("row_number", row_number().over(windowSpec)).show(8)

        df.withColumn("total_amount", sum("age").over(windowSpec)).show(10)


       // val value: RDD[((Int, String), Int)] =

        rdd.map(x => ((x._1, x._2), x._3)).groupBy(_._1).mapValues(_.toList.sortBy(-_._2).take(10)).collect().foreach(println)



    }
}
