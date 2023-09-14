
package com.ku.test

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable

object TestAddField0 {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\work\\hadoop-common-2.2.0-bin-master")
    val spark = SparkSession
      .builder()
      .appName("test")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._
    val sourceFile: DataFrame = Seq(
      (1, "24_男_上海市"),
      (2, "33_女_北京市")
    ).toDF("id", "message")

    sourceFile.show()

    val fieldsConf = new mutable.HashMap[String, DataType]()
    fieldsConf += (("age", IntegerType))
    fieldsConf += (("gender", StringType))
    fieldsConf += (("address", StringType))

    val dataSF: RDD[Row] = sourceFile.rdd.map(
      row => {

        val message = row.getAs[String]("message")
        val buffer = Row.unapplySeq(row).get.toBuffer

        message.split("_").foreach(
          dataVal => {
            buffer.append("new_" + dataVal)
          }
        )

        var schemaNew: StructType = row.schema
        fieldsConf.foreach(conf => {
          schemaNew = schemaNew.add(conf._1, conf._2)
        })

        // 使用Row的子类GenericRowWithSchema创建新的Row
        val newSchema = new GenericRowWithSchema(buffer.toArray, schemaNew).asInstanceOf[Row]
        newSchema
      })

    dataSF.foreach(row => println(row))
  }
}
