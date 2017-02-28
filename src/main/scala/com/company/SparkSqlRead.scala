package com.company

import org.apache.spark.sql.SparkSession

object SparkSqlRead {


  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark SQL read example")
      .getOrCreate()

    readParquet(spark)

//    readAvro(spark)

    System.in.read()
  }

  private def readAvro(spark: SparkSession) = {
    val brandsDF = spark.read.format("com.databricks.spark.avro").load("/Users/yang.yang/Downloads/brands.avro")
    brandsDF.printSchema()
    brandsDF.createOrReplaceTempView("brands")
    spark.sql("SELECT brand_name FROM brands WHERE brand_code BETWEEN 3200 AND 3300").show()
  }

  private def readParquet(spark: SparkSession) = {
    val brandsDF = spark.read.format("parquet").load("/Users/yang.yang/Downloads/brands.parquet")
    brandsDF.printSchema()
    brandsDF.createOrReplaceTempView("brands")
    spark.sql("SELECT brand_name FROM brands WHERE brand_code BETWEEN 3200 AND 3300").show()


  }
}
