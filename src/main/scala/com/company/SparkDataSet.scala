package com.company

import org.apache.spark.sql.SparkSession

object SparkDataSet {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark SQL basic example")
      .getOrCreate()

    import spark.implicits._
//    val advertiserDF = spark.read
//      .format("jdbc")
//      .option("url", "jdbc:mysql://localhost:3306/ptv_working?useCompression=true&rewriteBatchedStatements=true")
//      .option("dbtable", "mp_advertisers")
//      .option("user", "root")
//      .load()

    val bra = readParquet(spark).as[Brand]

    bra.filter(_.brand_code==3139).show()
  }

  private def readParquet(spark: SparkSession) = {
    val brandsDF = spark.read.format("parquet").load("/Users/yang.yang/Downloads/brands.parquet")
    brandsDF.printSchema()
    brandsDF.createOrReplaceTempView("brands")
    brandsDF
  }
}
