package com.SparkSqlReadParquet

import org.apache.spark.sql.{SaveMode, SparkSession}
object SparkSql {


  def main(args : Array[String]) {



    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark SQL basic example")
      .getOrCreate()
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/ptv_working?useCompression=true&rewriteBatchedStatements=true")
//      .option("dbtable", "mp_advertisers")
      .option("dbtable", "mp_brands")
      .option("user", "root")
      //      .option("driver", "com.mysql.jdbc.Driver")
      //      .option("password", "password")
      .load()

    import spark.implicits._

    jdbcDF.printSchema()

//    val count = jdbcDF.count()

//    println(s"allCount = ${count}")
//
//    jdbcDF.groupBy("adv_parent_desc").count().show()
//
//    println(jdbcDF.take(10).mkString(","))

//    println(jdbcDF.select("adv_subsid_desc").take(5).mkString(","))

//      jdbcDF.filter($"adv_parent_desc".contains("MICROSOFT")).show()

    jdbcDF.createOrReplaceTempView("brands");

    val sqlDf=spark.sql("SELECT * FROM brands")
    println(s"allCount = ${sqlDf.count()}")
    println("write to parquet file")
//    sqlDf.write.format("parquet").mode(SaveMode.Overwrite).save("/Users/yang.yang/Downloads/brands.parquet")
//    sqlDf.write.format("parquet").mode(SaveMode.Overwrite).save("/Users/yang.yang/Downloads/brands.parquet")
    sqlDf.write.format("com.databricks.spark.avro").mode(SaveMode.Overwrite).save("/Users/yang.yang/Downloads/brands.avro")
    println("input anything to terminate")
    System.in.read();

  }
}
