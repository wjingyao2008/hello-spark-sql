package com.SparkSqlReadParquet

import org.apache.spark.sql.SparkSession
/**
 * @author ${user.name}
 */
object App {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {
    println( "Hello World!" )
    println("concat arguments = " + foo(args))
//
    //    val conf = new SparkConf().setAppName("yang").setMaster("local")
    //    val sparkContext = new SparkContext(conf)
    //    val a = sparkContext.textFile("sda")

    val spark = SparkSession
      .builder()
        .master("local")
      .appName("Spark SQL basic example")
      .getOrCreate()
//    val df = spark.read.json("examples/src/main/resources/people.json")

    // Displays the content of the DataFrame to stdout
//    df.show()
    import spark.implicits._
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/ptv_working?useCompression=true&rewriteBatchedStatements=true")
      .option("dbtable", "mp_advertisers")
      .option("user", "root")
//      .option("driver", "com.mysql.jdbc.Driver")
//      .option("password", "password")
      .load()
    jdbcDF.printSchema()
    println(jdbcDF.take(10))


    //test
  }

}
