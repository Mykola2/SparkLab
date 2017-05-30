package org.training.fpm

import org.apache.spark.sql.{DataFrame, SparkSession};

object CSVHandler {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "c:\\\\\\winutils\\\\\\");
    val sparkSession = SparkSession.builder.
      master("local")
      .appName("csv task")
      .getOrCreate()
    doTask(sparkSession)


  }

  def doTask(sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    val df = sparkSession.read.option("header", true).csv("Books.csv")
    df.printSchema()
    df.show()
    df.select($"name").show()
    df.filter($"name".contains("Dark")).show()
    df.sort($"publish_date").show()
    df.createTempView("temp")
    val publishers = sparkSession.sql("Select distinct publisher from temp")
    publishers.coalesce(1).write.option("header", "true").csv("output")


  }

  def getCount(dataFrame: DataFrame): Long={
    dataFrame.count()
  }
}
