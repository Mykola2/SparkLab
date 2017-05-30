package org.training.fpm

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}

/**
  * Created on 4/12/2017.
  */
class CSVTest extends FunSuite with BeforeAndAfter{

  before{

  }

  test("testCount") {
    val sparkSession = SparkSession.builder.
      master("local")
      .appName("csv task_test")
      .getOrCreate();
    val df = sparkSession.read.option("header", true).csv("Books.csv");
    assert(CSVHandler.getCount(df) ==  10)
  }
}
