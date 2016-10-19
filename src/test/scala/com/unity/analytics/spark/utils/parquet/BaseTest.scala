package com.unity.analytics.spark.utils.parquet

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}


class BaseTest extends FunSuite with BeforeAndAfterAll with BeforeAndAfter {
  private val master = "local[*]"
  private val appName = "example-spark"
  private val conf = new SparkConf()
    .setMaster(master)
    .setAppName(appName)
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.task.maxFailures", "1")


  protected var sc: SparkContext = _
  protected var sqlContext: SQLContext = _

  protected def getSQLContext(): SQLContext = sqlContext


  override def beforeAll() {
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.hostPort")
    sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    sqlContext = new SQLContext(sc)
  }

  override def afterAll() {
    if (sc != null) {
      sc.stop()
      sc = null
      System.clearProperty("spark.driver.port")
      System.clearProperty("spark.hostPort")
    }
  }
}
