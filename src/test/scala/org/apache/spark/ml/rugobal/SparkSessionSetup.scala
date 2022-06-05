package org.apache.spark.ml.rugobal

import com.rugobal.SparkConfUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkSessionSetup {

  import SparkSessionSetup._

  def withSparkContext(testMethod: SparkSession => Any) {
    val sparkSession = SparkSession.builder.config(testConf()).getOrCreate()
    try {
      testMethod(sparkSession)
    }
    finally sparkSession.stop()
  }
}

object SparkSessionSetup {
  def testConf() = {
    val conf = new SparkConf().setMaster("local").setAppName("Spark test")
    conf.set("spark.testing.memory", "512000000") // 512 MB
    SparkConfUtils.setProperties(conf)
    conf
  }

  def deleteFile(spark: SparkSession, location: String): Unit = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val outPutPath = new Path(location)
    if (fs.exists(outPutPath))
      fs.delete(outPutPath, true)
  }
}