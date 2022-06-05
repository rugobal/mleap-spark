package com.rugobal

import java.net.URI
import SparkConfUtils.getClass
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.rugobal.feature.ColumnPercentageEstimator
import org.apache.spark.sql.SparkSession

object SavingModelHdfsExample extends App {
  @transient private lazy val log = Logger.getLogger(getClass.getName)
  val sparkConf = new SparkConf().setAppName("wirecheck-ds-example")
  SparkConfUtils.setProperties(sparkConf)
  val spark = SparkSession.builder.config(sparkConf).getOrCreate()
  val df = spark.createDataFrame(Seq("SPAIN", "SPAIN", "FRANCE", "ITALY").map(Tuple1.apply)).toDF("country")
  val estimator = new ColumnPercentageEstimator()
    .setInputCol("country")
    .setOutputCol("country_perc")
    .setMissingValue(-1.0)
  val model = estimator.fit(df)

  import ml.combust.mleap.spark.SparkSupport._

  val hdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  val homeDir = hdfs.getHomeDirectory
  log.info("HDFS HOME DIR = " + homeDir)
  log.info(s"PATH OF MODEL: ${homeDir}/hdfs-saved-from-spark-job.bundle.zip")
  implicit val sbc: SparkBundleContext = SparkBundleContext.defaultContext.withDataset(model.transform(df))
  model.writeBundle.save(new URI(s"${homeDir}/hdfs-saved-from-spark-job.bundle.zip"))
}