package org.apache.spark.ml.rugobal.feature

import com.rugobal.mleap.MLeapUtils
import org.apache.spark.ml.rugobal.SparkSessionSetup
import org.apache.spark.sql.{functions => F}
import org.scalatest.{Matchers, WordSpec}

class ColumnPercentageEstimatorSpec extends WordSpec with Matchers with SparkSessionSetup {
  "a spark ColumnPercentageEstimator" should {
    "work as intended" in withSparkContext { spark =>
      val df = spark.createDataFrame(Seq("SPAIN", "SPAIN", "FRANCE", "ITALY").map(Tuple1.apply)).toDF("country")
      val estimator = new ColumnPercentageEstimator()
        .setInputCol("country")
        .setOutputCol("country_perc")
        .setMissingValue(-1.0)
      val model = estimator.fit(df)
      val testDf = spark.createDataFrame(Seq("SPAIN", "FRANCE", "ITALY", "UK").map(Tuple1.apply)).toDF("country")
      val transformed = model.transform(testDf)
      // transformed.show
      val spainPerc = transformed.select(F.col(estimator.getOutputCol)).where(F.col("country") === "SPAIN").head().getDouble(0)
      val francePerc = transformed.select(F.col(estimator.getOutputCol)).where(F.col("country") === "FRANCE").head().getDouble(0)
      val italyPerc = transformed.select(F.col(estimator.getOutputCol)).where(F.col("country") === "ITALY").head().getDouble(0)
      val ukPerc = transformed.select(F.col(estimator.getOutputCol)).where(F.col("country") === "UK").head().getDouble(0)
      assert(spainPerc === 0.5)
      assert(francePerc === 0.25)
      assert(italyPerc === 0.25)
      assert(ukPerc === -1.0)
    }
    "work as intended after being persisted with spark ml persistence" in withSparkContext { spark =>
      val df = spark.createDataFrame(Seq("SPAIN", "SPAIN", "FRANCE", "ITALY").map(Tuple1.apply)).toDF("country")
      val estimator = new ColumnPercentageEstimator()
        .setInputCol("country")
        .setOutputCol("country_perc")
        .setMissingValue(-1.0)
      val model = estimator.fit(df)
      // persist model
      val path = "./col-perc-estimator-test-spec.model"
      model.save(path)
      // load and check results
      val loadedModel = ColumnPercentageModel.load(path)
      SparkSessionSetup.deleteFile(spark, path)
      val testDf = spark.createDataFrame(Seq("SPAIN", "FRANCE", "ITALY", "UK").map(Tuple1.apply)).toDF("country")
      val transformed = loadedModel.transform(testDf)
      transformed.show
      val spainPerc = transformed.select(F.col(estimator.getOutputCol)).where(F.col("country") === "SPAIN").head().getDouble(0)
      val francePerc = transformed.select(F.col(estimator.getOutputCol)).where(F.col("country") === "FRANCE").head().getDouble(0)
      val italyPerc = transformed.select(F.col(estimator.getOutputCol)).where(F.col("country") === "ITALY").head().getDouble(0)
      val ukPerc = transformed.select(F.col(estimator.getOutputCol)).where(F.col("country") === "UK").head().getDouble(0)
      assert(spainPerc === 0.5)
      assert(francePerc === 0.25)
      assert(italyPerc === 0.25)
      assert(ukPerc === -1.0)
    }
    "work as intended after being persisted with MLeap persistence" in withSparkContext { spark =>
      val df = spark.createDataFrame(Seq("SPAIN", "SPAIN", "FRANCE", "ITALY").map(Tuple1.apply)).toDF("country")
      val estimator = new ColumnPercentageEstimator()
        .setInputCol("country")
        .setOutputCol("country_perc")
        .setMissingValue(-1.0)
      val model = estimator.fit(df)
      // persist model
      val path = "/home/rugobal/Projects/wirecheck-ds/col-perc-estimator-test-spec.model.zip"
      MLeapUtils.saveSparkModel(model, model.transform(df), path)
      // load and check results
      val loadedModel = MLeapUtils.loadSparkModel(path)
      val prefix = if (System.getProperty("os.name").toLowerCase().contains("windows")) "c:" else ""
      SparkSessionSetup.deleteFile(spark, s"$prefix$path")
      val testDf = spark.createDataFrame(Seq("SPAIN", "FRANCE", "ITALY", "UK").map(Tuple1.apply)).toDF("country")
      val transformed = loadedModel.transform(testDf)
      transformed.show
      val spainPerc = transformed.select(F.col(estimator.getOutputCol)).where(F.col("country") === "SPAIN").head().getDouble(0)
      val francePerc = transformed.select(F.col(estimator.getOutputCol)).where(F.col("country") === "FRANCE").head().getDouble(0)
      val italyPerc = transformed.select(F.col(estimator.getOutputCol)).where(F.col("country") === "ITALY").head().getDouble(0)
      val ukPerc = transformed.select(F.col(estimator.getOutputCol)).where(F.col("country") === "UK").head().getDouble(0)
      assert(spainPerc === 0.5)
      assert(francePerc === 0.25)
      assert(italyPerc === 0.25)
      assert(ukPerc === -1.0)
    }
  }
}

object ColumnPercentageEstimatorSpec {
  // def deleteLocalFile(path: String) = {
  // val fileTemp = new File(path)
  // if (fileTemp.exists) {
  // fileTemp.delete()
  // }
  // }
}