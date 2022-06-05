package org.apache.spark.ml.rugobal.classification

import com.rugobal.mleap.MLeapUtils
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.rugobal.SparkSessionSetup
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.{Matchers, WordSpec}

class OneVsRestSpec extends WordSpec with Matchers with SparkSessionSetup {
  "a OneVsRest estimator" should {
    "work as expected after being persisted with spark ml persistence" in withSparkContext { spark =>
      import spark.sqlContext.implicits._
      val countriesMap: Map[String, Int] = Array("spain", "italy", "france").zipWithIndex.toMap
      val df = spark.createDataFrame(Seq(
        (Vectors.dense(0.9), countriesMap("spain")),
        (Vectors.dense(0.9), countriesMap("spain")),
        (Vectors.dense(0.5), countriesMap("france")),
        (Vectors.dense(0.5), countriesMap("france")),
        (Vectors.dense(0.1), countriesMap("italy")),
        (Vectors.dense(0.1), countriesMap("italy"))
      )).toDF("features", "label")
      val rf = new RandomForestClassifier()
        .setFeaturesCol("features")
        .setLabelCol("label")
        .setNumTrees(1)
      val ovr = new OneVsRest()
        .setLabelCol("label")
        .setFeaturesCol("features")
        .setClassifier(rf)
      val model: OneVsRestModel = ovr.fit(df)
      // persist model
      val path = "./one-vs-rest-all-class-probs-test-spec.model"
      model.save(path)
      // load and check results
      val loadedModel = OneVsRestModel.load(path)
      SparkSessionSetup.deleteFile(spark, path)
      val testDf = spark.createDataFrame(Seq(
        Vectors.dense(0.9),
        Vectors.dense(0.5),
        Vectors.dense(0.1)
      ).map(Tuple1.apply)).toDF("features")
      val transformed = loadedModel.transform(testDf)
      transformed.show(false)
      // check the probabilities is a sequence of length = 3
      val row0Probabilities = transformed.head.getAs[Seq[Double]]("probability")
      row0Probabilities should have length 3
    }
    "work as intended after being persisted with MLeap persistence" in withSparkContext { spark =>
      val countriesMap: Map[String, Int] = Array("spain", "italy", "france").zipWithIndex.toMap
      val df = spark.createDataFrame(Seq(
        (Vectors.dense(0.9), countriesMap("spain")),
        (Vectors.dense(0.9), countriesMap("spain")),
        (Vectors.dense(0.5), countriesMap("france")),
        (Vectors.dense(0.5), countriesMap("france")),
        (Vectors.dense(0.1), countriesMap("italy")),
        (Vectors.dense(0.1), countriesMap("italy"))
      )).toDF("features", "label")
      val rf = new RandomForestClassifier()
        .setFeaturesCol("features")
        .setLabelCol("label")
        .setNumTrees(1)
      val ovr = new OneVsRest()
        .setLabelCol("label")
        .setFeaturesCol("features")
        .setClassifier(rf)
      val model: OneVsRestModel = ovr.fit(df)
      // persist model
      val path = "/home/rugobal/Projects/wirecheck-ds/one-vs-rest-all-class-probs-test-spec.model.zip"
      MLeapUtils.saveSparkModel(model, model.transform(df), path)
      // load and check results
      val loadedModel: Transformer = MLeapUtils.loadSparkModel(path)
      val prefix = if (System.getProperty("os.name").toLowerCase().contains("windows")) "c:" else ""
      SparkSessionSetup.deleteFile(spark, s"$prefix$path")
      val testDf = spark.createDataFrame(Seq(
        Vectors.dense(0.9),
        Vectors.dense(0.5),
        Vectors.dense(0.1)
      ).map(Tuple1.apply)).toDF("features")
      val transformed = loadedModel.transform(testDf)
      // check the probabilities is a sequence of length = 3
      val row0Probabilities = transformed.head.getAs[Seq[Double]]("probability")
      row0Probabilities should have length 3
      transformed.show(false)
    }
  }
}