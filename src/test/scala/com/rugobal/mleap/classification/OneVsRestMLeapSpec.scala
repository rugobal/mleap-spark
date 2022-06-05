package com.rugobal.mleap.classification

import com.rugobal.PathUtils

import java.io.File
import com.rugobal.mleap.MLeapUtils
import ml.combust.mleap.core.types.{NodeShape, ScalarType, StructField, TensorType}
import ml.combust.mleap.runtime.frame.Transformer
import ml.combust.mleap.tensor.Tensor
import org.apache.spark.ml.rugobal.SparkSessionSetup
import org.apache.spark.ml.rugobal.classification.{OneVsRest => OneVsRestSpark}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

class OneVsRestMLeapSpec extends WordSpec with Matchers with BeforeAndAfterAll {
  val modelPath = PathUtils.currentPath() + "/one-vs-rest-all-class-probs-mleap-test-spec.model.zip"

  // setup
  override def beforeAll(): Unit = {
    // create the model and persist it
    val spark = SparkSession.builder.config(SparkSessionSetup.testConf()).getOrCreate()
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
    val ovr = new OneVsRestSpark()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setClassifier(rf)
    val model = ovr.fit(df)
    // persist model
    MLeapUtils.saveSparkModel(model, model.transform(df), modelPath)
  }

  override protected def afterAll(): Unit = {
    // delete the model file
    val fileTemp = new File(s"c:$modelPath")
    if (fileTemp.exists) {
      fileTemp.delete()
    }
  }

  "a OneVsRest mleap estimator" should {
    "have the correct input/output schema without probability column" in {
      val transformer = OneVsRest(
        shape = NodeShape.basicClassifier(),
        model = new OneVsRestModel(null, numClasses = 4, numFeatures = 3))
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType.Double(3)),
          StructField("prediction", ScalarType.Double)))
    }
    "have the correct input/output schema with probability column" in {
      val transformer = OneVsRest(shape = NodeShape().withInput("features", "features")
        .withOutput("probability", "prob")
        .withOutput("prediction", "prediction"),
        model = new OneVsRestModel(null, numClasses = 4, numFeatures = 3))
      assert(transformer.schema.fields ==
        Seq(StructField("features", TensorType.Double(3)),
          StructField("prob", TensorType.Double(4)),
          StructField("prediction", ScalarType.Double)))
    }
    "score as intended when loaded with mleap runtime" in {
      val model: Transformer = MLeapUtils.loadMleapModel(modelPath)
      // create a simple LeapFrame to transform
      import ml.combust.mleap.core.types._
      import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}
      val schema = StructType(StructField("features", TensorType.Double(1))).get
      val start = System.currentTimeMillis()
      val data = Seq(Row(Tensor.scalar(0.9)), Row(Tensor.scalar(0.5)), Row(Tensor.scalar(0.1)))
      val frame = DefaultLeapFrame(schema, data)
      val transformed = model.transform(frame).get
      val time = System.currentTimeMillis() - start
      println(s"scored in $time millis")
      transformed.show()
      val row0Probabilities = transformed.collect()(0).getSeq[Double](1)
      row0Probabilities should have size 3
    }
  }
}