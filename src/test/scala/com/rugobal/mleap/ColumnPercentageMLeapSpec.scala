package com.rugobal.mleap

import com.rugobal.PathUtils

import java.io.File
import java.nio.file.{Path, Paths}
import ml.combust.mleap.runtime.frame.Transformer
import ml.combust.mleap.runtime.function.{FieldSelector, StructSelector, UserDefinedFunction}
import org.apache.spark.ml.rugobal.SparkSessionSetup
import org.apache.spark.ml.rugobal.feature.ColumnPercentageEstimator
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

class ColumnPercentageMLeapSpec extends WordSpec with Matchers with BeforeAndAfterAll {
  val modelPath = PathUtils.currentPath() + "/col-perc-mleap-test-spec.model.zip"

  // setup
  override def beforeAll(): Unit = {
    // create the model and persist it
    val spark = SparkSession.builder.config(SparkSessionSetup.testConf()).getOrCreate()
    val df = spark.createDataFrame(Seq("SPAIN", "SPAIN", "FRANCE", "ITALY").map(Tuple1.apply)).toDF("country")
    val estimator = new ColumnPercentageEstimator()
      .setInputCol("country")
      .setOutputCol("country_perc")
      .setMissingValue(-1.0)
    val model = estimator.fit(df)
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

  "a ColumnPercentage mleap estimator" should {
    "score as intended when loaded with mleap runtime" in {
      val model: Transformer = MLeapUtils.loadMleapModel(modelPath)
      // create a simple LeapFrame to transform
      import ml.combust.mleap.core.types._
      import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}
      val schema = StructType(StructField("country", ScalarType.String)).get
      val start = System.currentTimeMillis()
      val data = Seq(Row("SPAIN"), Row("FRANCE"), Row("ITALY"), Row("UK"))
      val frame = DefaultLeapFrame(schema, data)
      val transformed = model.transform(frame).get
      val time = System.currentTimeMillis() - start
      println(s"scored in $time millis")
      transformed.show()
      val f: String => Boolean = (values: String) => {
        values == "SPAIN"
      }
      val inputSchema = StructType(StructField("country", ScalarType.String)).get
      val outputSchema = StructType(StructField("output", ScalarType.Boolean)).get
      val spainPerc = transformed.filter(FieldSelector("country"))(UserDefinedFunction(f, outputSchema, inputSchema)).get.collect()(0).getDouble(1)
      val f2: Row => Boolean = (r: Row) => {
        r.getString(0) == "SPAIN"
      }
      val spainPerc2 = transformed.filter(StructSelector(Seq("country", "country_perc")))(UserDefinedFunction(f2, outputSchema, Seq(SchemaSpec(schema)))).get.collect()(0).getDouble(1)
      assert(spainPerc === 0.5)
      assert(spainPerc2 === 0.5)
    }
  }
}