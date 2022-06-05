package com.rugobal.mleap.spark.ops

import com.rugobal.mleap.MLeapColumnPercentageModel
import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.rugobal.feature.ColumnPercentageModel

class ColumnPercentageEstimatorOp extends OpNode[SparkBundleContext, ColumnPercentageModel, MLeapColumnPercentageModel] {
  override val Model: OpModel[SparkBundleContext, MLeapColumnPercentageModel] = new OpModel[SparkBundleContext, MLeapColumnPercentageModel] {
    // the class of the model is needed for when we go to serialize JVM objects
    override val klazz: Class[MLeapColumnPercentageModel] = classOf[MLeapColumnPercentageModel]

    // a unique name for our op: "column_percentage_estimator"
    // this should be the same as for the MLeap transformer serialization
    override def opName: String = "column_percentage_estimator"

    override def store(model: Model, obj: MLeapColumnPercentageModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      // unzip our categories/percentages map so we can store the cat and the percentage value
      // as two parallel arrays, we do this because MLeap Bundles do
      // not support storing data as a map
      val (categories, percentages) = obj.catMap.toSeq.unzip
      val missingValue: Double = obj.missingValue
      // add the categories, percentages and missingValue to the Bundle model that
      // will be serialized to our MLeap bundle
      model.withValue("categories", Value.stringList(categories))
        .withValue("percentages", Value.doubleList(percentages))
        .withValue("missingValue", Value.double(missingValue))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): MLeapColumnPercentageModel = {
      // retrieve our list of categories
      val categories = model.value("categories").getStringList
      // retrieve our list of percentages
      val percentages = model.value("percentages").getDoubleList
      // retrieve the missingValue
      val missingValue = model.value("missingValue").getDouble
      // reconstruct the model using the parallel categories and percentages and missingValue
      MLeapColumnPercentageModel(categories.zip(percentages).toMap, missingValue)
    }
  }
  override val klazz: Class[ColumnPercentageModel] = classOf[ColumnPercentageModel]

  override def name(node: ColumnPercentageModel): String = node.uid

  override def model(node: ColumnPercentageModel): MLeapColumnPercentageModel = MLeapColumnPercentageModel(node.catMap, node.getMissingValue)

  override def load(node: Node, model: MLeapColumnPercentageModel)
                   (implicit context: BundleContext[SparkBundleContext]): ColumnPercentageModel = {
    new ColumnPercentageModel(uid = node.name, catMap = model.catMap)
      .setInputCol(node.shape.standardInput.name)
      .setOutputCol(node.shape.standardOutput.name)
      .setMissingValue(model.missingValue)
  }

  override def shape(node: ColumnPercentageModel)(implicit context: BundleContext[SparkBundleContext]): NodeShape =
    NodeShape().withStandardIO(node.getInputCol, node.getOutputCol)
}