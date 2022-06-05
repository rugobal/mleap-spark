package com.rugobal.mleap.ops

import com.rugobal.mleap.{ColumnPercentage, MLeapColumnPercentageModel}
import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.runtime.MleapContext

class ColumPercentageOp extends MleapOp[ColumnPercentage, MLeapColumnPercentageModel] {
  override val Model: OpModel[MleapContext, MLeapColumnPercentageModel] = new OpModel[MleapContext, MLeapColumnPercentageModel] {
    // the class of the model is needed for when we go to serialize JVM objects
    override val klazz: Class[MLeapColumnPercentageModel] = classOf[MLeapColumnPercentageModel]

    // a unique name for our op: "column_percentage_estimator"
    override def opName: String = "column_percentage_estimator"

    override def store(model: Model, obj: MLeapColumnPercentageModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      // unzip our categories/percentages map so we can store the cat and the percentage value
      // as two parallel arrays, we do this because MLeap Bundles do
      // not support storing data as a map
      val (categories, percentages) = obj.catMap.toSeq.unzip
      val missingValue = obj.missingValue
      // add the categories, percentages and missingValue to the Bundle model that
      // will be serialized to our MLeap bundle
      model.withValue("categories", Value.stringList(categories))
        .withValue("percentages", Value.doubleList(percentages))
        .withValue("missingValue", Value.double(missingValue))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): MLeapColumnPercentageModel = {
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

  // the core model that is used by the transformer
  override def model(node: ColumnPercentage): MLeapColumnPercentageModel = node.model
}