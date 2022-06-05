package com.rugobal.mleap.spark.ops.classification

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.bundle.serializer.ModelSerializer
import org.apache.spark.ml.attribute.NominalAttribute
import org.apache.spark.ml.bundle.{ParamSpec, SimpleParamSpec, SimpleSparkOp, SparkBundleContext}
import org.apache.spark.ml.rugobal.classification.OneVsRestModel
import org.apache.spark.ml.classification.ClassificationModel

class OneVsRestOp extends SimpleSparkOp[OneVsRestModel] {
  override val Model: OpModel[SparkBundleContext, OneVsRestModel] = new OpModel[SparkBundleContext, OneVsRestModel] {
    override val klazz: Class[OneVsRestModel] = classOf[OneVsRestModel]

    override def opName: String = "one_vs_rest_with_all_class_probabilities"

    override def store(model: Model, obj: OneVsRestModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      var i = 0
      for (cModel <- obj.models) {
        val name = s"model$i"
        ModelSerializer(context.bundleContext(name)).write(cModel)
        i = i + 1
        name
      }
      model.withValue("num_classes", Value.long(obj.models.length))
        .withValue("num_features", Value.long(obj.models.head.numFeatures))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): OneVsRestModel = {
      val numClasses = model.value("num_classes").getLong.toInt
      val models = (0 until numClasses).toArray.map {
        i => ModelSerializer(context.bundleContext(s"model$i")).read().get.asInstanceOf[ClassificationModel[_, _]]
      }
      val labelMetadata = NominalAttribute.defaultAttr.
        withName("prediction").
        withNumValues(models.length).
        toMetadata
      new OneVsRestModel(uid = "", labelMetadata = labelMetadata, models = models)
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: OneVsRestModel): OneVsRestModel = {
    val labelMetadata = NominalAttribute.defaultAttr.
      withName(shape.output("prediction").name).
      withNumValues(model.models.length).
      toMetadata
    new OneVsRestModel(uid = uid, labelMetadata = labelMetadata, models = model.models)
  }

  override def sparkInputs(obj: OneVsRestModel): Seq[ParamSpec] = {
    Seq("features" -> obj.featuresCol)
  }

  override def sparkOutputs(obj: OneVsRestModel): Seq[SimpleParamSpec] = {
    Seq("probability" -> obj.probabilityCol,
      "prediction" -> obj.predictionCol)
  }
}