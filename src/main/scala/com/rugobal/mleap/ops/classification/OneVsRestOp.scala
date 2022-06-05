package com.rugobal.mleap.ops.classification

import com.rugobal.mleap.classification.{OneVsRest, OneVsRestModel}
import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl.{Bundle, Model, Value}
import ml.combust.bundle.op.OpModel
import ml.combust.bundle.serializer.ModelSerializer
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core.classification.ProbabilisticClassificationModel
import ml.combust.mleap.runtime.MleapContext

class OneVsRestOp extends MleapOp[OneVsRest, OneVsRestModel] {
  override val Model: OpModel[MleapContext, OneVsRestModel] = new OpModel[MleapContext, OneVsRestModel] {
    override val klazz: Class[OneVsRestModel] = classOf[OneVsRestModel]

    override def opName: String = "one_vs_rest_with_all_class_probabilities"

    override def store(model: Model, obj: OneVsRestModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      var i = 0
      for (cModel <- obj.classifiers) {
        val name = s"model$i"
        ModelSerializer(context.bundleContext(name)).write(cModel).get
        i = i + 1
        name
      }
      model.withValue("num_classes", Value.long(obj.classifiers.length)).
        withValue("num_features", Value.long(obj.numFeatures))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): OneVsRestModel = {
      val numClasses = model.value("num_classes").getLong.toInt
      val numFeatures = model.value("num_features").getLong.toInt
      val models = (0 until numClasses).toArray.map {
        i => ModelSerializer(context.bundleContext(s"model$i")).read().get.asInstanceOf[ProbabilisticClassificationModel]
      }
      OneVsRestModel(classifiers = models, numClasses = numClasses, numFeatures = numFeatures)
    }
  }

  override def model(node: OneVsRest): OneVsRestModel = node.model
}