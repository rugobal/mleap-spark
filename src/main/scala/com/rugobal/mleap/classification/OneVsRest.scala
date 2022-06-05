package com.rugobal.mleap.classification

import ml.combust.mleap.core.types.NodeShape
import ml.combust.mleap.runtime.frame.{MultiTransformer, Row, Transformer}
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.tensor.{DenseTensor, Tensor}
import ml.combust.mleap.core.util.VectorConverters._

case class OneVsRest(override val uid: String = Transformer.uniqueName("one_vs_rest_with_all_class_probabilities"),
                     override val shape: NodeShape,
                     override val model: OneVsRestModel) extends MultiTransformer {
  override val exec: UserDefinedFunction = {
    val f = shape.getOutput("probability") match {
      case Some(_) =>
        (features: Tensor[Double]) => {
          val (prediction, probability) = model.predictWithProbability(features)
          // val probabilityTensor: Tensor[Double] = Tensor.denseVector(probability)
          Row(probability.toSeq, prediction)
        }
      case None =>
        (features: Tensor[Double]) => Row(model(features))
    }
    UserDefinedFunction(f, outputSchema, inputSchema)
  }
}