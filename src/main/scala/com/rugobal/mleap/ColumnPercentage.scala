package com.rugobal.mleap

import ml.combust.mleap.core.types.NodeShape
import ml.combust.mleap.runtime.frame.{SimpleTransformer, Transformer}
import ml.combust.mleap.runtime.function.UserDefinedFunction

case class ColumnPercentage(override val uid: String = Transformer.uniqueName("column_percentage_estimator"),
                            override val shape: NodeShape,
                            override val model: MLeapColumnPercentageModel) extends SimpleTransformer {
  override val exec: UserDefinedFunction = (label: String) => model(label)
}