package com.rugobal.mleap

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.types.{ScalarType, StructType}

case class MLeapColumnPercentageModel(catMap: Map[String, Double], missingValue: Double) extends Model {
  /** Convert a string into percentage.
   *
   * @param value category
   * @return the percentage associated to the category
   */
  def apply(value: String): Double = catMap.getOrElse(value, missingValue)

  override def inputSchema: StructType = StructType("input" -> ScalarType.String).get

  override def outputSchema: StructType = StructType("output" -> ScalarType.Double.nonNullable).get
}