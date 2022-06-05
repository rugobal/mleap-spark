package com.rugobal.mleap.classification

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.classification.ProbabilisticClassificationModel
import ml.combust.mleap.core.types.{ScalarType, StructType, TensorType}
import org.apache.spark.ml.linalg.Vector

/** Class for multinomial one vs rest models.
 *
 * One vs rest models are comprised of a series of
 * [[ProbabilisticClassificationModel]]s which are used to
 * predict each class.
 *
 * @param classifiers binary classification models
 */
case class OneVsRestModel(classifiers: Array[ProbabilisticClassificationModel],
                          numClasses: Int,
                          numFeatures: Int) extends Model {
  /** Alias for [[ml.combust.mleap.core.classification.OneVsRestModel#predict]].
   *
   * @param features feature vector
   * @return prediction
   */
  def apply(features: Vector): Double = predict(features)

  /** Predict the class for a feature vector.
   *
   * @param features feature vector
   * @return predicted class
   */
  def predict(features: Vector): Double = {
    predictWithProbability(features)._1
  }

  def predictProbability(features: Vector): Array[Double] = {
    predictWithProbability(features)._2
  }

  /** Predict the class and probability for a feature vector.
   *
   * @param features feature vector
   * @return (predicted class, probability of class)
   */
  def predictWithProbability(features: Vector): (Double, Array[Double]) = {
    // probabilities for each classifier
    val probability: Array[Double] = classifiers.map(c => c.predictProbabilities(features)(1))
    val prediction = probability.indexOf(probability.max).toDouble
    (prediction, probability)
  }

  override def inputSchema: StructType = StructType("features" -> TensorType.Double(numFeatures)).get

  override def outputSchema: StructType = StructType("probability" -> TensorType.Double(numClasses),
    "prediction" -> ScalarType.Double).get
}