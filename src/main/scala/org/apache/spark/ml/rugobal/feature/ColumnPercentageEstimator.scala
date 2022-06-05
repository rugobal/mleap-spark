package org.apache.spark.ml.rugobal.feature

import org.apache.hadoop.fs.Path
import org.apache.spark.ml.rugobal.feature.ColumnPercentageModel.ColumnPercentageModelWriter
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.param.{DoubleParam, ParamMap, Params}
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, functions => F}

trait ColumnPercentageParams extends Params with HasInputCol with HasOutputCol {
  final val missingValue = new DoubleParam(this, "missingValue",
    "The value to set for unseen categories when transforming")
  setDefault(missingValue, 0.0)

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def getMissingValue: Double = $(missingValue)

  def setMissingValue(value: Double): this.type = set(missingValue, value)

  /** Validates and transforms the input schema. */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    // Check that the input type is a string
    val idx = schema.fieldIndex($(inputCol))
    val field = schema.fields(idx)
    if (field.dataType != StringType) {
      throw new IllegalArgumentException(s"Input type ${field.dataType} did not match input type StringType")
    }
    // Add the return field
    schema.add(StructField($(outputCol), DoubleType, false))
  }
}

/**
 * Estimator that calculates 'category column' percentages: Given an category column, it calculates the
 * percentage of every category over the total number of rows.
 *
 * It does the following:
 *
 * Given the input column, it performs: select $inputColumn, count($inputColumn) from dataset group by $inputColumn.
 * Then it counts the total number of rows of the original dataset and divides every 'count' value of the grouped
 * dataset by it. So it's calculating the percentage of every category in the $inputColumn (which must be of
 * string type) over the total number of rows.
 *
 * This is performed in the 'fit' method. And the results of this map of 'category -> percentage' are held for the
 * transform phase.
 *
 * During the transform phase, the values of the $inputColumn of the input dataset are looked up on the map and the
 * corresponding percentages added to the $outputColumn. Values of the $inputColumn that have not been 'seen' during
 * training (fitting), get a percentage of 0.0. This value can be configured through the $missingValue parameter.
 *
 * This estimator is persistable. It can be saved and loaded.
 *
 * @param uid
 */
class ColumnPercentageEstimator(override val uid: String)
  extends Estimator[ColumnPercentageModel] with ColumnPercentageParams with DefaultParamsWritable {
  def this() = this(Identifiable.randomUID("column_percentage_estimator"))

  override def copy(extra: ParamMap): ColumnPercentageEstimator = {
    defaultCopy(extra)
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def fit(dataset: Dataset[_]): ColumnPercentageModel = {
    val statsDf: Dataset[_] = getColumnStats(dataset)
    val catMap: Map[String, Double] = statsDf.select(F.col($(inputCol)), F.col($(outputCol))).collect().map(r =>
      (r.getString(0), r.getDouble(1))).toMap
    // Check if the input column contains null values, and fail if it does, since it would give problems when
    // serializing the model
    if (catMap.keySet.contains(null)) throw new IllegalStateException(s"The ${$(inputCol)} column cannot contain null values")
    copyValues(new ColumnPercentageModel(uid, catMap)).setParent(this)
  }

  private def getColumnStats(df: Dataset[_]): Dataset[_] = {
    val columnDf = df.select($(inputCol)).cache
    val total = columnDf.count // Total number of rows
    columnDf.groupBy($(inputCol))
      .agg(F.count($(inputCol)).as($(inputCol) + "_count"))
      .withColumn($(outputCol), F.col($(inputCol) + "_count") / total)
      .drop($(inputCol) + "_count")
  }
}

class ColumnPercentageModel(override val uid: String, val catMap: Map[String, Double])
  extends Model[ColumnPercentageModel] with ColumnPercentageParams with MLWritable {
  override def copy(extra: ParamMap): ColumnPercentageModel = {
    val copied = new ColumnPercentageModel(uid, catMap).setParent(parent)
    copyValues(copied, extra)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val estimator = F.udf { cat: String => catMap.getOrElse(cat, $(missingValue)) }
    dataset.select(F.col("*"), estimator(F.col($(inputCol))).as($(outputCol)))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def write: MLWriter = new ColumnPercentageModelWriter(this)
}

object ColumnPercentageModel extends MLReadable[ColumnPercentageModel] {
  private[ColumnPercentageModel] class ColumnPercentageModelWriter(instance: ColumnPercentageModel) extends MLWriter {
    private case class Data(catMap: Map[String, Double])

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.catMap)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class ColumnPercentageModelReader extends MLReader[ColumnPercentageModel] {
    private val className = classOf[ColumnPercentageModel].getName

    override def load(path: String): ColumnPercentageModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      //      implicit val formats = DefaultFormats
      //      val missingValue = metadata.getParamValue("missingValue").extract[Double]
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath)
        .select("catMap")
        .head()
      val catMap: Map[String, Double] = data.getAs[Map[String, Double]](0)
      val model = new ColumnPercentageModel(metadata.uid, catMap)
      metadata.getAndSetParams(model)
      model
    }
  }

  override def read: MLReader[ColumnPercentageModel] = new ColumnPercentageModelReader()

  override def load(path: String): ColumnPercentageModel = super.load(path)
}