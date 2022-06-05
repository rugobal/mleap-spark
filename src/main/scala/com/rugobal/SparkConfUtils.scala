package com.rugobal

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.{DenseMatrix, SparseMatrix}
import scala.util.Try

/**
 * Class used to set our specific spark configuration properties.
 */
object SparkConfUtils {
  @transient private lazy val log = Logger.getLogger(getClass.getName)

  /**
   * Set specific properties in the SparkConf object.
   */
  def setProperties(conf: SparkConf): Unit = {
    // If the master is not set at this point raise an exception
    if (Try(conf.get("spark.master")).isFailure)
      throw new IllegalStateException("configuration property 'spark.master' must be set already at this point")
    // Try to set the right parallelism level
    val parLevel: Option[Int] = getLevelOfParalelism(conf)
    parLevel.foreach { x =>
      conf.set("spark.default.parallelism", String.valueOf(x))
      conf.set("spark.sql.shuffle.partitions", String.valueOf(x))
    }
    // Set Kryo serializer and our Kryo registrator
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryoserializer.buffer.max", "1024m")
    conf.registerKryoClasses(Array(classOf[DenseMatrix], classOf[SparseMatrix], classOf[Array[Double]]))
  }

  private def getLevelOfParalelism(conf: SparkConf): Option[Int] = {
    val localMasterPattern = "local\\[([0-9]+)\\]".r
    // If the master is local, then return the number of processors
    // chosen * 2, and if not, (i.e, we run on the cluster)
    // we try to read the number of cores and number of executors
    // and set the value based on that
    val sparkMaster = conf.get("spark.master")
    if (sparkMaster.startsWith("local")) {
      val numCores = sparkMaster match {
        case "local" => Some(1)
        case "local[*]" => Some(Runtime.getRuntime.availableProcessors)
        case localMasterPattern(nCores) => {
          if (nCores.toInt > 0) Some(nCores.toInt) else None
        }
        case _ => None
      }
      if (numCores.isEmpty) throw new IllegalStateException(s"Invalid value for spark.master=$sparkMaster")
      log.info(s"Local master detected. Setting default parallelism level to: ${numCores.get * 2}")
      Some(numCores.get * 2)
    } else if (sparkMaster.contains("yarn")) {
      val parallelismLevel =
        for (executors <- Try(conf.get("spark.executor.instances").toInt).toOption;
             coresPerExecutor <- Try(conf.get("spark.executor.cores").toInt).toOption)
        yield executors * coresPerExecutor * 2
      if (parallelismLevel.isDefined) {
        log.info(s"Setting default parallelism level to: ${parallelismLevel.get}")
      } else {
        log.warn("Could not set default parallelism level. Please provide --num-executors (spark.executor.instances) and --executor-cores (spark.executor.cores) when submitting the application")
        log.warn("spark.executor.instances = " + Try(conf.get("spark.executor.instances")).getOrElse("null"))
        log.warn("spark.executor.cores = " + Try(conf.get("spark.executor.cores")).getOrElse("null"))
      }
      parallelismLevel
    } else {
      throw new IllegalStateException("Unrecognized spark.master value: " + sparkMaster)
    }
  }
}