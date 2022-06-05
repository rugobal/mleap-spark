package com.rugobal.mleap

import java.net.URI
import ml.combust.bundle.BundleFile
import ml.combust.bundle.dsl.Bundle
import ml.combust.bundle.serializer.SerializationFormat
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.MleapSupport._
import ml.combust.mleap.runtime.frame.{Transformer => MLeapTransformer}
import ml.combust.mleap.spark.SparkSupport._
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.sql.DataFrame
import resource.managed

object MLeapUtils {
  /**
   * Saves a spark model in Mleap format. It saves both in local file system or hdfs.
   *
   * To save in local file system, the path must start with '/' (it works as well for windows systems)
   * To save in hdfs, the path must start with 'hdfs://'
   *
   * @param model
   * @param transformed
   * @param path
   */
  def saveSparkModel(model: Transformer, transformed: DataFrame, path: String): Unit = {
    path match {
      case p if p.startsWith("/") => saveSparkModelAsJar(model, transformed, p)
      case p if p.startsWith("hdfs") => saveSparkModelInHdfs(model, transformed, p)
    }
  }

  /**
   * Loads a spark model stored in Mleap format. It loads both from local file system or hdfs.
   *
   * To load from local file system, the path must start with '/' (it works as well for windows systems)
   * To load from hdfs, the path must start with 'hdfs://'
   *
   * @param path
   * @return
   */
  def loadSparkModel(path: String): Transformer = {
    path match {
      case p if p.startsWith("/") => loadSparkModelFromLocalFileSystem(p)
      case p if p.startsWith("hdfs") => loadSparkModelFromHdfs(p)
    }
  }

  /**
   * Loads an MLeap model stored in Mleap format stored in the local file sytem.
   *
   * @param path must start with '/'
   * @return
   */
  def loadMleapModel(path: String): MLeapTransformer = {
    assert(path.startsWith("/"), "path must start with a single /")
    assert(!path.startsWith("//"), "path must start with a single /")
    assert(path.endsWith(".zip"), "path must end in .zip")
    val bundle: Bundle[MLeapTransformer] = (for (bundleFile <- managed(BundleFile(s"jar:file:$path"))) yield {
      bundleFile.loadMleapBundle().get
    }).opt.get
    bundle.root
  }

  /**
   * Loads an MLeap model stored in Mleap format stored in HDFS.
   *
   * @param path must start with hdfs://
   * @param context
   * @return
   */
  def loadMLeapModelFromHdfs(path: String)(implicit context: MleapContext): MLeapTransformer = {
    assert(path.startsWith("hdfs://"), "path must start with hdfs://")
    assert(path.endsWith(".zip"), "path must end in .zip")
    val bundle = (for (bf <- managed(BundleFile.load(new URI(path)))) yield {
      bf.load[MleapContext, MLeapTransformer]().get
    }).tried
    bundle.get.root
  }

  private def saveSparkModelAsJar(model: Transformer, transformed: DataFrame, path: String): Unit = {
    assert(path.startsWith("/"), "path must start with a single /")
    assert(!path.startsWith("//"), "path must start with a single /")
    assert(path.endsWith(".zip"), "path must end in .zip")
    val sbc = SparkBundleContext().withDataset(transformed)
    for (bf <- managed(BundleFile(s"jar:file:$path"))) {
      model.writeBundle.format(SerializationFormat.Json).save(bf)(sbc).get
    }
  }

  private def saveSparkModelInHdfs(model: Transformer, transformed: DataFrame, path: String): Unit = {
    assert(path.startsWith("hdfs://"), "path must start with hdfs://")
    assert(path.endsWith(".zip"), "path must end in .zip")
    implicit val sbc: SparkBundleContext = SparkBundleContext.defaultContext.withDataset(transformed)
    model.writeBundle.save(new URI(path))
  }

  private def loadSparkModelFromLocalFileSystem(path: String): Transformer = {
    assert(path.startsWith("/"), "path must start with a single /")
    assert(!path.startsWith("//"), "path must start with a single /")
    assert(path.endsWith(".zip"), "path must end in .zip")
    val bundle: Bundle[Transformer] = (for (bundleFile <- managed(BundleFile(s"jar:file:$path"))) yield {
      bundleFile.loadSparkBundle().get
    }).opt.get
    bundle.root
  }

  private def loadSparkModelFromHdfs(path: String): Transformer = {
    assert(path.startsWith("hdfs://"), "path must start with hdfs://")
    assert(path.endsWith(".zip"), "path must end in .zip")
    import ml.combust.mleap.spark.SparkSupport._
    new URI(path).loadMleapBundle().get.root
  }
}