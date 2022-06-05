package com.rugobal.mleap

import java.net.URI
import ml.bundle.hdfs.HadoopBundleFileSystem
import ml.combust.bundle.BundleFile
import ml.combust.bundle.dsl.Bundle
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.MleapSupport._
import ml.combust.mleap.runtime.frame.{Transformer => MLeapTransformer}
import ml.combust.mleap.runtime.javadsl.ContextBuilder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import resource.managed

object MLeapJavaUtils {
  /**
   * Loads an Mleap model stored in Mleap format. It loads both from local file system or hdfs.
   *
   * To load from local file system, the path must start with '/' (it works as well for windows systems)
   * To load from hdfs, the path must start with 'hdfs://'
   *
   * @param path
   * @return
   */
  def loadMleapModel(path: String, context: MleapContext): MLeapTransformer = {
    path match {
      case p if p.startsWith("/") =>
        loadMleapModelFromLocalFileSystem(p, if (context == null) new ContextBuilder().createMleapContext() else context)
      case p if p.startsWith("hdfs") =>
        loadMleapModelFromHdfs(p, context)
    }
  }

  private def loadMleapModelFromHdfs(path: String, context: MleapContext): MLeapTransformer = {
    assert(path.startsWith("hdfs://"), "path must start with hdfs://")
    assert(path.endsWith(".zip"), "path must end in .zip")
    implicit val c: MleapContext = context
    val bundle = (for (bf <- managed(BundleFile.load(new URI(path)))) yield {
      bf.load[MleapContext, MLeapTransformer]().get
    }).tried
    bundle.get.root
  }

  private def loadMleapModelFromLocalFileSystem(path: String, context: MleapContext): MLeapTransformer = {
    assert(path.startsWith("/"), "path must start with a single /")
    assert(!path.startsWith("//"), "path must start with a single /")
    assert(path.endsWith(".zip"), "path must end in .zip")
    implicit val c: MleapContext = context
    val bundle: Bundle[MLeapTransformer] = (for (bundleFile <- managed(BundleFile(s"jar:file:$path"))) yield {
      bundleFile.loadMleapBundle().get
    }).opt.get
    bundle.root
  }

  def createMleapHdfsContext(config: Configuration): MleapContext = {
    // Create the hadoop file system
    val fs = FileSystem.get(config)
    // Create the hadoop bundle file system
    val bundleFs = new HadoopBundleFileSystem(fs)
    MleapContext.defaultContext.copy(
      registry = MleapContext.defaultContext.bundleRegistry.registerFileSystem(bundleFs)
    )
  }
}