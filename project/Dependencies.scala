import sbt._
object Version {
  val spark = "2.4.0"
  val scalaTest = "2.2.2"
  val mleap = "0.13.0"
}
object Library {
  // general
  lazy val scalaTest = "org.scalatest" %% "scalatest" % Version.scalaTest
  // spark
  lazy val sparkCore           = "org.apache.spark" %% "spark-core" % Version.spark
  lazy val sparkSql            = "org.apache.spark" %% "spark-hive" % Version.spark
  lazy val sparkMllib          = "org.apache.spark" %% "spark-mllib" % Version.spark
  
  // others
  lazy val mleapRuntime        = "ml.combust.mleap" %% "mleap-runtime" % Version.mleap
  lazy val mleapSpark          = "ml.combust.mleap" %% "mleap-spark" % Version.mleap
}
object Dependencies {

  import Library._

  val common = Seq(
    scalaTest % Test withSources() withJavadoc()
  )
  val spark = Seq(
    sparkCore % "provided" withSources() withJavadoc(),
    sparkSql % "provided" withSources() withJavadoc(),
    sparkMllib % "provided" withSources() withJavadoc()
  )
  val others = Seq(
    mleapRuntime withSources() withJavadoc(),
    mleapSpark withSources() withJavadoc()
  )
  val all = common ++ spark ++ others
}
