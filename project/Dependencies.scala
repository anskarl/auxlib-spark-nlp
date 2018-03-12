import sbt._
import sbt.Keys._

object Dependencies  {

  object v {

    final val LanguageDetector = "0.6"

    final val OpenNLP = "1.8.3"

    final val UrlDetector = "0.1.17"

    final val AutoLink = "0.6.0"

    final val ScalaTest = "3.0.5"
    
    final val Logback = "1.2.3"
    
    final val SLF4J = "1.7.25"

  }

  lazy val LanguageDetector = "com.optimaize.languagedetector" % "language-detector" % v.LanguageDetector

  lazy val OpenNLP = Seq("opennlp-tools", "opennlp-uima") map {
    artifactId => "org.apache.opennlp" % artifactId % v.OpenNLP
  }

  lazy val UrlDetector = "com.linkedin.urls" % "url-detector" % v.UrlDetector

  lazy val AutoLinkJava = "org.nibor.autolink" % "autolink" % v.AutoLink

  lazy val SparkTest =
    Seq("spark-core", "spark-sql", "spark-mllib")
      .map(a => "org.apache.spark" %% a % "2.0.2" % "test" excludeAll(
        ExclusionRule(organization = "org.scalacheck"),
        ExclusionRule(organization = "org.scalactic"),
        ExclusionRule(organization = "org.scalatest")
      ))

  lazy val Spark =
    Seq("spark-core", "spark-streaming", "spark-sql", "spark-hive", "spark-mllib", "spark-graphx", "spark-catalyst")
      .map(a => "org.apache.spark" %% a % "2.0.2" % "provided" excludeAll(
        ExclusionRule(organization = "org.scalacheck"),
        ExclusionRule(organization = "org.scalactic"),
        ExclusionRule(organization = "org.scalatest")
      ))

  lazy val Test = Seq(
    "org.scalatest" %% "scalatest" % v.ScalaTest % "test",
    "ch.qos.logback" % "logback-classic" % v.Logback % "test",
    "org.slf4j" % "jcl-over-slf4j" % v.SLF4J % "test",
    "org.slf4j" % "jul-to-slf4j" % v.SLF4J % "test",
    "org.slf4j" % "log4j-over-slf4j" % v.SLF4J % "test",
    "org.slf4j" % "slf4j-api" % v.SLF4J % "test",
    "org.slf4j" % "slf4j-nop" % v.SLF4J % "test"
  )

}