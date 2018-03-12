lazy val root = Project("auxlib-spark-nlp", file("."))
  .settings(libraryDependencies ++= Dependencies.SparkTest)
  .settings(libraryDependencies ++= Dependencies.Spark)
  .settings(libraryDependencies ++= Dependencies.OpenNLP)
  .settings(libraryDependencies += Dependencies.LanguageDetector)
  .settings(libraryDependencies += Dependencies.UrlDetector)
  .settings(libraryDependencies += Dependencies.AutoLinkJava)
  .settings(libraryDependencies ++= Dependencies.Test)
  .settings(Seq(
    parallelExecution in Test := false,
    fork in Test := true,
    javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled"))
  )