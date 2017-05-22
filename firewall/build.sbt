lazy val root = (project in file(".")).
    settings(
        name := "liuliang",
        version := "1.3",
        scalaVersion := "2.11.8",
        libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.0.1"
    ) 