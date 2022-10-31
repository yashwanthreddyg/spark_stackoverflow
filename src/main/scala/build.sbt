ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.2.1"

libraryDependencies ++= Seq(
  ("org.apache.spark" %% "spark-sql" % "3.2.0" % "provided").cross(CrossVersion.for3Use2_13)
)

libraryDependencies += "io.github.vincenzobaz" %% "spark-scala3" % "0.1.3"

Compile / run := Defaults.runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner).evaluated


lazy val root = (project in file("."))
  .settings(
    name := "stackoverflow"
  )
