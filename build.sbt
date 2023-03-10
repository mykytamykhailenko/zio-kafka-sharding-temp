ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "zio-kafka-sharding-refactoring"
  )

libraryDependencies ++= Seq(
  "dev.zio" %% "zio" % "2.0.9",
  "dev.zio" %% "zio-concurrent" % "2.0.9",
  "dev.zio" %% "zio-streams" % "2.0.9",
  "dev.zio" %% "zio-kafka" % "2.0.7",
  "dev.zio" %% "zio-logging" % "2.1.9",
  "com.sksamuel.avro4s" %% "avro4s-core" % "4.1.0"
)

addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")

