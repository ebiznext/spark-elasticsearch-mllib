scalacOptions in (Compile, console) += "-Yrepl-sync"

organization := "com.ebiznext.scalaio"

name := "import"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.10.4"

resolvers += "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/"

resolvers += "Conjars" at "http://conjars.org/repo"

resolvers += "cljars" at "https://clojars.org/repo/"

val jacksonV = "2.4.3"

val elastic4sV = "1.3.2"

val elasticSearchV = "1.3.2"

val sparkV = "1.1.0"

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.0.2",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonV,
  "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonV,
  "com.fasterxml.jackson.core" % "jackson-core" % jacksonV,
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonV,
  "com.sksamuel.elastic4s" %% "elastic4s" % elastic4sV exclude("org.elasticsearch", "elasticsearch"),
  "org.elasticsearch" % "elasticsearch" % elasticSearchV,
  "org.apache.spark" %% "spark-core" % sparkV,
  "org.apache.spark" %% "spark-mllib" % sparkV,
  "org.elasticsearch" % "elasticsearch-hadoop" % "2.1.0.Beta2",
  "org.apache.mesos" % "mesos" % "0.18.1" exclude("com.google.protobuf", "protobuf-java"),
  "org.specs2" %% "specs2" % "2.3.13" % "test"
)

packAutoSettings


