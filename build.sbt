name := "nifi-processor-scala-bundle"

version := "0.1"

scalaVersion := "2.12.10"

val nifiVersion = "1.9.2"
val circeVersion = "0.9.3"

libraryDependencies ++= Seq(
  "org.apache.nifi" % "nifi-api",
  "org.apache.nifi" % "nifi-processor-utils",
  "org.apache.nifi" % "nifi-ssl-context-service-api",
  "org.apache.nifi" % "nifi-record",
  "org.apache.nifi" % "nifi-record-serialization-service-api",
  "org.apache.nifi" % "nifi-dbcp-service-api",
  "org.apache.nifi" % "nifi-schema-registry-service-api",
  "org.apache.nifi" % "nifi-hadoop-utils",
  "org.apache.nifi" % "nifi-mock"
).map(_ % nifiVersion)

libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser",
  "io.circe" %% "circe-optics"
).map(_ % circeVersion)

enablePlugins(PackPlugin, NarPlugin)
