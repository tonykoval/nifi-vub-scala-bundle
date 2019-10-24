enablePlugins(NarPlugin)

name := "nifi-vub-scala-bundle"
organization := "sk.vub"
version := "0.1"

scalaVersion := "2.13.1"
nifiVersion := "1.9.2"
val circeVersion = "0.12.0"

libraryDependencies ++= Seq(
  "org.apache.nifi" % "nifi-api",
  "org.apache.nifi" % "nifi-dbcp-service-api",
  "org.apache.nifi" % "nifi-hadoop-utils",
  "org.apache.nifi" % "nifi-processor-utils",
  "org.apache.nifi" % "nifi-record",
  "org.apache.nifi" % "nifi-record-serialization-service-api",
  "org.apache.nifi" % "nifi-record-serialization-services",
  "org.apache.nifi" % "nifi-schema-registry-service-api",
  "org.apache.nifi" % "nifi-ssl-context-service-api",
  "org.apache.nifi" % "nifi-standard-record-utils",
  "org.apache.nifi" % "nifi-documentation",
  "org.apache.nifi" % "nifi-framework-nar-utils"
).map(_ % nifiVersion.value)

libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser",
  "io.circe" %% "circe-optics"
).map(_ % circeVersion)

libraryDependencies ++= Seq(
  "com.github.wnameless" % "json-flattener" % "0.7.1",
  "org.scalactic" %% "scalactic" % "3.0.8",
  "org.scalatest" %% "scalatest" % "3.0.8" % Test,
  "org.apache.nifi" % "nifi-record-serialization-services" % nifiVersion.value % Test,
  "org.apache.nifi" % "nifi-mock" % nifiVersion.value % Test,
  "org.apache.nifi" % "nifi-mock-record-utils" % nifiVersion.value % Test
)
