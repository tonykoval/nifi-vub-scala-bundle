enablePlugins(NarPlugin)

name := "nifi-processor-scala-bundle"
organization := "sk.vub"
version := "0.1"

scalaVersion := "2.13.1"
nifiVersion := "1.9.2"

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
).map(_ % nifiVersion.value)

libraryDependencies ++= Seq(
  "org.scalactic" %% "scalactic" % "3.0.8",
  "org.scalatest" %% "scalatest" % "3.0.8" % Test,
  "org.apache.nifi" % "nifi-record-serialization-services" % nifiVersion.value % Test,
  "org.apache.nifi" % "nifi-mock-record-utils" % nifiVersion.value % Test
)
