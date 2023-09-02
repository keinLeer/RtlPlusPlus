ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.11"

lazy val root = (project in file("."))
  .settings(
    name := "RtlPlusPlus"
  )

resolvers += "Akka library repository".at("https://repo.akka.io/maven")

val AkkaVersion = "2.8.2"
val AkkaHttpVersion = "10.5.2"
val circeVersion = "0.14.3"
libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor-typed" % AkkaVersion,
  "com.typesafe.akka" %% "akka-stream" % AkkaVersion,
  "com.typesafe.akka" %% "akka-http" % AkkaHttpVersion,
  "com.typesafe.akka" %% "akka-stream-typed" % AkkaVersion
)
libraryDependencies += "com.typesafe" % "config" % "1.4.2"
libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % circeVersion)
libraryDependencies += "org.neo4j.driver" % "neo4j-java-driver" % "5.6.0"
