import sbt.Keys.resolvers


name := "testing-spark-cassandra"

version := "0.1"

scalaVersion := "2.11.12"

updateOptions := updateOptions.value.withCachedResolution(true)
parallelExecution in test := false
//Forking is required for the Embedded Cassandra
fork in Test := true

resolvers ++= Seq(
  "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  "Hortonworks" at "http://repo.hortonworks.com/content/repositories/releases/",
  "Hortonworks Groups" at "http://repo.hortonworks.com/content/groups/public/",
  "Apache Snapshots" at "https://repository.apache.org/content/repositories/releases/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.0",
  "org.apache.spark" %% "spark-sql" % "2.3.0",
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.3.0",
  "com.datastax.spark" %% "spark-cassandra-connector-embedded" % "2.3.0" % Test,
  "org.apache.cassandra" % "cassandra-all" % "3.2" % Test,
  "com.holdenkarau" %% "spark-testing-base" % "2.3.0_0.10.0" % Test,
  "org.scalatest" %% "scalatest" % "3.0.1" % Test,
  "org.scalacheck" %% "scalacheck" % "1.10.0" % Test
).map(_.exclude("org.slf4j", "log4j-over-slf4j")) // Excluded to allow Cassandra to run embedded