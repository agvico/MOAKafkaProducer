name := "MOAKafkaProducer"

version := "0.1"

scalaVersion := "2.11.12"

mainClass in Compile := Some("Main")

libraryDependencies ++= Seq(

  // PicoCLI
  "info.picocli" % "picocli" % "3.8.0",

  // MOA
  "nz.ac.waikato.cms.moa" % "moa" % "2019.05.0",

  //Kafka (ONLY PRODUCER!)
  "org.apache.kafka" % "kafka-clients" % "2.3.1"


)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
