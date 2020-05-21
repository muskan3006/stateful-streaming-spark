name := "stateful-streaming"

version := "0.1"

scalaVersion := "2.12.6"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.0-preview2"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.0.0-preview2"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.0.0-preview2"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.5.0"
libraryDependencies += "org.apache.kafka" %% "kafka" % "2.5.0"
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.7.4"