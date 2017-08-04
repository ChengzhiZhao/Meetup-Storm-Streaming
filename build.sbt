name := "Longpull_Storm"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.storm" % "storm-core" % "1.0.2" exclude("junit", "junit")
libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.1"
libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.1"
libraryDependencies += "org.scala-lang.modules" %% "scala-java8-compat" % "0.8.0"
libraryDependencies += "org.json4s" %% "json4s-native" % "3.4.0"
libraryDependencies += "org.apache.avro" % "avro" % "1.7.7"
libraryDependencies += "org.elasticsearch" % "elasticsearch-storm" % "5.5.0"
libraryDependencies += "commons-httpclient" % "commons-httpclient" % "3.1"
libraryDependencies += "javax.websocket" % "javax.websocket-client-api" % "1.1"
libraryDependencies += "org.glassfish.tyrus.bundles" % "tyrus-standalone-client" % "1.9"
libraryDependencies += "com.typesafe.play" % "play_2.11" % "2.6.0"
libraryDependencies += "redis.clients" % "jedis" % "1.5.2"