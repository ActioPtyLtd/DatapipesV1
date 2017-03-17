name := "dpipes"

version := "1.2.12"

scalaVersion := "2.11.8"

javacOptions ++= Seq("-Xlint:unchecked")

scalacOptions ++= Seq("-unchecked", "-deprecation", "-Ylog-classpath")

publishTo := Some(Resolver.file("file", new File(Path.userHome.absolutePath+"/lib/")))

libraryDependencies ++= Seq(
  "org.scala-lang.modules" %% "scala-xml" % "1.0.4",
  "org.slf4j" % "slf4j-api" % "1.7.19",
  "ch.qos.logback" % "logback-classic" % "1.1.7",
  "org.json4s" %% "json4s-jackson" % "3.3.0",
  "org.json4s" %% "json4s-ext" % "3.3.0",
  "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4",
  "com.typesafe" % "config" % "1.3.0",
  "commons-lang" % "commons-lang" % "2.6",
  "commons-codec" % "commons-codec" % "1.10",
  "org.apache.httpcomponents" % "httpclient" % "4.5.2",
  "com.googlecode.java-diff-utils" % "diffutils" % "1.3.0",
  "commons-cli" % "commons-cli" % "1.3.1",
  "com.sparkjava" % "spark-core" % "2.5",
  "org.postgresql" % "postgresql" % "9.4.1209.jre7",
  "org.scalameta" %% "scalameta" % "1.0.0",
  "commons-net" % "commons-net" % "3.5",
  "me.chrons" %% "boopickle" % "1.2.5"
)

enablePlugins(JavaAppPackaging)

enablePlugins(DockerPlugin)


// set the main class for 'sbt run'
mainClass in (Compile, run) := Some("com.actio.Main")
