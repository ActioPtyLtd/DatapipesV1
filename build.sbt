name := "dpipes"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq("org.slf4j" % "slf4j-api" % "1.7.19",
                            "org.slf4j" % "slf4j-simple" % "1.7.19")


// set the main class for 'sbt run'
mainClass in (Compile, run) := Some("com.actio.Main")


