name := "dpipes"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq("org.slf4j" % "slf4j-api" % "1.7.19",
                            "org.slf4j" % "slf4j-simple" % "1.7.19",
                            "org.json4s" %% "json4s-jackson" % "3.2.11",
                            "org.json4s" %% "json4s-ext" % "3.2.11"
)

//val json4sNative = "org.json4s" %% "json4s-native" % "{latestVersion}"
// val json4sJackson = "org.json4s" %% "json4s-jackson" % "{}"

// set the main class for 'sbt run'
mainClass in (Compile, run) := Some("Main")


