name := "aialib"

version := "1.0"

scalaVersion := "2.11.7"

scalacOptions := Seq("-deprecation", "-feature", "-unchecked", "-language:implicitConversions", "-language:postfixOps")

val akkaVersion = "2.4-M2"
val breezeVersion = "0.12-SNAPSHOT"

resolvers ++= Seq( //needed for Breeze 0.12-SNAPSHOT
  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
  "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/"
)

assemblyJarName in assembly := "aialib.jar"
mainClass in assembly := Some("aialib.examples.GPRProblem")
test in assembly := {}

libraryDependencies ++= Seq(
  "org.scalanlp" %% "breeze" % breezeVersion,
  "org.scalanlp" %% "breeze-natives" % breezeVersion,
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-remote" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster-tools" % akkaVersion,
  ("com.twitter" %% "chill-akka" % "0.6.0")
    .exclude("com.esotericsoftware.minlog", "minlog") //duplicate dependency from kryo
)