name := """Akka Streams Test GroupStage"""

moduleName := "test-groupstage-akka-streams"

organization  := "de.flwi"

version := "0.0.1"

scalaVersion := "2.11.7"

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

libraryDependencies ++= {
  val akkaStreamVersion = "2.0-M1"
  val akkaVersion = "2.4.0"

  Seq(
//    "com.typesafe.akka" %% "akka-actor"                           % akkaVersion,
    "com.typesafe.akka" %% "akka-stream-experimental"             % akkaStreamVersion,
    "com.typesafe.akka" %% "akka-stream-testkit-experimental"     % akkaStreamVersion,
//    "com.typesafe.akka" %% "akka-http-experimental"               % akkaStreamVersion,
//    "com.typesafe.akka" %% "akka-http-core-experimental"          % akkaStreamVersion,
//    "com.typesafe.akka" %% "akka-http-testkit-experimental"       % akkaStreamVersion,
    "org.scalatest"     %% "scalatest"                            % "2.2.5" % "test"
//    "com.typesafe.akka" %% "akka-testkit"                         % akkaVersion % "test"
  )
}