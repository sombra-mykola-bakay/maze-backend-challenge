lazy val akkaHttpVersion = "10.1.5"
lazy val akkaVersion    = "2.5.16"

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.sombrainc",
      scalaVersion := "2.12.6",
      name := "maze-backend-challenge"
    )),
    name := "maze-backend-challenge",
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-http"            % akkaHttpVersion,
      "com.typesafe.akka" %% "akka-stream"          % akkaVersion,
      "com.typesafe.akka" %% "akka-http-jackson"    % akkaHttpVersion,
      "org.slf4j" % "slf4j-api" % "1.7.5",
      "org.slf4j" % "slf4j-simple" % "1.7.5",

      "com.typesafe.akka" %% "akka-http-testkit" % akkaHttpVersion % Test,
      "junit"              % "junit"             % "4.12"          % Test,
      "com.novocode"       % "junit-interface"   % "0.10"          % Test
    ),

    testOptions += Tests.Argument(TestFrameworks.JUnit, "-v")
  )
