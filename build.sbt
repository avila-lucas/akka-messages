lazy val akkaHttpVersion = "10.1.10"
lazy val akkaVersion    = "2.5.25"

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization    := "com.omnipresent",
      scalaVersion    := "2.12.8"
    )),
    name := "akka-messages",
    libraryDependencies ++= Seq(
      "org.apache.commons"  %  "commons-lang3"             % "3.9",
      "com.github.dnvriend" %% "akka-persistence-inmemory" % "2.5.15.2",
      "com.typesafe.akka"   %% "akka-multi-node-testkit"   % akkaVersion,
      "com.typesafe.akka"   %% "akka-cluster-sharding"     % akkaVersion,
      "com.typesafe.akka"   %% "akka-cluster-tools"        % akkaVersion,
      "com.typesafe.akka"   %% "akka-persistence"          % akkaVersion,
      "com.typesafe.akka"   %% "akka-http"                 % akkaHttpVersion,
      "com.typesafe.akka"   %% "akka-http-spray-json"      % akkaHttpVersion,
      "com.typesafe.akka"   %% "akka-http-xml"             % akkaHttpVersion,
      "com.typesafe.akka"   %% "akka-stream"               % akkaVersion,

      "com.typesafe.akka"   %% "akka-http-testkit"         % akkaHttpVersion % Test,
      "com.typesafe.akka"   %% "akka-testkit"              % akkaVersion     % Test,
      "com.typesafe.akka"   %% "akka-stream-testkit"       % akkaVersion     % Test,
      "org.scalatest"       %% "scalatest"                 % "3.0.5"         % Test
    )
  )
