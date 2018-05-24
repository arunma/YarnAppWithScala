name := "SampleYarnApp"

version := "0.1"

scalaVersion := "2.11.12"


libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-yarn-client" % "2.7.3" % Provided,
  "org.apache.hadoop" % "hadoop-common" % "2.7.3" % Provided,
  "org.apache.hadoop" % "hadoop-yarn-api" % "2.7.3" % Provided,
  "org.apache.hadoop" % "hadoop-hdfs" % "2.7.3" % Provided
    )


    assemblyMergeStrategy in assembly := {
    case PathList ("org", "aopalliance", xs@_*) => MergeStrategy.last
    case PathList ("javax", "inject", xs@_*) => MergeStrategy.last
    case PathList ("javax", "servlet", xs@_*) => MergeStrategy.last
    case PathList ("javax", "activation", xs@_*) => MergeStrategy.last
    case PathList ("org", "apache", xs@_*) => MergeStrategy.last
    case PathList ("com", "google", xs@_*) => MergeStrategy.last
    case PathList ("com", "esotericsoftware", xs@_*) => MergeStrategy.last
    case PathList ("com", "codahale", xs@_*) => MergeStrategy.last
    case PathList ("com", "yammer", xs@_*) => MergeStrategy.last
    case "about.html" => MergeStrategy.rename
    case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
    case "META-INF/mailcap" => MergeStrategy.last
    case "META-INF/mimetypes.default" => MergeStrategy.last
    case "plugin.properties" => MergeStrategy.last
    case "log4j.properties" => MergeStrategy.last
    case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy (x)
    }

    run in Compile := Defaults.runTask (fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run) ).evaluated