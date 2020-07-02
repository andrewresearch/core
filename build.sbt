name := "core"

organization := "io.nlytx"

version := "0.2.10"

scalaVersion := "2.11.12"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.6",
  "org.apache.spark" %% "spark-sql" % "2.4.6",
  "org.apache.spark" %% "spark-mllib" % "2.4.6",
  "com.johnsnowlabs.nlp" %% "spark-nlp" % "2.5.2"
)

publishMavenStyle := true
githubOwner := "andrewresearch"
githubRepository := "core"
githubTokenSource := TokenSource.GitConfig("github.token")
