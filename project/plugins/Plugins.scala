import sbt._

class Plugins(info: ProjectInfo) extends PluginDefinition(info) {
  val twttrRepo = "twitter.com" at "http://maven.twttr.com"

  val standardProject = "com.twitter" % "standard-project" % "0.12.6"
  val scrooge         = "com.twitter" % "sbt-scrooge"      % "2.3.1"
}
