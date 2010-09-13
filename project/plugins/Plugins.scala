import sbt._

class Plugins(info: ProjectInfo) extends PluginDefinition(info) {
  val scalaTools = "scala-tools.org" at "http://scala-tools.org/repo-releases/"
  val lagNet = "twitter.com" at "http://www.lag.net/repo/"
  val defaultProject = "com.twitter" % "standard-project" % "0.7.1"
}
