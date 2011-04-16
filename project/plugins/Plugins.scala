import sbt._

class Plugins(info: ProjectInfo) extends PluginDefinition(info) {
  val twttrRepo = "twitter.com" at "http://maven.twttr.com"
  val defaultProject = "com.twitter" % "standard-project" % "0.11.33-NEST"
  val sbtThrift      = "com.twitter" % "sbt-thrift"       % "1.3.12"
}
