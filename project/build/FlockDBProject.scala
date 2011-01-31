import sbt._
import Process._
import com.twitter.sbt._

class FlockDBProject(info: ProjectInfo) extends StandardProject(info) with SubversionPublisher with InlineDependencies {

  override def filterScalaJars = false

  val scalaTools = "org.scala-lang" % "scala-compiler" % "2.7.7"

  val gizzard =  "com.twitter" % "gizzard" % "1.7.4"
  val asm       = "asm" % "asm" %  "1.5.3" % "test"
  val cglib     = "cglib" % "cglib" % "2.1_3" % "test"
  val hamcrest  = "org.hamcrest" % "hamcrest-all" % "1.1" % "test"
  val jmock     = "org.jmock" % "jmock" % "2.4.0" % "test"
  val objenesis = "org.objenesis" % "objenesis" % "1.1" % "test"
  val specs     = "org.scala-tools.testing" % "specs" % "1.6.2.1" % "test"

  override def subversionRepository = Some("http://svn.local.twitter.com/maven-public/")
}
