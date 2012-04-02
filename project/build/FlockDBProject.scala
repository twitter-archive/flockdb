import sbt._
import Process._
import com.twitter.sbt._

class FlockDBProject(info: ProjectInfo) extends StandardLibraryProject(info)
with CompileThriftScrooge
with DefaultRepos
with SubversionPublisher {

  override def filterScalaJars = false
  val scalaTools = "org.scala-lang" % "scala-compiler" % "2.8.1"

  val gizzard = "com.twitter" % "gizzard"         % "3.0.13" withSources()
  val scrooge = "com.twitter" % "scrooge-runtime" % "1.0.3" withSources()

  val asm       = "asm"                     % "asm"          % "1.5.3" % "test"
  val cglib     = "cglib"                   % "cglib"        % "2.2"   % "test"
  val hamcrest  = "org.hamcrest"            % "hamcrest-all" % "1.1"   % "test"
  val jmock     = "org.jmock"               % "jmock"        % "2.4.0" % "test"
  val objenesis = "org.objenesis"           % "objenesis"    % "1.1"   % "test"
  val specs     = "org.scala-tools.testing" % "specs_2.8.1"  % "1.6.6" % "test"

  override def subversionRepository = Some("https://svn.twitter.biz/maven-public/")
}
