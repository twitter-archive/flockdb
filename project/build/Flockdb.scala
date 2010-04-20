import sbt._
import Process._

class FlockdbProject(info: ProjectInfo) extends DefaultProject(info) {
  override def dependencyPath = "lib"
  override def disableCrossPaths = true

  override def managedDependencyPath = ".ivy2cache"

  val jbossRepository   = "jboss" at "http://repository.jboss.org/maven2/"
  val lagRepository     = "lag.net" at "http://www.lag.net/repo/"
  val twitterRepository = "twitter.com" at "http://www.lag.net/nest/"
  val ibiblioRepository = "ibiblio" at "http://mirrors.ibiblio.org/pub/mirrors/maven2/"

  val asm       = "asm" % "asm" %  "1.5.3"
  val cglib     = "cglib" % "cglib" % "2.1_3"
  val configgy  = "net.lag" % "configgy" % "1.4.3"
  val dbcp      = "commons-dbcp" % "commons-dbcp" % "1.2.2"
  val gizzard   = "com.twitter" % "gizzard" % "1.0.5"
  val hamcrest  = "org.hamcrest" % "hamcrest-all" % "1.1"
  val jmock     = "org.jmock" % "jmock" % "2.4.0"
  val kestrel   = "net.lag" % "kestrel" % "1.2"
  val mysqljdbc = "mysql" % "mysql-connector-java" % "5.1.6"
  val objenesis = "org.objenesis" % "objenesis" % "1.1"
  val ostrich   = "com.twitter" % "ostrich" % "1.1.14"
  val pool      = "commons-pool" % "commons-pool" % "1.3"
  val querulous = "com.twitter" % "querulous" % "1.1.7"
  val results   = "com.twitter" % "results" % "1.0"
  val slf4j     = "org.slf4j" % "slf4j-jdk14" % "1.5.2"
  val slf4jApi  = "org.slf4j" % "slf4j-api" % "1.5.2"
  val smile     = "net.lag" % "smile" % "0.8.11"
  val specs     = "org.scala-tools.testing" % "specs" % "1.6.1"
  val thrift    = "thrift" % "libthrift" % "0.2.0"
  val xrayspecs = "com.twitter" % "xrayspecs" % "1.0.7"

  val log4j = "log4j" % "log4j" % "1.2.12"

  def thriftCompileTask(lang: String, paths: PathFinder) = dynamic({
    def thriftCompile = {
      (paths.getPaths.map { path =>
        execTask { "thrift --gen %s -o %s %s".format(lang, outputPath.absolutePath, path) }
      } map ( _.run ) flatMap ( _.toList ) toSeq).firstOption
    }

    val thriftCompilePath = (outputPath / ("gen-"+lang) ##)
    if (thriftCompilePath.exists) {
      val thriftCompiledFiles = thriftCompilePath ** "*.java"
      fileTask(thriftCompiledFiles.get from paths) {
        thriftCompile
      }
    } else {
      task { thriftCompile }
    }
  })

  outputPath.asFile.mkdir()

  val mainThriftPath = (mainSourcePath / "thrift" ##)
  lazy val thriftClean = cleanTask(outputPath / "gen-java") describedAs("clean Thrift")
  lazy val thriftJava = thriftCompileTask("java",  mainThriftPath ** "*.thrift") describedAs ("compile java thrift")
  override def cleanAction = super.cleanAction dependsOn(thriftClean)
  override def compileAction = super.compileAction dependsOn(thriftJava)
  override def compileOrder = CompileOrder.JavaThenScala
  override val mainJavaSourcePath = outputPath / "gen-java"
}

