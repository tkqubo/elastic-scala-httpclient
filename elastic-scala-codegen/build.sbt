sbtPlugin := true

name := "elastic-scala-codegen"

organization := "jp.co.bizreach"

version := "1.0.5-SNAPSHOT"

scalaVersion := "2.10.3"

libraryDependencies ++= Seq(
  "commons-io" % "commons-io" % "2.5",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.8.4"
)

publishMavenStyle := true

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (version.value.trim.endsWith("SNAPSHOT"))
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

scalacOptions := Seq("-deprecation")

javacOptions in compile ++= Seq("-source","1.7", "-target","1.7", "-encoding","UTF-8")

publishArtifact in Test := false

pomIncludeRepository := { _ => false }

pomExtra := (
  <url>https://github.com/bizreach/elastic-scala-httpclient</url>
    <licenses>
      <license>
        <name>The Apache Software License, Version 2.0</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
      </license>
    </licenses>
    <scm>
      <url>https://github.com/bizreach/elastic-scala-httpclient</url>
      <connection>scm:git:https://github.com/bizreach/elastic-scala-httpclient.git</connection>
    </scm>
    <developers>
      <developer>
        <id>takezoe</id>
        <name>Naoki Takezoe</name>
        <email>naoki.takezoe_at_bizreach.co.jp</email>
        <timezone>+9</timezone>
      </developer>
    </developers>)
