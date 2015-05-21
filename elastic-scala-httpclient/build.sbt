name := "elastic-scala-httpclient"

organization := "jp.co.bizreach"

version := "1.0.2"

scalaVersion := "2.11.2"

crossScalaVersions := Seq("2.10.4", "2.11.2")

libraryDependencies <++= scalaVersion(sv => Seq(
  "org.elasticsearch"            %  "elasticsearch"         % "1.1.0",
  "org.slf4j"                    %  "slf4j-api"             % "1.7.7",
  "joda-time"                    %  "joda-time"             % "2.2",
  "org.joda"                     %  "joda-convert"          % "1.6",
  "commons-io"                   %  "commons-io"            % "2.4",
  "com.ning"                     %  "async-http-client"     % "1.8.15",
  "com.fasterxml.jackson.module" %% "jackson-module-scala"  % (if(sv.startsWith("2.10")) "2.3.3" else "2.4.1"),
  "org.scalatest"                %% "scalatest"             % "2.2.1" % "test"
))

publishMavenStyle := true

publishTo <<= version { (v: String) =>
  val nexus = "https://oss.sonatype.org/"
  if (v.trim.endsWith("SNAPSHOT")) Some("snapshots" at nexus + "content/repositories/snapshots")
  else                             Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

scalacOptions := Seq("-deprecation")

javacOptions in compile ++= Seq("-source","1.7", "-target","1.7", "-encoding","UTF-8")

publishArtifact in Test := false

pomIncludeRepository := { _ => false }

pomExtra := (
  <url>https://github.com/bizreach/elasticsearch4s</url>
    <licenses>
      <license>
        <name>The Apache Software License, Version 2.0</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
      </license>
    </licenses>
    <scm>
      <url>https://github.com/bizreach/elasticsearch4s</url>
      <connection>scm:git:https://github.com/bizreach/elasticsearch4s.git</connection>
    </scm>
    <developers>
      <developer>
        <id>takezoe</id>
        <name>Naoki Takezoe</name>
        <email>naoki.takezoe_at_bizreach.co.jp</email>
        <timezone>+9</timezone>
      </developer>
    </developers>)
