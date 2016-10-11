name := "elastic-scala-httpclient"

organization := "jp.co.bizreach"

version := "2.0.3"

scalaVersion := "2.11.7"

libraryDependencies <++= scalaVersion(sv => Seq(
  "jp.co.bizreach"               %  "elasticsearch"         % "2.3.5",
  "org.slf4j"                    %  "slf4j-api"             % "1.7.7",
  "joda-time"                    %  "joda-time"             % "2.2",
  "org.joda"                     %  "joda-convert"          % "1.6",
  "commons-io"                   %  "commons-io"            % "2.4",
  "com.ning"                     %  "async-http-client"     % "1.9.38",
  "com.fasterxml.jackson.module" %% "jackson-module-scala"  % "2.7.2",
  "org.elasticsearch.plugin"     %  "delete-by-query"       % "2.3.5" % "test",
  "org.elasticsearch.module"     %  "lang-groovy"           % "2.3.5" % "test",
  "org.codelibs"                 %  "elasticsearch-sstmpl"  % "2.3.1" % "test",
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
      <developer>
        <id>hajimeni</id>
        <name>Hajime Nishiyama</name>
        <email>nishiyama_at_bizreach.co.jp</email>
        <timezone>+9</timezone>
      </developer>
      <developer>
        <id>saito400</id>
        <name>Kenichi Saito</name>
        <email>kenichi.saito_at_bizreach.co.jp</email>
        <timezone>+9</timezone>
      </developer>
      <developer>
        <id>shimamoto</id>
        <name>Takako Shimamoto</name>
        <email>takako.shimamoto_at_bizreach.co.jp</email>
        <timezone>+9</timezone>
      </developer>
    </developers>)
