
scalaVersion := "2.12.1"

crossScalaVersions := Seq("2.12.1", "2.11.8")

lazy val root =
  (project in file("."))
  .dependsOn(httpclient)

lazy val httpclient =
  (project in file("elastic-scala-httpclient"))
 