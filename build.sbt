scalaVersion := "2.11.7"
crossScalaVersions := Seq("2.11.7")

lazy val root =
  (project in file("."))
  .dependsOn(httpclient)

lazy val httpclient =
  (project in file("elastic-scala-httpclient"))
 