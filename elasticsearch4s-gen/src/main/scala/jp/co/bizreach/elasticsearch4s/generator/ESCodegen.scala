package jp.co.bizreach.elasticsearch4s.generator

import sbt._

object ESCodegen extends AutoPlugin {

  override def trigger = allRequirements

  object autoImport {
    lazy val codegen = TaskKey[Unit]("gen-es")
  }

  import autoImport._

  override def projectSettings = Seq(
    codegen := generate.value
  )

  private def generate: Def.Initialize[Task[Unit]] = Def.task {
    ESSchemaCodeGenerator.generate()
  }

}
