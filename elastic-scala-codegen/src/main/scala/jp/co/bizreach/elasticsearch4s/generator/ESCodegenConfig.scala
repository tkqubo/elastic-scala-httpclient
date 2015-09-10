package jp.co.bizreach.elasticsearch4s.generator

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import java.nio.file.Paths
import Utils._

import org.json4s._

case class ESCodegenConfig(
  outputDir: Option[String],
  mappings: Seq[Mapping],
  typeMappings: Option[Map[String, String]]
)

case class Mapping(
  path: String,
  packageName: String,
  arrayProperties: Option[Seq[String]],
  ignoreProperties: Option[Seq[String]],
  className: Option[String]
)

object ESCodegenConfig {
  implicit val jsonFormats = DefaultFormats

  def load(): ESCodegenConfig = {
    val json = read(Paths.get("es-codegen.json").toFile)

    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper.readValue(json, classOf[ESCodegenConfig])
  }
}