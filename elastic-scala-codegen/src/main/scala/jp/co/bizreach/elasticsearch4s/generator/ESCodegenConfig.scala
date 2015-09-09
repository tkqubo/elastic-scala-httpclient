package jp.co.bizreach.elasticsearch4s.generator

import java.io.FileInputStream
import org.apache.commons.io.IOUtils
import java.nio.file.Paths

import org.json4s._
import org.json4s.jackson.JsonMethods._

case class ESCodegenConfig(
  outputDir: String = "src/main/scala",
  mappings: Seq[Mapping] = Nil,
  typeMappings: Map[String, String] = Map.empty
)

case class Mapping(
  path: String = "schema.json",
  packageName: String = "models",
  arrayProperties: Seq[String] = Nil,
  ignoreProperties: Seq[String] = Nil,
  className: Option[String]
)

object ESCodegenConfig {
  implicit val jsonFormats = DefaultFormats

  def load(): ESCodegenConfig = {
    val json = IOUtils.toString(new FileInputStream(Paths.get("es-gen.conf").toFile), "UTF-8")
    parse(json).extract[ESCodegenConfig]
  }
}