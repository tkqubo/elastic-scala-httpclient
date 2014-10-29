package jp.co.bizreach.elasticsearch4s.generator

case class ESCodegenConfig(
  outputDir: String = "src/main/scala",
  packageName: String = "models",
  jsonFiles: Seq[String] = Seq("schema.json"),
  classMappings: Map[String, String] = Map.empty,
  typeMappings: Map[String, String] = Map.empty,
  arrayProperties: Map[String, Seq[String]] = Map.empty,
  ignoreProperties: Seq[String] = Nil
)

object ESCodegenConfig {
  import com.typesafe.config.ConfigFactory
  import collection.JavaConverters._
  import java.nio.file.Paths

  def load(): ESCodegenConfig = {
    val config = ConfigFactory.parseFileAnySyntax(Paths.get("es-gen.conf").toFile)
    ESCodegenConfig(
      outputDir       = if(config.hasPath("es-gen.output.dir")) config.getString("es-gen.output.dir") else "src/main/scala",
      packageName     = if(config.hasPath("es-gen.package.name")) config.getString("es-gen.package.name") else "models",
      jsonFiles       = if(config.hasPath("es-gen.json.files")) config.getStringList("es-gen.json.files").asScala.toSeq else Seq("schema.json"),
      classMappings   = if(config.hasPath("es-gen.class.mappings")) config.getStringList("es-gen.class.mappings").asScala.map { x =>
        val array = x.split(":")
        val key   = array(0).trim
        val value = array(1).trim
        key -> value
      }.toMap else Map.empty,
      typeMappings   = if(config.hasPath("es-gen.type.mappings")) config.getStringList("es-gen.type.mappings").asScala.map { x =>
        val array = x.split(":")
        val key   = array(0).trim
        val value = array(1).trim
        key -> value
      }.toMap else Map.empty,
      arrayProperties = if(config.hasPath("es-gen.array.properties")) config.getStringList("es-gen.array.properties").asScala.map { x =>
        val array = x.split(":")
        val key   = array(0).trim
        val value = array(1).trim
        key -> value.split(",").map(_.trim).toSeq
      }.toMap else Map.empty,
      ignoreProperties = if(config.hasPath("es-gen.ignore.properties")) config.getStringList("es-gen.ignore.properties").asScala.toSeq else Nil
    )
  }
}