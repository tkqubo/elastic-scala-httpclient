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
      outputDir       = if(config.hasPath("es-codegen.output.dir"))     config.getString("es-codegen.output.dir")   else "src/main/scala",
      packageName     = if(config.hasPath("es-codegen.package.name"))   config.getString("es-codegen.package.name") else "models",
      jsonFiles       = if(config.hasPath("es-codegen.json.files"))     config.getStringList("es-codegen.json.files").asScala.toSeq else Seq("schema.json"),
      classMappings   = if(config.hasPath("es-codegen.class.mappings")) config.getStringList("es-codegen.class.mappings").asScala.map { x =>
        val array = x.split(":")
        val key   = array(0).trim
        val value = array(1).trim
        key -> value
      }.toMap else Map.empty,
      typeMappings   = if(config.hasPath("es-codegen.type.mappings")) config.getStringList("es-codegen.type.mappings").asScala.map { x =>
        val array = x.split(":")
        val key   = array(0).trim
        val value = array(1).trim
        key -> value
      }.toMap else Map.empty,
      arrayProperties = if(config.hasPath("es-codegen.array.properties")) config.getStringList("es-codegen.array.properties").asScala.map { x =>
        val array = x.split(":")
        val key   = array(0).trim
        val value = array(1).trim
        key -> value.split(",").map(_.trim).toSeq
      }.toMap else Map.empty,
      ignoreProperties = if(config.hasPath("es-codegen.ignore.properties")) config.getStringList("es-codegen.ignore.properties").asScala.toSeq else Nil
    )
  }
}