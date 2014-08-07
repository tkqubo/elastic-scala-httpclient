package jp.co.bizreach.elasticsearch4s.generator

case class ESCodegenConfig(
  outputDir: String = "src/main/scala",
  packageName: String = "models",
  jsonFiles: Seq[String] = Seq("schema.json"),
  arrayProperties: Map[String, Seq[String]] = Map.empty[String, Seq[String]]
)

object ESCodegenConfig {
  import com.typesafe.config.ConfigFactory
  import collection.JavaConverters._
  import java.nio.file.Paths

  def load(): ESCodegenConfig = {
    val config = ConfigFactory.parseFileAnySyntax(Paths.get("es-gen.conf").toFile)
    ESCodegenConfig(
      outputDir       = config.getString("es-gen.output.dir"),
      packageName     = config.getString("es-gen.package.name"),
      jsonFiles       = config.getStringList("es-gen.json.files").asScala.toSeq,
      arrayProperties = config.getStringList("es-gen.array.properties").asScala.map { x =>
        val array = x.split(":")
        val key   = array(0).trim
        val value = array(1).trim
        key -> value.split(",").map(_.trim).toSeq
      }.toMap
    )
  }
}