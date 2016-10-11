package jp.co.bizreach.elasticsearch4s.generator

import java.io.File
import Utils._

object ESSchemaCodeGenerator {

  @SuppressWarnings(Array("unchecked"))
  def generate(): Unit = {
    val config = ESCodegenConfig.load()
    config.mappings.foreach { mapping =>
      val file = new File(mapping.path)
      val map = parseJson[Map[String, Any]](read(file))

      map.get("mappings").foreach { case values: Map[String, Map[String, _]] @unchecked =>
        val schemaInfoList = values.map { case (key, value) =>
          val props = value("properties").asInstanceOf[Map[String, _]]
          extractClassInfo(mapping, config, key, props, true)
        }

        schemaInfoList.foreach { classInfoList =>
          val sb = new StringBuilder()
          val head = classInfoList.head
          val tail = classInfoList.tail

          sb.append(s"package ${mapping.packageName}\n")
          sb.append(s"import ${head.name}._\n")
          sb.append("\n")
          sb.append(generateSource("", head))
          sb.append("\n")
          sb.append(s"object ${head.name} {\n")
          sb.append("\n")
          sb.append(s"""  val typeName = "${toLowerCamel(head.name)}"\n""")
          sb.append("\n")
          tail.foreach { classInfo =>
            sb.append(generateSource("  ", classInfo))
          }
          if(tail.exists(x => x.props.exists(_.rawTypeName == "GeoPoint"))){
            sb.append(generateSource("  ", ClassInfo("GeoPoint", List(PropInfo("lat", "Double", "Double"), PropInfo("lon", "Double", "Double")))))
          }
          sb.append("\n")

          sb.append("  implicit val reflectiveCalls = scala.language.reflectiveCalls\n")
          sb.append("  implicit val implicitConversions = scala.language.implicitConversions\n")
          sb.append("\n")
          sb.append("  implicit def name2string(name: Name): String = name.toString()\n")
          sb.append("\n")
          sb.append("  case class Name(name: String){\n")
          sb.append("    lazy val key = if(name.contains(\".\")) name.split(\"\\\\.\").last else name\n")
          sb.append("    override def toString(): String = name\n")
          sb.append("  }\n")

          head.props.foreach { prop =>
            sb.append(generateNames("  ", "", prop, tail))
          }
          sb.append("\n")
          sb.append("}")

          val outputDir = config.outputDir.getOrElse("src/main/scala")
          val file = new java.io.File(s"${outputDir}/${mapping.packageName.replace('.', '/')}/${head.name}.scala")
          write(file, sb.toString)
        }
      }
    }
  }

  private def generateNames(indent: String, prefix: String, prop: PropInfo, classes: List[ClassInfo]): String = {
    val sb = new StringBuilder()
    if(classes.exists(_.name == prop.rawTypeName)){
      if(isValidIdentifier(prop.name)){
        sb.append(s"""${indent}val ${prop.name} = new Name("${prefix}${prop.name}"){""")
      } else {
        sb.append(s"""${indent}val `${prop.name}` = new Name("${prefix}${prop.name}"){""")
      }
      sb.append("\n")
      classes.find(_.name == prop.rawTypeName).get.props.foreach { child =>
        sb.append(generateNames(indent + "  ", prefix + prop.name + ".", child, classes))
      }
      sb.append(s"${indent}}\n")
    } else {
      if(isValidIdentifier(prop.name)){
        sb.append(s"""${indent}val ${prop.name} = Name("${prefix}${prop.name}")\n""")
      } else {
        sb.append(s"""${indent}val `${prop.name}` = Name("${prefix}${prop.name}")\n""")
      }
    }
    sb.toString
  }

  case class Name(name: String)

  private def isValidIdentifier(name: String): Boolean = !name.matches("^[0-9].*")

  private def extractClassInfo(mapping: Mapping, config: ESCodegenConfig, key: String, props: Map[String, _], topLevel: Boolean): List[ClassInfo] = {

    val name = if(topLevel) mapping.className.getOrElse(toUpperCamel(key)) else toUpperCamel(key)

    ClassInfo(mapping, config, name, props) :: props.flatMap { case (key: String, value: Map[String, _] @unchecked) =>
      if(value.contains("properties")){
        Some(extractClassInfo(mapping, config, key, value("properties").asInstanceOf[Map[String, _]], false))
      } else {
        None
      }
    }.flatten.toSeq.distinct.toList
  }

  private def generateSource(indent: String, classInfo: ClassInfo): String = {
    val sb = new StringBuilder()
    if(classInfo.props.length > 22){
      sb.append(indent + s"class ${classInfo.name}(\n")
      sb.append(classInfo.props.map { propInfo =>
        indent + s"  val ${if(isValidIdentifier(propInfo.name)) propInfo.name else s"`${propInfo.name}`"}: ${propInfo.typeName}"
      }.mkString("", ", \n", "\n"))
      sb.append(indent + ")\n")
    } else {
      sb.append(indent + s"case class ${classInfo.name}(\n")
      sb.append(classInfo.props.map { propInfo =>
         indent+ s"  ${if(isValidIdentifier(propInfo.name)) propInfo.name else s"`${propInfo.name}`"}: ${propInfo.typeName}"
      }.mkString("", ", \n", "\n"))
      sb.append(indent + ")\n")
    }
    sb.toString
  }


  case class ClassInfo(name: String, props: List[PropInfo])

  object ClassInfo {
    def apply(mapping: Mapping, config: ESCodegenConfig, name: String, props: Map[String, _]): ClassInfo = {
      ClassInfo(
        name,
        props
          .filter { case (key: String, value: Map[String, _] @unchecked) =>
            !mapping.ignoreProperties.exists(_.contains(key))
          }
          .map { case (key: String, value: Map[String, _] @unchecked) => {
            val typeName = if(value.contains("type")){
              value("type").toString match {
                case "date" if(value("format").toString == "dateOptionalTime")   => "org.joda.time.DateTime"
                case "date" if(value("format").toString == "date_optional_time") => "org.joda.time.DateTime"
                //case "date" if(value("format").toString.startsWith("yyyy/MM/dd||"))  => "org.joda.time.LocalDate"
                case "long"      => "Long"
                case "double"    => "Double"
                case "string"    => "String"
                case "boolean"   => "Boolean"
                case "geo_point" => "GeoPoint"
                case x           => config.typeMappings.map(_.getOrElse(x, x)).getOrElse(x)
              }
            } else {
              toUpperCamel(key)
            }

            val arrayType = if(mapping.arrayProperties.exists(_.contains(key))){
              s"Array[${typeName}]"
            } else {
              typeName
            }

            val optionType = if(value.get("null_value").isDefined){
              arrayType
            } else {
              s"Option[${arrayType}]"
            }
            PropInfo(key, optionType, typeName)
          }
        }.toList
      )
    }
  }

  case class PropInfo(name: String, typeName: String, rawTypeName: String)

}
