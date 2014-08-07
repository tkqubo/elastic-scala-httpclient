package jp.co.bizreach.elasticsearch4s.generator

import org.apache.commons.io.{FileUtils, IOUtils}

object ESSchemaCodeGenerator {

  import org.json4s._
  import org.json4s.jackson.JsonMethods._

  implicit val jsonFormats = DefaultFormats

  @SuppressWarnings(Array("unchecked"))
  def generate(): Unit = {
    val config = ESCodegenConfig.load()
    config.jsonFiles.foreach { fileName =>
      val json = parse(read(fileName))

      (json \\ "mappings") match {
        case mappings: JArray => {
          mappings.values.foreach { case schemas: Map[String, _] @unchecked =>
            val schemaInfoList = schemas.map { case (key: String, value: Map[String, _] @unchecked) =>
              val props = value("properties").asInstanceOf[Map[String, _]]
              extractClassInfo(config, toUpperCamel(key), props)
            }

            schemaInfoList.foreach { classInfoList =>
              val sb = new StringBuilder()
              val head = classInfoList.head
              val tail = classInfoList.tail
              sb.append(s"package ${config.packageName}\n")
              sb.append(s"import ${head.name}._\n")
              sb.append("\n")
              sb.append(generateSource(head))
              sb.append("\n")
              sb.append(s"object ${head.name} {\n")
              sb.append("\n")
              sb.append(s"""  val typeName = "${toLowerCamel(head.name)}"\n""")
              sb.append("\n")
              tail.foreach { classInfo =>
                sb.append("  " + generateSource(classInfo))
              }
              sb.append("\n")
              head.props.foreach { prop =>
                sb.append(generateNames("  ", "", prop, tail))
              }
              sb.append("\n")
              sb.append("}")

              val file = new java.io.File(s"${config.outputDir}/${config.packageName.replace('.', '/')}/${head.name}.scala")
              FileUtils.write(file, sb.toString, "UTF-8")
            }
          }
        }
        case _ =>
      }
    }
  }

  private def generateNames(indent: String, prefix: String, prop: PropInfo, classes: List[ClassInfo]): String = {
    val sb = new StringBuilder()
    if(classes.exists(_.name == prop.rawTypeName)){
      if(isValidIdentifier(prop.name)){
        sb.append(s"${indent}val ${prop.name} = ${prop.rawTypeName}Names\n")
      } else {
        sb.append(s"${indent}val `${prop.name}` = ${prop.rawTypeName}Names\n")
      }
      sb.append(s"${indent}object ${prop.rawTypeName}Names {\n")
      classes.find(_.name == prop.rawTypeName).get.props.foreach { child =>
        sb.append(generateNames(indent + "  ", prefix + prop.name + ".", child, classes))
      }
      sb.append(s"""${indent}${indent}override def toString() = "${prefix}${prop.name}"\n""")
      sb.append(s"${indent}}\n")
    } else {
      if(isValidIdentifier(prop.name)){
        sb.append(s"""${indent}val ${prop.name} = "${prefix}${prop.name}"\n""")
      } else {
        sb.append(s"""${indent}val `${prop.name}` = "${prefix}${prop.name}"\n""")
      }
    }
    sb.toString
  }

  // クラスパスからファイルを読み込む
  private def read(path: String): String = IOUtils.toString(Thread.currentThread.getContextClassLoader.getResourceAsStream(path))

  // とりあえず先頭が数値ではじまっているかどうかのみチェック
  private def isValidIdentifier(name: String): Boolean = !name.matches("^[0-9].*")

  private def extractClassInfo(config: ESCodegenConfig, name: String, props: Map[String, _], classes: List[ClassInfo] = Nil): List[ClassInfo] = {
    ClassInfo(config, name, props) :: props.flatMap { case (key: String, value: Map[String, _] @unchecked) =>
      if(value.contains("properties")){
        Some(extractClassInfo(config, toUpperCamel(key), value("properties").asInstanceOf[Map[String, _]]))
      } else {
        None
      }
    }.flatten.toList
  }

  private def generateSource(classInfo: ClassInfo): String = {
    val sb = new StringBuilder()
    if(classInfo.props.length > 22){
      sb.append(s"class ${classInfo.name}(")
      sb.append(classInfo.props.map { propInfo =>
        s"val ${if(isValidIdentifier(propInfo.name)) propInfo.name else s"`${propInfo.name}`"}: ${propInfo.typeName}"
      }.mkString(", "))
      sb.append(")\n")
    } else {
      sb.append(s"case class ${classInfo.name}(")
      sb.append(classInfo.props.map { propInfo =>
        s"${if(isValidIdentifier(propInfo.name)) propInfo.name else s"`${propInfo.name}`"}: ${propInfo.typeName}"
      }.mkString(", "))
      sb.append(")\n")
    }
    sb.toString
  }


  case class ClassInfo(name: String, props: List[PropInfo])

  object ClassInfo {
    def apply(config: ESCodegenConfig, name: String, props: Map[String, _]): ClassInfo = {
      ClassInfo(
        name,
        props.map { case (key: String, value: Map[String, _] @unchecked) => {
          // 型の名前を取得
          val typeName = if(value.contains("type")){
            value("type").toString match {
              case "date" if(value("format").toString == "dateOptionalTime") => "org.joda.time.LocalDateTime"
              case "date" if(value("format").toString.startsWith("yyyy/MM/dd||"))  => "org.joda.time.LocalDate"
              case "long"   => "Long"
              case "string" => "String"
              case x        => x
            }
          } else {
            toUpperCamel(key)
          }
          // 配列の場合はArrayで包む
          val arrayType = if(config.arrayProperties.get(name).exists(_.contains(key))){
            s"Array[${typeName}]"
          } else {
            typeName
          }
          // null可の場合はOptionで包む
          val optionType = if(value.get("null_value") == Some("na")){
            arrayType
          } else {
            s"Option[${arrayType}]"
          }
          PropInfo(key, optionType, typeName)
        }}.toList
      )
    }
  }

  case class PropInfo(name: String, typeName: String, rawTypeName: String)

  def toUpperCamel(name: String) = name.substring(0, 1).toUpperCase + name.substring(1)

  def toLowerCamel(name: String) = name.substring(0, 1).toLowerCase + name.substring(1)

}
