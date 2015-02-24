package jp.co.bizreach.elasticsearch4s

import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.core.{JsonGenerator, JsonParser, Version}
import java.util.Locale
import org.joda.time.{DateTime, LocalDate}
import org.joda.time.format.DateTimeFormat
import scala.reflect.ClassTag

private[elasticsearch4s] object JsonUtils {

  def serialize(doc: AnyRef): String = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val testModule = new SimpleModule("MyModule", Version.unknownVersion())
      .addSerializer(classOf[DateTime], new JsonSerializer[DateTime] {
        override def serialize(value: DateTime, generator: JsonGenerator, provider: SerializerProvider): Unit = {
          val formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZoneUTC()
          generator.writeString(formatter.print(value))
        }
      })

    mapper.registerModule(testModule)
    mapper.writeValueAsString(doc)
  }

  def deserialize[T](json: String)(implicit c: ClassTag[T]): T = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper.enable(DeserializationFeature.UNWRAP_SINGLE_VALUE_ARRAYS)

    val testModule = new SimpleModule("MyModule", Version.unknownVersion())
      .addDeserializer(classOf[DateTime], new JsonDeserializer[DateTime](){
        override def deserialize(parser: JsonParser, context: DeserializationContext): DateTime = {
          val formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZoneUTC()
          formatter.parseDateTime(parser.getValueAsString)
        }
      })

    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.registerModule(testModule)
    mapper.readValue(json, c.runtimeClass).asInstanceOf[T]
  }

}
