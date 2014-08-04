package jp.co.bizreach.elasticsearch4s

import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.core.{JsonGenerator, JsonParser, Version}
import java.util.Locale
import org.joda.time.{LocalDateTime, LocalDate}
import org.joda.time.format.DateTimeFormat

private[elasticsearch4s] object JsonUtils {

  def serialize(doc: AnyRef): String = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val testModule = new SimpleModule("MyModule", Version.unknownVersion())
      .addSerializer(classOf[LocalDate], new JsonSerializer[LocalDate] {
        override def serialize(value: LocalDate, generator: JsonGenerator, provider: SerializerProvider): Unit = {
          generator.writeString(value.toString("yyyy/MM/dd", Locale.ENGLISH))
        }
      })
      .addSerializer(classOf[LocalDateTime], new JsonSerializer[LocalDateTime] {
        override def serialize(value: LocalDateTime, generator: JsonGenerator, provider: SerializerProvider): Unit = {
          generator.writeString(value.toString("yyyy-MM-dd'T'HH:mm:ss.SSSZ", Locale.ENGLISH))
        }
      })

    mapper.registerModule(testModule)
    mapper.writeValueAsString(doc)
  }

  def deserialize[T](json: String, clazz: Class[T]): T = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    val testModule = new SimpleModule("MyModule", Version.unknownVersion())
      .addDeserializer(classOf[LocalDate], new JsonDeserializer[LocalDate](){
        override def deserialize(parser: JsonParser, context: DeserializationContext): LocalDate = {
          DateTimeFormat.forPattern("yyyy/MM/dd").parseLocalDate(parser.getValueAsString)
        }
      })
      .addDeserializer(classOf[LocalDateTime], new JsonDeserializer[LocalDateTime](){
        override def deserialize(parser: JsonParser, context: DeserializationContext): LocalDateTime = {
          DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ").parseLocalDateTime(parser.getValueAsString)
        }
      })

    mapper.registerModule(testModule)
    mapper.readValue(json, clazz)
  }

}
