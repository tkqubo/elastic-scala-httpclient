package jp.co.bizreach.elasticsearch4s

import java.io.File
import java.util

import org.apache.commons.io.FileUtils
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.node.{Node, NodeBuilder}
import org.scalatest._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.io._
import IntegrationTest._
import org.elasticsearch.Version
import org.elasticsearch.env.Environment
import org.elasticsearch.plugin.deletebyquery.DeleteByQueryPlugin
import org.elasticsearch.plugins.Plugin
import org.elasticsearch.script.groovy.GroovyPlugin
import org.codelibs.elasticsearch.sstmpl.ScriptTemplatePlugin

import scala.concurrent.ExecutionContext.Implicits.global

class IntegrationTest extends FunSuite with BeforeAndAfter {

  System.setSecurityManager(null) // to enable execution of script
  var node: Node = null

  /**
   * Extend the Node class to supply plugins on the classpath.
   */
  class EmbeddedNode(environment: Environment, version: Version,
      classpathPlugins: util.Collection[Class[_ <: Plugin]]) extends Node(environment, version, classpathPlugins) {
    def getPlugins(): util.Collection[Class[_ <: Plugin]] = classpathPlugins
    def getVersion(): Version = version
  }

  before {
    val builder = Settings.settingsBuilder
        .put("http.enabled", true)
        .put("http.port", 9200)
        .put("path.data", "elasticsearch-test-data")
        .put("path.home", "src/test/resources")

    val environment = new Environment(builder.build())

    val plugins = new java.util.ArrayList[Class[_ <: Plugin]]()
    plugins.add(classOf[DeleteByQueryPlugin])
    plugins.add(classOf[GroovyPlugin])
    plugins.add(classOf[ScriptTemplatePlugin])

    node = new EmbeddedNode(environment, Version.CURRENT, plugins)
    node.start()

    //node = NodeBuilder.nodeBuilder().settings(builder).node()

    val client = HttpUtils.createHttpClient()
    HttpUtils.post(client, "http://localhost:9200/my_index",
      Source.fromFile("src/test/resources/schema.json")(Codec("UTF-8")).toString())
    client.close()

    ESClient.init()
    AsyncESClient.init()
  }

  after {
    node.close()
    FileUtils.forceDelete(new File("elasticsearch-test-data"))

    ESClient.shutdown()
    AsyncESClient.shutdown()
  }

  test("Insert with id"){
    val config = ESConfig("my_index", "my_type")
    val client = ESClient("http://localhost:9200", true, true)

    client.insert(config, "123", Blog("Hello World!", "This is a first registration test!"))

    client.refresh(config)

    val result = client.find[Blog](config){ searcher =>
      searcher.setQuery(idsQuery("my_type").addIds("123"))
    }

    assert(result == Some("123", Blog("Hello World!", "This is a first registration test!")))
  }

  test("Update partially"){
    val config = ESConfig("my_index", "my_type")
    val client = ESClient("http://localhost:9200", true, true)

    client.insert(config, "1234", Blog("Hello World!", "This is a registered data"))
    client.refresh(config)
    val registrationResult = client.find[Blog](config){ searcher =>
      searcher.setQuery(idsQuery("my_type").addIds("1234"))
    }
    assert(registrationResult == Some("1234", Blog("Hello World!", "This is a registered data")))

    client.updatePartially(config, "1234", BlogContent("This is a updated data"))
    client.refresh(config)
    val updateResult1 = client.find[Blog](config){ searcher =>
      searcher.setQuery(idsQuery("my_type").addIds("1234"))
    }
    assert(updateResult1 == Some("1234", Blog("Hello World!", "This is a updated data")))

    client.updatePartiallyJson(config, "1234", "{ \"subject\": \"Hello Scala!\" }")
    client.refresh(config)
    val updateResult2 = client.find[Blog](config){ searcher =>
      searcher.setQuery(idsQuery("my_type").addIds("1234"))
    }
    assert(updateResult2 == Some("1234", Blog("Hello Scala!", "This is a updated data")))
  }

  test("MultiGet") {
    val config = ESConfig("my_index", "my_type")
    val client = ESClient("http://localhost:9200", true, true)

    val ids = Seq("1001", "1002", "1003")
    ids.foreach(id => client.insert(config, id, Blog(s"[${id}]Hello World!", s"${id}This is a mget test!")))

    client.refresh(config)

    // Check mutiget results
    val mgetResults1 = client.findAllByIdsAsList[Blog](config, ids)
    mgetResults1 foreach {
      case (id, Some(result)) =>
        assert(result.subject == s"[${id}]Hello World!")
        assert(result.content == s"${id}This is a mget test!")
    }
    val mgetResults2 = client.findAllByTypeAndIdAsList[Blog](ids.map(id => config -> id))
    mgetResults2 foreach {
      case (id, Some(result)) =>
        assert(result.subject == s"[${id}]Hello World!")
        assert(result.content == s"${id}This is a mget test!")
    }
  }

  test("Error response"){
    val client = HttpUtils.createHttpClient()
    intercept[HttpResponseException] {
      // Create existing index to cause HttpResponseException
      HttpUtils.post(client, "http://localhost:9200/my_index",
        Source.fromFile("src/test/resources/schema.json")(Codec("UTF-8")).toString())
    }
    client.close()
  }

  test("Error response in async API"){
    val client = HttpUtils.createHttpClient()
    // Create existing index to cause HttpResponseException
    val f = HttpUtils.postAsync(client, "http://localhost:9200/my_index",
      Source.fromFile("src/test/resources/schema.json")(Codec("UTF-8")).toString())

    intercept[HttpResponseException] {
      Await.result(f, Duration.Inf)
    }
    client.close()
  }

  test("Sync client"){
    val config = ESConfig("my_index", "my_type")
    val client = ESClient("http://localhost:9200", true, true)

    // Register 100 docs
    (1 to 100).foreach { num =>
      client.insert(config, Map(
        "subject" -> s"[$num]Hello World!",
        "content" -> "This is a first registration test!"
      ))
    }
    client.refresh(config)

    // Check doc count
    val count1 = client.countAsInt(config){ searcher =>
      searcher.setQuery(matchAllQuery)
    }
    assert(count1 == 100)

    // Check doc exists
    val result1 = client.find[Blog](config){ searcher =>
      searcher.setQuery(matchPhraseQuery("subject", "10"))
    }
    assert(result1.get._2.subject == "[10]Hello World!")
    assert(result1.get._2.content == "This is a first registration test!")

    // Delete 1 doc
//    client.delete(config, result1.get._1)
    client.deleteByQuery(config){ searcher =>
      searcher.setQuery(matchPhraseQuery("subject", "10"))
    }
    client.refresh(config)

    // Check doc doesn't exist
    val result2 = client.find[Blog](config){ searcher =>
      searcher.setQuery(matchPhraseQuery("subject", "10"))
    }
    assert(result2.isEmpty)

    // Check doc count
    val count2 = client.countAsInt(config){ searcher =>
      searcher.setQuery(matchAllQuery)
    }
    assert(count2 == 99)

    // Scroll search
    val sum = client.scroll[Blog, Int](config){ searcher =>
      searcher.setQuery(matchPhraseQuery("subject", "Hello"))
    }{ case (id, blog) =>
      assert(blog.content == "This is a first registration test!")
      1
    }.sum
    assert(sum == 99)

    // Count by template
    val count3 = client.countByTemplateAsInt(config)(
      lang = "groovy",
      template = "test_script",
      params = Map("subjectValue" -> "Hello")
    )
    assert(count3 === 99)
  }

  test("index exist"){
    val config = ESConfig("my_index")
    val client = AsyncESClient("http://localhost:9200")

    for {
      _ <- client.createOrUpdateIndexAsync(config, Map())
      res <- client.indexExistAsync(config)
    } yield {
      assert(res.isRight)
    }
  }

  test("index not exist"){
    val config = ESConfig("my_not_existing_index")
    val client = AsyncESClient("http://localhost:9200")

    for {
      res <- client.indexExistAsync(config)
    } yield {
      assert(res.isLeft)
    }
  }

  test("index not exist sync"){
    val config = ESConfig("my_not_existing_index")
    val client = ESClient("http://localhost:9200")

    val res = client.indexExist(config)
    assert(res.isLeft)
  }

  test("create index with settings"){
    val config = ESConfig("my_index", "my_type")
    val client = AsyncESClient("http://localhost:9200")
    val settings = Map(
      "mappings" -> Map(
        "type_one" -> Map(
          "properties" -> Map(
            "text" -> Map(
              "type" -> "string",
              "analyzer" -> "standard"
            )
          )
        ),
        "type_two" -> Map(
          "properties" -> Map(
            "text" -> Map(
              "type" -> "string",
              "analyzer" -> "standard"
            )
          )
        )
      )
    )

    client.createOrUpdateIndexAsync(config, settings).map { result =>
      assert(result.isRight)
    }

  }

  test("Async client"){
    val config = ESConfig("my_index", "my_type")
    val client = AsyncESClient("http://localhost:9200")

    val seqf = (1 to 100).map { num =>
      client.insertAsync(config, Map(
        "subject" -> s"[$num]Hello World!",
        "content" -> "This is a first registration test!"
      ))
    }

    val f = for {
      _ <- Future.sequence(seqf)
      _ <- client.refreshAsync(config)
      count <- client.countAsIntAsync(config) { searcher =>
        searcher.setQuery(matchAllQuery)
      }
    } yield count

    val count = Await.result(f, Duration.Inf)
    assert(count == 100)
  }

}

object IntegrationTest {
  case class Blog(subject: String, content: String)
  case class BlogContent(content: String)
}
