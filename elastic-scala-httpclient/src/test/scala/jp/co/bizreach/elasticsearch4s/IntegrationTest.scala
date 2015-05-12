package jp.co.bizreach.elasticsearch4s

import org.scalatest._
import scala.io._
import org.elasticsearch.index.query.QueryBuilders
import IntegrationTest._
import scala.concurrent.ExecutionContext.Implicits.global

class IntegrationTest extends FunSuite {

//  test("Create schema"){
//    val client = HttpUtils.createHttpClient()
//    HttpUtils.post(client, "http://localhost:9200/my_index",
//      Source.fromFile("src/test/resources/schema.json")(Codec("UTF-8")).toString())
//  }
//
//  test("Register documents"){
//    val config = ESConfig("my_index", "my_type")
//    ESClient.using("http://localhost:9200"){ client =>
//      (1 to 100).foreach { num =>
//        client.insert(config, Map(
//          "subject" -> s"[$num]Hello World!",
//          "content" -> "This is a first registration test!"
//        ))
//      }
//    }
//  }
//
//  test("Scroll search"){
//    val config = ESConfig("my_index", "my_type")
//    ESClient.using("http://localhost:9200"){ client =>
//      val result = client.scroll(config){ searcher =>
//        searcher.setQuery(QueryBuilders.matchPhraseQuery("subject", "Hello"))
//      }{ blog: Blog =>
//        blog.subject
//      }
//
//      result.foreach(println)
//    }
//  }
//
//  test("Delete by Query"){
//    val config = ESConfig("my_index", "my_type")
//    ESClient.using("http://localhost:9200"){ client =>
//      val result = client.deleteByQuery(config){ searcher =>
//        searcher.setQuery(QueryBuilders.matchPhraseQuery("subject", "10"))
//      }
//      println(result)
//    }
//  }

//  test("Search by async API"){
//    val config = ESConfig("my_index", "my_type")
//    AsyncESClient.using("http://localhost:9200"){ client =>
//      val f = client.searchAsync(config){ searcher =>
//        searcher.setQuery(QueryBuilders.matchPhraseQuery("subject", "Hello"))
//      }
//      f.foreach { map =>
//        println(map)
//        AsyncESClient.shutdown()
//      }
//    }
//  }

}

object IntegrationTest {
  case class Blog(subject: String, content: String)
}