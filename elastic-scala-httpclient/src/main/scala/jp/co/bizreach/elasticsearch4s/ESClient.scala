package jp.co.bizreach.elasticsearch4s

import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.search.SearchRequestBuilder
import ESClient._
import org.slf4j.LoggerFactory
import org.elasticsearch.client.support.AbstractClient
import scala.reflect.ClassTag
import scala.annotation.tailrec
import com.ning.http.client.{AsyncHttpClient, AsyncHttpClientConfig, Realm}

/**
 * Helper for accessing to Elasticsearch.
 */
object ESClient {

  val logger = LoggerFactory.getLogger(classOf[ESClient])

  /**
   * This is the entry point of processing using ElasticSearch.
   * Give ESConfig and your function which takes ESSearchHelper as an argument.
   */
  def using[T](url: String)(f: ESClient => T): T = {
    val httpClient: AsyncHttpClient =  Option(new java.net.URL(url).getUserInfo) match {
      case Some(x) => {
        val userInfo = x.split(":")
        val realm = new Realm.RealmBuilder()
          .setPrincipal(userInfo(0))
          .setPassword(userInfo.lift(1).getOrElse(""))
          .build()
        HttpUtils.createHttpClient(new AsyncHttpClientConfig.Builder().setRealm(realm).build())
      }
      case _ => HttpUtils.createHttpClient()
    }
    val client = new ESClient(new QueryBuilderClient(), httpClient, url)
    try {
      f(client)
    } finally {
      client.release()
    }
  }
}

class ESClient(queryClient: AbstractClient, httpClient: AsyncHttpClient, url: String) {

  def insertJson(config: ESConfig, json: String): Either[Map[String, Any], Map[String, Any]] = {
    logger.debug(s"insertJson:\n${json}")
    logger.debug(s"insertRequest:\n${json}")

    val resultJson = HttpUtils.post(httpClient, config.url(url), json)
    val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
    map.get("error").map { case message: String => Left(map) }.getOrElse(Right(map))
  }

  def insert(config: ESConfig, entity: AnyRef):  Either[Map[String, Any], Map[String, Any]] = {
    insertJson(config, JsonUtils.serialize(entity))
  }

  def updateJson(config: ESConfig, id: String, json: String): Either[Map[String, Any], Map[String, Any]] = {
    logger.debug(s"updateJson:\n${json}")
    logger.debug(s"updateRequest:\n${json}")

    val resultJson = HttpUtils.put(httpClient, config.url(url) + "/" + id, json)
    val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
    map.get("error").map { case message: String => Left(map) }.getOrElse(Right(map))
  }

  def update(config: ESConfig, id: String, entity: AnyRef): Either[Map[String, Any], Map[String, Any]] = {
    updateJson(config, id, JsonUtils.serialize(entity))
  }

  def delete(config: ESConfig, id: String): Either[Map[String, Any], Map[String, Any]] = {
    logger.debug(s"delete id:\n${id}")

    val resultJson = HttpUtils.delete(httpClient, config.url(url) + "/" + id)
    val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
    map.get("error").map { case message: String => Left(map) }.getOrElse(Right(map))
  }

  def deleteByQuery(config: ESConfig)(f: SearchRequestBuilder => Unit): Either[Map[String, Any], Map[String, Any]] = {
    logger.debug("******** ESConfig:" + config.toString)
    val searcher = queryClient.prepareSearch(config.indexName)
    config.typeName.foreach(x => searcher.setTypes(x))
    f(searcher)
    logger.debug(s"deleteByQuery:${searcher.toString}")

    val resultJson = HttpUtils.delete(httpClient, config.url(url) + "/_query", searcher.toString)
    val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
    map.get("error").map { case message: String => Left(map) }.getOrElse(Right(map))
  }

  def count(config: ESConfig)(f: SearchRequestBuilder => Unit): Either[Map[String, Any], Map[String, Any]] = {
    logger.debug("******** ESConfig:" + config.toString)
    val searcher = queryClient.prepareSearch(config.indexName)
    config.typeName.foreach(x => searcher.setTypes(x))
    f(searcher)
    logger.debug(s"countRequest:${searcher.toString}")

    val resultJson = HttpUtils.post(httpClient, config.preferenceUrl(url, "_count"), searcher.toString)
    val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
    map.get("error").map { case message: String => Left(map) }.getOrElse(Right(map))
  }

  def countAsInt(config: ESConfig)(f: SearchRequestBuilder => Unit): Int = {
    count(config)(f) match {
      case Left(x)  => throw new RuntimeException(x("error").toString)
      case Right(x) => x("count").asInstanceOf[Int]
    }
  }

  def search(config: ESConfig)(f: SearchRequestBuilder => Unit): Either[Map[String, Any], Map[String, Any]] = {
    logger.debug("******** ESConfig:" + config.toString)
    val searcher = queryClient.prepareSearch(config.indexName)
    config.typeName.foreach(x => searcher.setTypes(x))
    f(searcher)
    logger.debug(s"searchRequest:${searcher.toString}")

    val resultJson = HttpUtils.post(httpClient, config.preferenceUrl(url, "_search"), searcher.toString)
    val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
    map.get("error").map { case message: String => Left(map) }.getOrElse(Right(map))
  }

  def searchAll(config: ESConfig)(f: SearchRequestBuilder => Unit): Either[Map[String, Any], Map[String, Any]] = {
    count(config)(f) match {
      case Left(x)  => Left(x)
      case Right(x) => {
        val total = x("count").asInstanceOf[Int]
        search(config) { searcher =>
          f(searcher)
          searcher.setFrom(0)
          searcher.setSize(total)
        }
      }
    }
  }

  def searchByTemplate(config: ESConfig)(lang: String, template: String, params: AnyRef, options: Option[String] = None): Either[Map[String, Any], Map[String, Any]] = {
    logger.debug("******** ESConfig:" + config.toString)
    val json = JsonUtils.serialize(
      Map(
        "lang" -> lang,
        "template" -> Map(
          "file" -> template
        ),
        "params" -> params
      )
    )
    logger.debug(s"searchRequest:${json}")

    val resultJson = HttpUtils.post(httpClient, config.preferenceUrl(url, "_search/template" + options.getOrElse("")), json)
    val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
    map.get("error").map { case message: String => Left(map) }.getOrElse(Right(map))
  }

  def find[T](config: ESConfig)(f: SearchRequestBuilder => Unit)(implicit c: ClassTag[T]): Option[(String, T)] = {
    search(config)(f) match {
      case Left(x)  => throw new RuntimeException(x("error").toString)
      case Right(x) => {
        val hits = x("hits").asInstanceOf[Map[String, Any]]("hits").asInstanceOf[Seq[Map[String, Any]]]
        if(hits.length == 0){
          None
        } else {
          Some((hits.head("_id").toString, JsonUtils.deserialize[T](JsonUtils.serialize(getDocumentMap(hits.head)))))
        }
      }
    }
  }

  def findAsList[T](config: ESConfig)(f: SearchRequestBuilder => Unit)(implicit c: ClassTag[T]): List[(String, T)] = {
    search(config)(f) match {
      case Left(x)  => throw new RuntimeException(x("error").toString)
      case Right(x) => createESSearchResult(x).list.map { x => (x.id, x.doc) }
    }
  }

  def findAllAsList[T](config: ESConfig)(f: SearchRequestBuilder => Unit)(implicit c: ClassTag[T]): List[(String, T)] = {
    findAsList(config){ searcher =>
      f(searcher)
      searcher.setFrom(0)
      searcher.setSize(countAsInt(config)(f))
    }
  }


  def list[T](config: ESConfig)(f: SearchRequestBuilder => Unit)(implicit c: ClassTag[T]): ESSearchResult[T] = {
    search(config)(f) match {
      case Left(x)  => throw new RuntimeException(x("error").toString)
      case Right(x) => createESSearchResult(x)
    }
  }

  def listAll[T](config: ESConfig)(f: SearchRequestBuilder => Unit)(implicit c: ClassTag[T]): ESSearchResult[T] = {
    list(config){ searcher =>
      f(searcher)
      searcher.setFrom(0)
      searcher.setSize(countAsInt(config)(f))
    }
  }

  def listByTemplate[T](config: ESConfig)(lang: String, template: String, params: AnyRef)(implicit c: ClassTag[T]): ESSearchResult[T] = {
    searchByTemplate(config)(lang, template, params) match {
      case Left(x)  => throw new RuntimeException(x("error").toString)
      case Right(x) => createESSearchResult(x)
    }
  }

  def countByTemplate(config: ESConfig)(lang: String, template: String, params: AnyRef): Either[Map[String, Any], Map[String, Any]] = {
    searchByTemplate(config)(lang, template, params, Some("?search_type=count"))
  }

  def countByTemplateAsInt(config: ESConfig)(lang: String, template: String, params: AnyRef): Int = {
    countByTemplate(config)(lang: String, template: String, params: AnyRef) match {
      case Left(x)  => throw new RuntimeException(x("error").toString)
      case Right(x) => x("hits").asInstanceOf[Map[String, Any]]("total").asInstanceOf[Int]
    }
  }

  def refresh(config: ESConfig)(): Either[Map[String, Any], Map[String, Any]] = {
    val resultJson = HttpUtils.post(httpClient, s"${url}/${config.indexName}/_refresh", "")
    val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
    map.get("error").map { case message: String => Left(map) }.getOrElse(Right(map))
  }

  def scroll[T, R](config: ESConfig)(f: SearchRequestBuilder => Unit)(p: (String, T) => R)(implicit c1: ClassTag[T], c2: ClassTag[R]): Stream[R] = {
    @tailrec
    def scroll0[R](searchUrl: String, body: String, stream: Stream[R], invoker: (String, Map[String, Any]) => R): Stream[R] = {
      val resultJson = HttpUtils.post(httpClient, searchUrl + "?scroll=5m", body)
      val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
      if(map.get("error").isDefined){
        throw new RuntimeException(map("error").toString)
      } else {
        val scrollId = map("_scroll_id").toString
        val list = map("hits").asInstanceOf[Map[String, Any]]("hits").asInstanceOf[List[Map[String, Any]]]
        list match {
          case Nil  => stream
          case list => scroll0(s"${url}/_search/scroll", scrollId,
            list.map { map => invoker(map("_id").toString, getDocumentMap(map))
            }.toStream #::: stream, invoker)
        }
      }
    }

    logger.debug("******** ESConfig:" + config.toString)
    val searcher = queryClient.prepareSearch(config.indexName)
    config.typeName.foreach(x => searcher.setTypes(x))
    f(searcher)
    logger.debug(s"searchRequest:${searcher.toString}")

    scroll0(config.url(url) + "/_search", searcher.toString, Stream.empty,
      (_id: String, map: Map[String, Any]) => p(_id, JsonUtils.deserialize[T](JsonUtils.serialize(map))))
  }

  def scrollChunk[T, R](config: ESConfig)(f: SearchRequestBuilder => Unit)(p: (Seq[(String, T)]) => R)(implicit c1: ClassTag[T], c2: ClassTag[R]): Stream[R] = {
    @tailrec
    def scroll0[R](searchUrl: String, body: String, stream: Stream[R], invoker: (Seq[(String, Map[String, Any])]) => R): Stream[R] = {
      val resultJson = HttpUtils.post(httpClient, searchUrl + "?scroll=5m", body)
      val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
      if(map.get("error").isDefined){
        throw new RuntimeException(map("error").toString)
      } else {
        val scrollId = map("_scroll_id").toString
        val list = map("hits").asInstanceOf[Map[String, Any]]("hits").asInstanceOf[List[Map[String, Any]]]
        list match {
          case Nil  => stream
          case list => scroll0(s"${url}/_search/scroll", scrollId,
            Seq(invoker(list.map { map => (map("_id").toString, getDocumentMap(map)) })).toStream #::: stream, invoker)
        }
      }
    }

    logger.debug("******** ESConfig:" + config.toString)
    val searcher = queryClient.prepareSearch(config.indexName)
    config.typeName.foreach(x => searcher.setTypes(x))
    f(searcher)
    logger.debug(s"searchRequest:${searcher.toString}")

    scroll0(config.url(url) + "/_search", searcher.toString, Stream.empty,
      (maps: Seq[(String, Map[String, Any])]) => p(maps.map { case (id, map) =>
        (id, JsonUtils.deserialize[T](JsonUtils.serialize(map)))
      })
    )
  }


//  def scrollAsMap[R](config: ESConfig)(f: SearchRequestBuilder => Unit)(p: Map[String, Any] => R)(implicit c: ClassTag[R]): Stream[R] = {
//    logger.debug("******** ESConfig:" + config.toString)
//    val searcher = queryClient.prepareSearch(config.indexName)
//    config.typeName.foreach(x => searcher.setTypes(x))
//    f(searcher)
//    logger.debug(s"searchRequest:${searcher.toString}")
//
//    scroll0(config.url(url) + "/_search", searcher.toString, Stream.empty, (_id: String, map: Map[String, Any]) => p(map))
//  }

  def bulk[T](actions: Seq[BulkAction]): Either[Map[String, Any], Map[String, Any]] = {
    val resultJson = HttpUtils.post(httpClient, s"${url}/_bulk", actions.map(_.jsonString).mkString("\n"))
    val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
    map.get("error").map { case message: String => Left(map) }.getOrElse(Right(map))
  }

  def release() = {
    queryClient.close()
    httpClient.close()
  }

  private def getDocumentMap(hit: Map[String, Any]): Map[String, Any] = {
    hit.get("_source").map(_.asInstanceOf[Map[String, Any]])
      .getOrElse(structuredMap(hit("fields").asInstanceOf[Map[String, Any]]))
  }

  private def createESSearchResult[T](x: Map[String, Any])(implicit c: ClassTag[T]): ESSearchResult[T] = {
    val total = x("hits").asInstanceOf[Map[String, Any]]("total").asInstanceOf[Int]
    val took  = x("took").asInstanceOf[Int]
    val hits  = x("hits").asInstanceOf[Map[String, Any]]("hits").asInstanceOf[Seq[Map[String, Any]]]

    ESSearchResult(
      total,
      took,
      hits.map { hit =>
        ESSearchResultItem(hit("_id").toString,
          JsonUtils.deserialize[T](JsonUtils.serialize(getDocumentMap(hit))),
          hit.get("highlight").asInstanceOf[Option[Map[String, List[String]]]].getOrElse(Map.empty)
        )
      }.toList,
      x.get("facets").asInstanceOf[Option[Map[String, Map[String, Any]]]].getOrElse(Map.empty),
      x.get("aggregations").asInstanceOf[Option[Map[String, Any]]].getOrElse(Map.empty),
      x
    )
  }

  private def structuredMap(map: Map[String, Any]): Map[String, Any] = {
    def structuredMap0(group: List[(List[String], Any)]): Any = {
      group.groupBy { case (key, value) => key.head }.map { case (key, value) =>
        key -> (if(value.head._1.length == 1){
          value.head._2
        } else {
          structuredMap0(value.map { case (key, value) => key.tail -> value })
        })
      }
    }

    val list = map.map { case (key, value) => key.split("\\.").toList -> value }.toList
    structuredMap0(list).asInstanceOf[Map[String, Any]]
  }

}
