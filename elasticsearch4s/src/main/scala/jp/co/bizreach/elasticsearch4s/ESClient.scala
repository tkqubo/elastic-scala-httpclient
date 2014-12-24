package jp.co.bizreach.elasticsearch4s

import org.elasticsearch.action.search.SearchRequestBuilder
import ESClient._
import org.slf4j.LoggerFactory
import org.elasticsearch.client.support.AbstractClient
import scala.reflect.ClassTag
import scala.annotation.tailrec
import com.ning.http.client.AsyncHttpClient

/**
 * Helper for accessing to Elasticsearch.
 */
object ESClient {

  val logger = LoggerFactory.getLogger(classOf[ESClient])

//  /**
//   * Create a ESSearchHelper instance.
//   */
//  def apply(url: String): ESClient = new ESClient(new QueryBuilderClient(), HttpUtils.createHttpClient(), url)

  /**
   * This is the entry point of processing using ElasticSearch.
   * Give ESConfig and your function which takes ESSearchHelper as an argument.
   */
  def using[T](url: String)(f: ESClient => T): T = {
    val client = new ESClient(new QueryBuilderClient(), HttpUtils.createHttpClient(), url)
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

    val resultJson = HttpUtils.post(httpClient,
      if(config.typeName.isDefined){
        s"${url}/${config.indexName}/${config.typeName.get}/"
      } else {
        s"${url}/${config.indexName}/"
      },
      json)
    val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
    map.get("error").map { case message: String => Left(map) }.getOrElse(Right(map))
  }

  def insert(config: ESConfig, entity: AnyRef):  Either[Map[String, Any], Map[String, Any]] = {
    insertJson(config, JsonUtils.serialize(entity))
  }

  def updateJson(config: ESConfig, id: String, json: String): Either[Map[String, Any], Map[String, Any]] = {
    logger.debug(s"updateJson:\n${json}")
    logger.debug(s"updateRequest:\n${json}")

    val resultJson = HttpUtils.put(httpClient,
      if(config.typeName.isDefined){
        s"${url}/${config.indexName}/${config.typeName.get}/${id}"
      } else {
        s"${url}/${config.indexName}/${id}"
      },
      json)
    val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
    map.get("error").map { case message: String => Left(map) }.getOrElse(Right(map))
  }

  def update(config: ESConfig, id: String, entity: AnyRef): Either[Map[String, Any], Map[String, Any]] = {
    updateJson(config, id, JsonUtils.serialize(entity))
  }

  def delete(config: ESConfig, id: String): Either[Map[String, Any], Map[String, Any]] = {
    logger.debug(s"delete id:\n${id}")

    val resultJson = HttpUtils.delete(httpClient,
      if(config.typeName.isDefined){
        s"${url}/${config.indexName}/${config.typeName.get}/${id}"
      } else {
        s"${url}/${config.indexName}/${id}"
      }
    )
    val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
    map.get("error").map { case message: String => Left(map) }.getOrElse(Right(map))
  }

  def deleteByQuery(config: ESConfig)(f: SearchRequestBuilder => Unit): Either[Map[String, Any], Map[String, Any]] = {
    logger.debug("******** ESConfig:" + config.toString)
    val searcher = queryClient.prepareSearch(config.indexName)
    config.typeName.foreach(x => searcher.setTypes(x))
    //searcher.setQuery(QueryBuilders.termQuery("multi", "test"))
    f(searcher)
    logger.debug(s"deleteByQuery:${searcher.toString}")

    val resultJson = HttpUtils.delete(httpClient,
      if(config.typeName.isDefined){
        s"${url}/${config.indexName}/${config.typeName.get}/_query"
      } else {
        s"${url}/${config.indexName}/_query"
      },
      searcher.toString)
    val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
    map.get("error").map { case message: String => Left(map) }.getOrElse(Right(map))
  }

  def count(config: ESConfig)(f: SearchRequestBuilder => Unit): Either[Map[String, Any], Map[String, Any]] = {
    logger.debug("******** ESConfig:" + config.toString)
    val searcher = queryClient.prepareSearch(config.indexName)
    config.typeName.foreach(x => searcher.setTypes(x))
    //searcher.setQuery(QueryBuilders.termQuery("multi", "test"))
    f(searcher)
    logger.debug(s"countRequest:${searcher.toString}")

    val resultJson = HttpUtils.post(httpClient,
      if(config.typeName.isDefined){
        s"${url}/${config.indexName}/${config.typeName.get}/_count"
      } else {
        s"${url}/${config.indexName}/_count"
      },
      searcher.toString)
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

    val resultJson = HttpUtils.post(httpClient,
      if(config.typeName.isDefined) {
        s"${url}/${config.indexName}/${config.typeName.get}/_search"
      } else {
        s"${url}/${config.indexName}/_search"
      },
      searcher.toString)
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

    val resultJson = HttpUtils.post(httpClient,
      if(config.typeName.isDefined){
        s"${url}/${config.indexName}/${config.typeName.get}/_search/template" + options.getOrElse("")
      } else {
        s"${url}/${config.indexName}/_search/template" + options.getOrElse("")
      },json)
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
          Some((hits.head("_id").toString, JsonUtils.deserialize[T](JsonUtils.serialize(hits.head("_source").asInstanceOf[Map[String, Any]]))))
        }
      }
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

  def scroll[T, R](config: ESConfig)(f: SearchRequestBuilder => Unit)(p: T => R)(implicit c1: ClassTag[T], c2: ClassTag[R]): Stream[R] = {
    logger.debug("******** ESConfig:" + config.toString)
    val searcher = queryClient.prepareSearch(config.indexName)
    config.typeName.foreach(x => searcher.setTypes(x))
    f(searcher)
    logger.debug(s"searchRequest:${searcher.toString}")

    scroll0(
      if(config.typeName.isDefined){
        s"${url}/${config.indexName}/${config.typeName.get}/_search"
      } else {
        s"${url}/${config.indexName}/_search"
      }
      ,searcher.toString, Stream.empty, (map: Map[String, Any]) => p(JsonUtils.deserialize[T](JsonUtils.serialize(map))))
  }

  def scrollAsMap[R](config: ESConfig)(f: SearchRequestBuilder => Unit)(p: Map[String, Any] => R)(implicit c: ClassTag[R]): Stream[R] = {
    logger.debug("******** ESConfig:" + config.toString)
    val searcher = queryClient.prepareSearch(config.indexName)
    config.typeName.foreach(x => searcher.setTypes(x))
    f(searcher)
    logger.debug(s"searchRequest:${searcher.toString}")

    scroll0(
      if(config.typeName.isDefined){
        s"${url}/${config.indexName}/${config.typeName.get}/_search"
      } else {
        s"${url}/${config.indexName}/_search"
      },
      searcher.toString, Stream.empty, (map: Map[String, Any]) => p(map))
  }

  @tailrec
  private def scroll0[R](searchUrl: String, body: String, stream: Stream[R], invoker: Map[String, Any] => R): Stream[R] = {
    val resultJson = HttpUtils.post(httpClient, searchUrl + "?scroll=5m", body)
    val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
    if(map.get("error").isDefined){
      throw new RuntimeException(map("error").toString)
    } else {
      val scrollId = map("_scroll_id").toString
      val list = map("hits").asInstanceOf[Map[String, Any]]("hits").asInstanceOf[List[Map[String, Any]]]
      list match {
        case Nil  => stream
        case list => scroll0(s"${url}/_search/scroll", scrollId, stream ++ list.map { map =>
          invoker(map("_source").asInstanceOf[Map[String, Any]])
        }.toStream, invoker)
      }
    }
  }

  def release() = {
    queryClient.close()
    httpClient.close()
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
          JsonUtils.deserialize[T](JsonUtils.serialize(hit("_source").asInstanceOf[Map[String, Any]])),
          hit.get("highlight").asInstanceOf[Option[Map[String, List[String]]]].getOrElse(Map.empty)
        )
      }.toList,
      x.get("facets").asInstanceOf[Option[Map[String, Map[String, Any]]]].getOrElse(Map.empty),
      x.get("aggregations").asInstanceOf[Option[Map[String, Any]]].getOrElse(Map.empty),
      x
    )
  }

}
