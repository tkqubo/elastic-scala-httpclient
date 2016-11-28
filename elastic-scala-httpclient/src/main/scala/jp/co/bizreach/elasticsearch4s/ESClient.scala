package jp.co.bizreach.elasticsearch4s

import org.elasticsearch.action.search.SearchRequestBuilder
import ESClient._
import ESUtils._
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag
import scala.annotation.tailrec
import com.ning.http.client.{AsyncHttpClient, AsyncHttpClientConfig, SignatureCalculator}

/**
 * Helper for accessing to Elasticsearch.
 */
object ESClient {

  private val logger = LoggerFactory.getLogger(classOf[ESClient])
  private var httpClient: AsyncHttpClient = null

  /**
   * This is the entry point of processing using Elasticsearch.
   */
  def using[T](url: String,
               config: AsyncHttpClientConfig = new AsyncHttpClientConfig.Builder().build(),
               deleteByQueryIsAvailable: Boolean = false,
               scriptTemplateIsAvailable: Boolean = false)(f: ESClient => T): T = {
    val httpClient = new AsyncHttpClient(config)
    val client = new ESClient(httpClient, url, deleteByQueryIsAvailable, scriptTemplateIsAvailable)
    try {
      f(client)
    } finally {
      httpClient.close()
    }
  }

  /**
   * Initialize AsyncHttpClient. ESClient is available by calling this method.
   */
  def init(): Unit = {
    httpClient = HttpUtils.createHttpClient()
  }

  /**
   * Return ESClient instance.
   */
  def apply(url: String, deleteByQueryIsAvailable: Boolean = false, scriptTemplateIsAvailable: Boolean = false): ESClient = {
    if(httpClient == null){
      throw new IllegalStateException("AsyncHttpClient has not been initialized. Call ESClient.init() at first.")
    }
    new ESClient(httpClient, url, deleteByQueryIsAvailable, scriptTemplateIsAvailable)
  }

  /**
   * Initialize AsyncHttpClient with given configuration. ESClient is available by calling this method.
   */
  def init(config: AsyncHttpClientConfig, signatureCalculator: Option[SignatureCalculator] = None): Unit = {
    httpClient = HttpUtils.createHttpClient(config)
    signatureCalculator.foreach(httpClient.setSignatureCalculator)
  }

  /**
   * Shutdown AsyncHttpClient. ESClient is disabled by calling this method.
   */
  def shutdown() = {
    httpClient.close()
    httpClient = null
  }

}

class ESClient(httpClient: AsyncHttpClient, url: String,
               deleteByQueryIsAvailable: Boolean = false, scriptTemplateIsAvailable: Boolean = false) {

  private val queryClient = new QueryBuilderClient()

  def insertJson(config: ESConfig, json: String): Either[Map[String, Any], Map[String, Any]] = {
    logger.debug(s"insertJson:\n${json}")

    val resultJson = HttpUtils.post(httpClient, config.url(url), json)
    val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
    map.get("error").map { case message: String => Left(map) }.getOrElse(Right(map))
  }

  def insertJson(config: ESConfig, id: String, json: String): Either[Map[String, Any], Map[String, Any]] = {
    logger.debug(s"insertJson:\n${json}")

    val resultJson = HttpUtils.post(httpClient, config.url(url) + "/" + id, json)
    val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
    map.get("error").map { case message: String => Left(map) }.getOrElse(Right(map))
  }

  def insert(config: ESConfig, entity: AnyRef):  Either[Map[String, Any], Map[String, Any]] = {
    insertJson(config, JsonUtils.serialize(entity))
  }

  def insert(config: ESConfig, id: String, entity: AnyRef):  Either[Map[String, Any], Map[String, Any]] = {
    insertJson(config, id, JsonUtils.serialize(entity))
  }

  def indexExist(config: ESConfig): Either[Map[String, Any], Map[String, Any]] = {
    logger.debug(s"index exist ${config.url(url)}")

    try {
      HttpUtils.head(httpClient, config.url(url))
      Right(Map("result" -> true))
    } catch {
      case HttpResponseException(status, _, _) if status == 404 => Right(Map("result" -> false))
      case ex: Throwable => Left(Map("error" -> ex))
    }
  }

  def putMapping(config: ESConfig, mapping: AnyRef): Either[Map[String, Any], Map[String, Any]] = {
    val json = JsonUtils.serialize(mapping)

    val resultJson = HttpUtils.put(httpClient, s"$url/${config.indexName}/_mapping/${config.typeName.get}", json)
    val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
    map.get("error").map { case message: String => Left(map) }.getOrElse(Right(map))
  }

  def createOrUpdateIndex(config: ESConfig, settings: AnyRef): Either[Map[String, Any], Map[String, Any]] = {
    val json = JsonUtils.serialize(settings)

    logger.debug(s"create or update ${config.indexName}: $json")
    val resultJson = HttpUtils.put(httpClient, config.url(url), json)
    val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
    map.get("error").map { case message: String => Left(map) }.getOrElse(Right(map))
  }

  def createOrUpdateIndex(config: ESConfig, settings: AnyRef): Either[Map[String, Any], Map[String, Any]] = {
    val json = JsonUtils.serialize(settings)

    logger.debug(s"put mapping ${config.indexName}: $json")
    val resultJson = HttpUtils.put(httpClient, s"${config.url(url)}/${config.indexName}", json)
    val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
    map.get("error").map { case message: String => Left(map) }.getOrElse(Right(map))
  }

  def updateJson(config: ESConfig, id: String, json: String): Either[Map[String, Any], Map[String, Any]] = {
    logger.debug(s"updateJson:\n${json}")

    val resultJson = HttpUtils.put(httpClient, config.url(url) + "/" + id, json)
    val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
    map.get("error").map { case message: String => Left(map) }.getOrElse(Right(map))
  }

  def update(config: ESConfig, id: String, entity: AnyRef): Either[Map[String, Any], Map[String, Any]] = {
    updateJson(config, id, JsonUtils.serialize(entity))
  }

  def updatePartiallyJson(config: ESConfig, id: String, json: String): Either[Map[String, Any], Map[String, Any]] = {
    logger.debug(s"updatePartiallyJson:\n${json}")

    val resultJson = HttpUtils.post(httpClient, config.url(url) + "/" + id + "/_update", "{\"doc\":"+ s"${json}}")
    val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
    map.get("error").map { case message: String => Left(map) }.getOrElse(Right(map))
  }

  def updatePartially(config: ESConfig, id: String, entity: AnyRef): Either[Map[String, Any], Map[String, Any]] = {
    updatePartiallyJson(config, id, JsonUtils.serialize(entity))
  }

  def delete(config: ESConfig, id: String): Either[Map[String, Any], Map[String, Any]] = {
    logger.debug(s"delete id:\n${id}")

    val resultJson = HttpUtils.delete(httpClient, config.url(url) + "/" + id)
    val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
    map.get("error").map { case message: String => Left(map) }.getOrElse(Right(map))
  }

  /**
   * Note: Need delete-by-query plugin to use this method.
   * https://www.elastic.co/guide/en/elasticsearch/plugins/2.3/plugins-delete-by-query.html
   */
  def deleteByQuery(config: ESConfig)(f: SearchRequestBuilder => Unit): Either[Map[String, Any], Map[String, Any]] = {
    if(deleteByQueryIsAvailable) {
      logger.debug("******** ESConfig:" + config.toString)
      val searcher = queryClient.prepareSearch(config.indexName)
      config.typeName.foreach(x => searcher.setTypes(x))
      f(searcher)
      logger.debug(s"deleteByQuery:${searcher.toString}")

      val resultJson = HttpUtils.delete(httpClient, config.url(url) + "/_query", searcher.toString)
      val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
      map.get("error").map { case message: String => Left(map) }.getOrElse(Right(map))
    } else {
      throw new UnsupportedOperationException("You can install delete-by-query plugin to use this method.")
    }
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

  /**
   * Note: Need elasticsearch-sstmpl plugin to use this method.
   * https://github.com/codelibs/elasticsearch-sstmpl
   */
  def searchByTemplate(config: ESConfig)(lang: String, template: String, params: AnyRef, options: Option[String] = None): Either[Map[String, Any], Map[String, Any]] = {
    if(scriptTemplateIsAvailable) {
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

      val resultJson = HttpUtils.post(httpClient, config.urlWithParameters(url, "_search/template" + options.getOrElse("")), json)
      val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
      map.get("error").map { case message: String => Left(map) }.getOrElse(Right(map))
    } else {
      throw new UnsupportedOperationException("You can install elasticsearch-sstmpl plugin to use this method.")
    }
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

  /**
   * Note: Need elasticsearch-sstmpl plugin to use this method.
   * https://github.com/codelibs/elasticsearch-sstmpl
   */
  def listByTemplate[T](config: ESConfig)(lang: String, template: String, params: AnyRef)(implicit c: ClassTag[T]): ESSearchResult[T] = {
    if(scriptTemplateIsAvailable) {
      searchByTemplate(config)(lang, template, params) match {
        case Left(x)  => throw new RuntimeException(x("error").toString)
        case Right(x) => createESSearchResult(x)
      }
    } else {
      throw new UnsupportedOperationException("You can install elasticsearch-sstmpl plugin to use this method.")
    }
  }

  def findAllByTypeAndId(ids: Seq[(ESConfig, String)]): List[Map[String, Any]] = {
    val docs =
      for {
        (config, id) <- ids
        typeName <- config.typeName
      } yield Map(
        "_index" -> config.indexName,
        "_type" -> typeName,
        "_id" -> id
      )
    val json = JsonUtils.serialize(Map("docs" -> docs))
    logger.debug(s"multigetRequest:${json}")
    val resultJson = HttpUtils.post(httpClient, s"${url}/_mget", json)
    val result = JsonUtils.deserialize[Map[String, Any]](resultJson)
    result.get("docs").map(_.asInstanceOf[List[Map[String, Any]]]).getOrElse(Nil)
  }

  def findAllByTypeAndIdAsList[T](ids: Seq[(ESConfig, String)])(implicit c: ClassTag[T]): List[(String, Option[T])] = {
    findAllByTypeAndId(ids).collect {
      case doc if doc.get("found").exists(_ == true) =>
        doc("_id").toString -> Some(JsonUtils.deserialize[T](JsonUtils.serialize(doc.get("_source"))))
      case doc =>
        doc("_id").toString -> None
    }
  }

  def findAllByIds[T](config: ESConfig, ids: Seq[String])(implicit c: ClassTag[T]): List[Map[String, Any]] = {
    val json = JsonUtils.serialize(Map("ids" -> ids))
    logger.debug(s"multigetRequest:${json}")
    val resultJson = HttpUtils.post(httpClient, config.url(url) + "/_mget", json)
    val result = JsonUtils.deserialize[Map[String, Any]](resultJson)
    result.get("docs").map(_.asInstanceOf[List[Map[String, Any]]]).getOrElse(Nil)
  }

  def findAllByIdsAsList[T](config: ESConfig, ids: Seq[String])(implicit c: ClassTag[T]): List[(String, Option[T])] = {
    findAllByIds(config, ids).collect {
      case doc if doc.get("found").exists(_ == true) =>
        doc("_id").toString -> Some(JsonUtils.deserialize[T](JsonUtils.serialize(doc.get("_source"))))
      case doc =>
        doc("_id").toString -> None
    }
  }

  /**
   * Note: Need elasticsearch-sstmpl plugin to use this method.
   * https://github.com/codelibs/elasticsearch-sstmpl
   */
  def countByTemplate(config: ESConfig)(lang: String, template: String, params: AnyRef): Either[Map[String, Any], Map[String, Any]] = {
    if(scriptTemplateIsAvailable) {
      searchByTemplate(config)(lang, template, params, Some("?search_type=query_then_fetch&size=0"))
    } else {
      throw new UnsupportedOperationException("You can install elasticsearch-sstmpl plugin to use this method.")
    }
  }

  /**
   * Note: Need elasticsearch-sstmpl plugin to use this method.
   * https://github.com/codelibs/elasticsearch-sstmpl
   */
  def countByTemplateAsInt(config: ESConfig)(lang: String, template: String, params: AnyRef): Int = {
    if(scriptTemplateIsAvailable) {
      countByTemplate(config)(lang: String, template: String, params: AnyRef) match {
        case Left(x)  => throw new RuntimeException(x("error").toString)
        case Right(x) => x("hits").asInstanceOf[Map[String, Any]]("total").asInstanceOf[Int]
      }
    } else {
      throw new UnsupportedOperationException("You can install elasticsearch-sstmpl plugin to use this method.")
    }
  }

  def refresh(config: ESConfig)(): Either[Map[String, Any], Map[String, Any]] = {
    val resultJson = HttpUtils.post(httpClient, s"${url}/${config.indexName}/_refresh", "")
    val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
    map.get("error").map { case message: String => Left(map) }.getOrElse(Right(map))
  }

  def scroll[T, R](config: ESConfig)(f: SearchRequestBuilder => Unit)(p: (String, T) => R)(implicit c1: ClassTag[T], c2: ClassTag[R]): Stream[R] = {
    @tailrec
    def scroll0[R](init: Boolean, searchUrl: String, body: String, stream: Stream[R], invoker: (String, Map[String, Any]) => R): Stream[R] = {
      val resultJson = HttpUtils.post(httpClient, searchUrl + "?scroll=5m&sort=_doc", body)
      val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
      if(map.get("error").isDefined){
        throw new RuntimeException(map("error").toString)
      } else {
        val scrollId = map("_scroll_id").toString
        val list = map("hits").asInstanceOf[Map[String, Any]]("hits").asInstanceOf[List[Map[String, Any]]]
        list match {
          case Nil if init == false => stream
          case Nil  => scroll0(false, s"${url}/_search/scroll", scrollId, stream, invoker)
          case list => scroll0(false, s"${url}/_search/scroll", scrollId, list.map { map => invoker(map("_id").toString, getDocumentMap(map)) }.toStream #::: stream, invoker)
        }
      }
    }

    logger.debug("******** ESConfig:" + config.toString)
    val searcher = queryClient.prepareSearch(config.indexName)
    config.typeName.foreach(x => searcher.setTypes(x))
    f(searcher)
    logger.debug(s"searchRequest:${searcher.toString}")

    scroll0(true, config.url(url) + "/_search", searcher.toString, Stream.empty,
      (_id: String, map: Map[String, Any]) => p(_id, JsonUtils.deserialize[T](JsonUtils.serialize(map))))
  }

  def scrollChunk[T, R](config: ESConfig)(f: SearchRequestBuilder => Unit)(p: (Seq[(String, T)]) => R)(implicit c1: ClassTag[T], c2: ClassTag[R]): Stream[R] = {
    @tailrec
    def scroll0[R](init: Boolean, searchUrl: String, body: String, stream: Stream[R], invoker: (Seq[(String, Map[String, Any])]) => R): Stream[R] = {
      val resultJson = HttpUtils.post(httpClient, searchUrl + "?scroll=5m&sort=_doc", body)
      val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
      if(map.get("error").isDefined){
        throw new RuntimeException(map("error").toString)
      } else {
        val scrollId = map("_scroll_id").toString
        val list = map("hits").asInstanceOf[Map[String, Any]]("hits").asInstanceOf[List[Map[String, Any]]]
        list match {
          case Nil if init == false => stream
          case Nil  => scroll0(false, s"${url}/_search/scroll", scrollId, stream, invoker)
          case list => scroll0(false, s"${url}/_search/scroll", scrollId, Seq(invoker(list.map { map => (map("_id").toString, getDocumentMap(map)) })).toStream #::: stream, invoker)
        }
      }
    }

    logger.debug("******** ESConfig:" + config.toString)
    val searcher = queryClient.prepareSearch(config.indexName)
    config.typeName.foreach(x => searcher.setTypes(x))
    f(searcher)
    logger.debug(s"searchRequest:${searcher.toString}")

    scroll0(true, config.url(url) + "/_search", searcher.toString, Stream.empty,
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
    val resultJson = HttpUtils.post(httpClient, s"${url}/_bulk", actions.map(_.jsonString).mkString("", "\n", "\n"))
    val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
    map.get("error").map { case message: String => Left(map) }.getOrElse(Right(map))
  }

}
