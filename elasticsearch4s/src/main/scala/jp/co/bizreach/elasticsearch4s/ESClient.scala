package jp.co.bizreach.elasticsearch4s

import org.elasticsearch.action.search.SearchRequestBuilder
import ESClient._
import org.slf4j.LoggerFactory
import org.apache.http.impl.client.CloseableHttpClient
import org.elasticsearch.client.support.AbstractClient
import org.elasticsearch.index.query.{QueryBuilders, QueryBuilder}
import scala.reflect.ClassTag

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

case class ESConfig(indexName: String, typeName: String)
case class ESSearchResult[T](totalHits: Long, list: List[ESSearchResultItem[T]], facets: Map[String, Map[String, Any]])
case class ESSearchResultItem[T](id: String, doc: T, highlightFields: Map[String, List[String]])

class ESClient(queryClient: AbstractClient, httpClient: CloseableHttpClient, url: String) {

  def insertJson(config: ESConfig, json: String): Either[Map[String, Any], Map[String, Any]] = {
    logger.debug(s"insertJson:\n${json}")
    logger.debug(s"insertRequest:\n${json}")

    val resultJson = HttpUtils.post(httpClient, s"${url}/${config.indexName}/${config.typeName}/", json)
    val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
    map.get("error").map { case message: String => Left(map) }.getOrElse(Right(map))
  }

  def insert(config: ESConfig, entity: AnyRef):  Either[Map[String, Any], Map[String, Any]] = {
    insertJson(config, JsonUtils.serialize(entity))
  }

  def updateJson(config: ESConfig, id: String, json: String): Either[Map[String, Any], Map[String, Any]] = {
    logger.debug(s"updateJson:\n${json}")
    logger.debug(s"updateRequest:\n${json}")

    val resultJson = HttpUtils.put(httpClient, s"${url}/${config.indexName}/${config.typeName}/${id}", json)
    val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
    map.get("error").map { case message: String => Left(map) }.getOrElse(Right(map))
  }

  def update(config: ESConfig, id: String, entity: AnyRef): Either[Map[String, Any], Map[String, Any]] = {
    updateJson(config, id, JsonUtils.serialize(entity))
  }

  def delete(config: ESConfig, id: String): Either[Map[String, Any], Map[String, Any]] = {
    logger.debug(s"delete id:\n${id}")

    val resultJson = HttpUtils.delete(httpClient, s"${url}/${config.indexName}/${config.typeName}/${id}")
    val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
    map.get("error").map { case message: String => Left(map) }.getOrElse(Right(map))
  }

  def count(config: ESConfig)(f: SearchRequestBuilder => Unit): Either[Map[String, Any], Map[String, Any]] = {
    logger.debug("******** ESConfig:" + config.toString)
    val searcher = queryClient.prepareSearch(config.indexName).setTypes(config.typeName)
    searcher.setQuery(QueryBuilders.termQuery("multi", "test"))
    f(searcher)
    logger.debug(s"countRequest:${searcher.toString}")

    val resultJson = HttpUtils.post(httpClient, s"${url}/${config.indexName}/${config.typeName}/_count", searcher.toString)
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
    val searcher = queryClient.prepareSearch(config.indexName).setTypes(config.typeName)
    searcher.setQuery(QueryBuilders.termQuery("multi", "test"))
    f(searcher)
    logger.debug(s"searchRequest:${searcher.toString}")

    val resultJson = HttpUtils.post(httpClient, s"${url}/${config.indexName}/${config.typeName}/_search", searcher.toString)
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
      case Right(x) => {
        val total = x("hits").asInstanceOf[Map[String, Any]]("total").asInstanceOf[Int]
        val hits  = x("hits").asInstanceOf[Map[String, Any]]("hits").asInstanceOf[Seq[Map[String, Any]]]

        ESSearchResult(
          total,
          hits.map { hit =>
            ESSearchResultItem(hit("_id").toString,
              JsonUtils.deserialize[T](JsonUtils.serialize(hit("_source").asInstanceOf[Map[String, Any]])),
              hit.get("highlight").asInstanceOf[Option[Map[String, List[String]]]].getOrElse(Map.empty)
            )
          }.toList,
          x.get("facets").asInstanceOf[Option[Map[String, Map[String, Any]]]].getOrElse(Map.empty)
        )
      }
    }
  }

  def release() = {
    queryClient.close()
    httpClient.close()
  }

}
