package jp.co.bizreach.elasticsearch4s

import org.elasticsearch.action.search.SearchRequestBuilder
import ESClient._
import org.slf4j.LoggerFactory
import jp.co.bizreach.elasticsearch4s.ESClient.ESConfig
import jp.co.bizreach.elasticsearch4s.ESClient.ESSearchResult
import scala.Some
import jp.co.bizreach.elasticsearch4s.ESClient.ESSearchResultItem
import org.apache.http.impl.client.CloseableHttpClient
import org.elasticsearch.client.support.AbstractClient

/**
 * Helper for accessing to Elasticsearch.
 *
 * Created by nishiyama on 2014/04/25.
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

  case class ESConfig(indexName: String, typeName: String)
  case class ESSearchResult[T](totalHits: Long, list: List[ESSearchResultItem[T]], facets: Map[String, Map[String, Any]])
  case class ESSearchResultItem[T](id: String, doc: T, highlightFields: Map[String, String])
}

class ESClient(queryClient: AbstractClient, httpClient: CloseableHttpClient, url: String) {

  def insertJson(json: String)(implicit config: ESConfig): Either[Map[String, Any], Map[String, Any]] = {
    logger.debug(s"insertJson:\n${json}")
    logger.debug(s"insertRequest:\n${json}")

    val resultJson = HttpUtils.post(httpClient, s"${url}/${config.indexName}/${config.typeName}/", json)
    val map = JsonUtils.deserialize(resultJson, classOf[Map[String, Any]])
    map.get("error").map { case message: String => Left(map) }.getOrElse(Right(map))
  }

  def insert(entity: AnyRef)(implicit config: ESConfig):  Either[Map[String, Any], Map[String, Any]] = {
    insertJson(JsonUtils.serialize(entity))
  }

  def updateJson(id: String, json: String)(implicit config: ESConfig): Either[Map[String, Any], Map[String, Any]] = {
    logger.debug(s"updateJson:\n${json}")
    logger.debug(s"updateRequest:\n${json}")

    val resultJson = HttpUtils.put(httpClient, s"${url}/${config.indexName}/${config.typeName}/${id}", json)
    val map = JsonUtils.deserialize(resultJson, classOf[Map[String, Any]])
    map.get("error").map { case message: String => Left(map) }.getOrElse(Right(map))
  }

  def update(id: String, entity: AnyRef)(implicit config: ESConfig): Either[Map[String, Any], Map[String, Any]] = {
    updateJson(id, JsonUtils.serialize(entity))
  }

  def delete(id: String)(implicit config: ESConfig): Either[Map[String, Any], Map[String, Any]] = {
    logger.debug(s"delete id:\n${id}")

    val resultJson = HttpUtils.delete(httpClient, s"${url}/${config.indexName}/${config.typeName}/${id}")
    val map = JsonUtils.deserialize(resultJson, classOf[Map[String, Any]])
    map.get("error").map { case message: String => Left(map) }.getOrElse(Right(map))
  }

  def search(f: SearchRequestBuilder => Unit)(implicit config: ESConfig): Either[Map[String, Any], Map[String, Any]] = {
    logger.debug("******** ESConfig:" + config.toString)
    val searcher = queryClient.prepareSearch(config.indexName).setTypes(config.typeName)
    f(searcher)
    logger.debug(s"searchRequest:${searcher.toString}")

    val resultJson = HttpUtils.post(httpClient, s"${url}/${config.indexName}/${config.typeName}/_search", searcher.toString)
    val map = JsonUtils.deserialize(resultJson, classOf[Map[String, Any]])
    map.get("error").map { case message: String => Left(map) }.getOrElse(Right(map))
  }

  def find[T](clazz: Class[T])(f: SearchRequestBuilder => Unit)(implicit config: ESConfig): Option[(String, T)] = {
    search(f) match {
      case Left(x)  => throw new RuntimeException(x("error").toString)
      case Right(x) => {
        val hits = x("hits").asInstanceOf[Map[String, Any]]("hits").asInstanceOf[Seq[Map[String, Any]]]
        if(hits.length == 0){
          None
        } else {
          Some((hits.head("_id").toString, JsonUtils.deserialize(JsonUtils.serialize(hits.head("_source").asInstanceOf[Map[String, Any]]), clazz)))
        }
      }
    }
  }

  def list[T](clazz: Class[T])(f: SearchRequestBuilder => Unit)(implicit config: ESConfig): ESSearchResult[T] = {
    search(f) match {
      case Left(x)  => throw new RuntimeException(x("error").toString)
      case Right(x) => {
        val total = x("hits").asInstanceOf[Map[String, Any]]("total").asInstanceOf[Int]
        val hits  = x("hits").asInstanceOf[Map[String, Any]]("hits").asInstanceOf[Seq[Map[String, Any]]]

        ESSearchResult(
          total,
          hits.map { hit =>
            ESSearchResultItem(hit("_id").toString,
              JsonUtils.deserialize(JsonUtils.serialize(hit("_source").asInstanceOf[Map[String, Any]]), clazz),
              hit.get("highlight").asInstanceOf[Option[Map[String, String]]].getOrElse(Map.empty[String, String])
            )
          }.toList,
          x.get("facets").asInstanceOf[Option[Map[String, Map[String, Any]]]].getOrElse(Map.empty[String, Map[String, Any]])
        )
      }
    }
  }

  def release() = {
    queryClient.close()
    httpClient.close()
  }

}
