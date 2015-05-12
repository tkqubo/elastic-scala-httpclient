package jp.co.bizreach.elasticsearch4s

import org.elasticsearch.action.search.SearchRequestBuilder
import org.slf4j.LoggerFactory
import org.elasticsearch.client.support.AbstractClient
import scala.reflect.ClassTag
import scala.annotation.tailrec
import com.ning.http.client.AsyncHttpClient
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global

object AsyncESClient {
  private val httpClient = HttpUtils.createHttpClient()

  def using[T](url: String)(f: AsyncESClient => T): T = {
    val client = new AsyncESClient(new QueryBuilderClient(), HttpUtils.createHttpClient(), url)
    f(client)
  }

  def shutdown() = {
    httpClient.close()
  }
}

class AsyncESClient(queryClient: AbstractClient, httpClient: AsyncHttpClient, url: String) {

  val logger = LoggerFactory.getLogger(classOf[AsyncESClient])

  def searchAsync(config: ESConfig)(f: SearchRequestBuilder => Unit): Future[Either[Map[String, Any], Map[String, Any]]] = {
    logger.debug("******** ESConfig:" + config.toString)
    val searcher = queryClient.prepareSearch(config.indexName)
    config.typeName.foreach(x => searcher.setTypes(x))
    f(searcher)
    logger.debug(s"searchRequest:${searcher.toString}")

    val future = HttpUtils.postAsync(httpClient, config.url(url) + "/_search", searcher.toString)
    future.map { resultJson =>
      val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
      map.get("error").map { case message: String => Left(map) }.getOrElse(Right(map))
    }
  }

  def insertJsonAsync(config: ESConfig, json: String): Future[Either[Map[String, Any], Map[String, Any]]] = {
    logger.debug(s"insertJson:\n${json}")
    logger.debug(s"insertRequest:\n${json}")

    val future = HttpUtils.postAsync(httpClient, config.url(url), json)
    future.map { resultJson =>
      val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
      map.get("error").map { case message: String => Left(map) }.getOrElse(Right(map))
    }
  }

  def insertAsync(config: ESConfig, entity: AnyRef):  Future[Either[Map[String, Any], Map[String, Any]]] = {
    insertJsonAsync(config, JsonUtils.serialize(entity))
  }

  def updateJsonAsync(config: ESConfig, id: String, json: String): Future[Either[Map[String, Any], Map[String, Any]]] = {
    logger.debug(s"updateJson:\n${json}")
    logger.debug(s"updateRequest:\n${json}")

    val future = HttpUtils.putAsync(httpClient, config.url(url) + "/" + id, json)
    future.map { resultJson =>
      val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
      map.get("error").map { case message: String => Left(map) }.getOrElse(Right(map))
    }
  }

  def updateAsync(config: ESConfig, id: String, entity: AnyRef): Future[Either[Map[String, Any], Map[String, Any]]] = {
    updateJsonAsync(config, id, JsonUtils.serialize(entity))
  }

  def deleteAsync(config: ESConfig, id: String): Future[Either[Map[String, Any], Map[String, Any]]] = {
    logger.debug(s"delete id:\n${id}")

    val future = HttpUtils.deleteAsync(httpClient, config.url(url) + "/" + id)
    future.map { resultJson =>
      val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
      map.get("error").map { case message: String => Left(map) }.getOrElse(Right(map))
    }
  }

}
