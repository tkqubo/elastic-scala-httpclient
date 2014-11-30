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
    val searcher = queryClient.prepareSearch(config.indexName).setTypes(config.typeName)
    f(searcher)
    logger.debug(s"searchRequest:${searcher.toString}")

    val future = HttpUtils.postAsync(httpClient, s"${url}/${config.indexName}/${config.typeName}/_search", searcher.toString)
    future.map { resultJson =>
      val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
      map.get("error").map { case message: String => Left(map) }.getOrElse(Right(map))
    }
  }

}
