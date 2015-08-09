package jp.co.bizreach.elasticsearch4s

import ESUtils._
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

    val future = HttpUtils.postAsync(httpClient, config.preferenceUrl(url, "/_search"), searcher.toString)
    future.map { resultJson =>
      val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
      map.get("error").map { case message: String => Left(map) }.getOrElse(Right(map))
    }
  }

  def searchAllAsync(config: ESConfig)(f: SearchRequestBuilder => Unit): Future[Either[Map[String, Any], Map[String, Any]]] = {
    countAsync(config)(f).flatMap { result =>
      result match {
        case Left(x)  => Future(Left(x))
        case Right(x) => {
          val total = x("count").asInstanceOf[Int]
          searchAsync(config) { searcher =>
            f(searcher)
            searcher.setFrom(0)
            searcher.setSize(total)
          }
        }
      }
    }
  }

  def searchByTemplateAsync(config: ESConfig)(lang: String, template: String, params: AnyRef, options: Option[String] = None): Future[Either[Map[String, Any], Map[String, Any]]] = {
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

    val future = HttpUtils.postAsync(httpClient, config.urlWithParameters(url, "_search/template" + options.getOrElse("")), json)
    future.map { resultJson =>
      val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
      map.get("error").map { case message: String => Left(map) }.getOrElse(Right(map))
    }
  }

  def findAsync[T](config: ESConfig)(f: SearchRequestBuilder => Unit)(implicit c: ClassTag[T]): Future[Option[(String, T)]] = {
    searchAsync(config)(f).map { result =>
      result match {
        case Left(x)  => throw new RuntimeException(x("error").toString) // TODO Call error handler instead of throwing exception
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
  }

  def findAsListAsync[T](config: ESConfig)(f: SearchRequestBuilder => Unit)(implicit c: ClassTag[T]): Future[List[(String, T)]] = {
    searchAsync(config)(f).map { result =>
      result match {
        case Left(x)  => throw new RuntimeException(x("error").toString) // TODO Call error handler instead of throwing exception
        case Right(x) => createESSearchResult(x).list.map { x => (x.id, x.doc) }
      }
    }
  }

  def findAllAsListAsync[T](config: ESConfig)(f: SearchRequestBuilder => Unit)(implicit c: ClassTag[T]): Future[List[(String, T)]] = {
    findAsListAsync(config){ searcher =>
      countAsIntAsync(config)(f).map { count =>
        f(searcher)
        searcher.setFrom(0)
        searcher.setSize(count)
      }
    }
  }

  def listAsync[T](config: ESConfig)(f: SearchRequestBuilder => Unit)(implicit c: ClassTag[T]): Future[ESSearchResult[T]] = {
    searchAsync(config)(f).map { result =>
      result match {
        case Left(x)  => throw new RuntimeException(x("error").toString) // TODO Call error handler instead of throwing exception
        case Right(x) => createESSearchResult(x)
      }
    }
  }

  def listAllAsync[T](config: ESConfig)(f: SearchRequestBuilder => Unit)(implicit c: ClassTag[T]): Future[ESSearchResult[T]] = {
    listAsync(config){ searcher =>
      countAsIntAsync(config)(f).map { count =>
        f(searcher)
        searcher.setFrom(0)
        searcher.setSize(count)
      }
    }
  }

  def listByTemplateAsync[T](config: ESConfig)(lang: String, template: String, params: AnyRef)(implicit c: ClassTag[T]): Future[ESSearchResult[T]] = {
    searchByTemplateAsync(config)(lang, template, params).map { result =>
      result match {
        case Left(x)  => throw new RuntimeException(x("error").toString)
        case Right(x) => createESSearchResult(x)
      }
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

  def deleteByQueryAsync(config: ESConfig)(f: SearchRequestBuilder => Unit): Future[Either[Map[String, Any], Map[String, Any]]] = ???

  def countAsync(config: ESConfig)(f: SearchRequestBuilder => Unit): Future[Either[Map[String, Any], Map[String, Any]]] = {
    logger.debug("******** ESConfig:" + config.toString)
    val searcher = queryClient.prepareSearch(config.indexName)
    config.typeName.foreach(x => searcher.setTypes(x))
    f(searcher)
    logger.debug(s"countRequest:${searcher.toString}")

    val future = HttpUtils.postAsync(httpClient, config.preferenceUrl(url, "_count"), searcher.toString)
    future.map { resultJson =>
      val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
      map.get("error").map { case message: String => Left(map) }.getOrElse(Right(map))
    }
  }

  def countAsIntAsync(config: ESConfig)(f: SearchRequestBuilder => Unit): Future[Int] = {
    countAsync(config)(f).map { result =>
      result match {
        case Left(x)  => throw new RuntimeException(x("error").toString)
        case Right(x) => x("count").asInstanceOf[Int]
      }
    }
  }

  def countByTemplateAsync(config: ESConfig)(lang: String, template: String, params: AnyRef): Future[Either[Map[String, Any], Map[String, Any]]] = {
    searchByTemplateAsync(config)(lang, template, params, Some("?search_type=count"))
  }

  def countByTemplateAsIntAsync(config: ESConfig)(lang: String, template: String, params: AnyRef): Future[Int] = {
    countByTemplateAsync(config)(lang: String, template: String, params: AnyRef).map { result =>
      result match {
        case Left(x)  => throw new RuntimeException(x("error").toString)
        case Right(x) => x("hits").asInstanceOf[Map[String, Any]]("total").asInstanceOf[Int]
      }
    }
  }

  def refreshAsync(config: ESConfig)(): Future[Either[Map[String, Any], Map[String, Any]]] = {
    val future = HttpUtils.postAsync(httpClient, s"${url}/${config.indexName}/_refresh", "")
    future.map { resultJson =>
      val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
      map.get("error").map { case message: String => Left(map) }.getOrElse(Right(map))
    }
  }

  // TODO scroll
  // TODO scrollChunk

  def bulkAsync[T](actions: Seq[BulkAction]): Future[Either[Map[String, Any], Map[String, Any]]] = {
    val future = HttpUtils.postAsync(httpClient, s"${url}/_bulk", actions.map(_.jsonString).mkString("", "\n", "\n"))
    future.map { resultJson =>
      val map = JsonUtils.deserialize[Map[String, Any]](resultJson)
      map.get("error").map { case message: String => Left(map) }.getOrElse(Right(map))
    }
  }

}
