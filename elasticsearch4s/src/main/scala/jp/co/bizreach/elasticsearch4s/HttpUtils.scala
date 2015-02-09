package jp.co.bizreach.elasticsearch4s

import com.ning.http.client._
import scala.concurrent._

object HttpUtils {

  def createHttpClient(): AsyncHttpClient = {
    new AsyncHttpClient()
  }

  def createHttpClient(builder: AsyncHttpClientConfig): AsyncHttpClient = {
    new AsyncHttpClient(builder)
  }

  def closeHttpClient(httpClient: AsyncHttpClient): Unit = {
    httpClient.close()
  }


  def put(httpClient: AsyncHttpClient, url: String, json: String): String = {
    val f = httpClient.preparePut(url).setBody(json.getBytes("UTF-8")).execute()
    f.get().getResponseBody("UTF-8")
  }

  def putAsync(httpClient: AsyncHttpClient, url: String, json: String): Future[String] = {
    val promise = Promise[String]()
    httpClient.preparePut(url).setBody(json.getBytes("UTF-8")).execute(new AsyncResultHandler(promise))
    promise.future
  }

  def post(httpClient: AsyncHttpClient, url: String, json: String): String = {
    val f = httpClient.preparePost(url).setBody(json.getBytes("UTF-8")).execute()
    f.get().getResponseBody("UTF-8")
  }

  def postAsync(httpClient: AsyncHttpClient, url: String, json: String): Future[String] = {
    val promise = Promise[String]()
    httpClient.preparePost(url).setBody(json.getBytes("UTF-8")).execute(new AsyncResultHandler(promise))
    promise.future
  }

  def delete(httpClient: AsyncHttpClient, url: String, json: String = ""): String = {
    val builder = httpClient.prepareDelete(url)
    if(json.nonEmpty){
      builder.setBody(json.getBytes("UTF-8"))
    }
    val f = builder.execute()
    f.get().getResponseBody("UTF-8")
  }

  def deleteAsync(httpClient: AsyncHttpClient, url: String, json: String = ""): Future[String] = {
    val promise = Promise[String]()
    val builder = httpClient.prepareDelete(url)
    if(json.nonEmpty){
      builder.setBody(json.getBytes("UTF-8"))
    }
    builder.execute(new AsyncResultHandler(promise))
    promise.future
  }

  private class AsyncResultHandler(promise: Promise[String]) extends AsyncCompletionHandler[Unit] {
    override def onCompleted(response: Response): Unit = {
      println("*** AsyncResultHandler#onComplete!")
      promise.success(response.getResponseBody("UTF-8"))
    }
//    override def onThrowable(t: Throwable): Unit = {
//      promise.failure(t)
//    }
  }
}
