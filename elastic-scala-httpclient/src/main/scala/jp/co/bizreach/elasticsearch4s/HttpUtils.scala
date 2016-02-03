package jp.co.bizreach.elasticsearch4s

import com.ning.http.client._
import scala.concurrent._
import scala.collection.JavaConverters._

class HttpResponseException(status: Int, headers: Seq[(String, String)], body: String)
  extends RuntimeException(
    s"HTTP response is bad. Response status: ${status}\n" +
    "---- headers ----\n" +
    headers.map { case (key, value) => s"${key}: ${value}" }.mkString("\n") + "\n" +
    "---- body ----\n" +
    body
  ){

  def this(response: Response) = {
    this(
      status  = response.getStatusCode,
      headers = response.getHeaders.asInstanceOf[java.util.Map[String, java.util.List[String]]]
        .asScala.map { case (key, values) => (key, values.asScala.mkString(", ")) }.toSeq,
      body    = response.getResponseBody
    )
  }
}

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
    val response = f.get()
    if (response.getStatusCode == 200){
      response.getResponseBody("UTF-8")
    } else {
      throw new HttpResponseException(response)
    }
  }

  def putAsync(httpClient: AsyncHttpClient, url: String, json: String): Future[String] = {
    val promise = Promise[String]()
    httpClient.preparePut(url).setBody(json.getBytes("UTF-8")).execute(new AsyncResultHandler(promise))
    promise.future
  }

  def post(httpClient: AsyncHttpClient, url: String, json: String): String = {
    val f = httpClient.preparePost(url).setBody(json.getBytes("UTF-8")).execute()
    val response = f.get()
    if (response.getStatusCode == 200) {
      response.getResponseBody("UTF-8")
    } else {
      throw new HttpResponseException(response)
    }
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
    val response = f.get()
    if (response.getStatusCode == 200) {
      response.getResponseBody("UTF-8")
    } else {
      throw new HttpResponseException(response)
    }
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
      if (response.getStatusCode == 200) {
        promise.success(response.getResponseBody("UTF-8"))
      } else {
        promise.failure(new HttpResponseException(response))
      }
    }
    override def onThrowable(t: Throwable): Unit = {
      promise.failure(t)
    }
  }
}
