package jp.co.bizreach.elasticsearch4s

import com.ning.http.client._
import scala.concurrent._
import java.util.concurrent.Future

object HttpUtils {

  def createHttpClient(): AsyncHttpClient = {
    new AsyncHttpClient()
  }

  def closeHttpClient(httpClient: AsyncHttpClient): Unit = {
    httpClient.close()
  }


  def put(httpClient: AsyncHttpClient, url: String, json: String): String = {
    val f = httpClient.preparePut(url).setBody(json.getBytes("UTF-8")).execute()
    f.get().getResponseBody("UTF-8")
  }

  def post(httpClient: AsyncHttpClient, url: String, json: String): String = {
    val f = httpClient.preparePost(url).setBody(json.getBytes("UTF-8")).execute()
    f.get().getResponseBody("UTF-8")
  }

  def delete(httpClient: AsyncHttpClient, url: String, json: String = ""): String = {
    val builder = httpClient.prepareDelete(url)
    if(json.nonEmpty){
      builder.setBody(json.getBytes("UTF-8"))
    }
    val f = builder.execute()
    val response = f.get()

    response.getResponseBody("UTF-8")
  }
}
