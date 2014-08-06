package jp.co.bizreach.elasticsearch4s

import org.apache.http.client.methods.{HttpDelete, HttpPost, HttpPut}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}
import org.apache.http.entity.StringEntity
import java.nio.charset.StandardCharsets
import org.apache.http.util.EntityUtils

object HttpUtils {

  def createHttpClient(): CloseableHttpClient = {
    HttpClientBuilder.create().build()
  }

  def closeHttpClient(httpClient: CloseableHttpClient): Unit = {
    httpClient.close()
  }

  def put(httpClient: CloseableHttpClient, url: String, json: String): String = {
    val request = new HttpPut(url)
    try {
      val entry = new StringEntity(json, StandardCharsets.UTF_8)
      entry.setContentType("application/json")
      request.setEntity(entry)
      val response = httpClient.execute(request)

      EntityUtils.toString(response.getEntity())
    } finally {
      request.releaseConnection()
    }
  }

  def post(httpClient: CloseableHttpClient, url: String, json: String): String = {
    val request = new HttpPost(url)
    try {
      val entry = new StringEntity(json, StandardCharsets.UTF_8)
      entry.setContentType("application/json")
      request.setEntity(entry)
      val response = httpClient.execute(request)

      EntityUtils.toString(response.getEntity())
    } finally {
      request.releaseConnection()
    }
  }

  def delete(httpClient: CloseableHttpClient, url: String): String = {
    val request = new HttpDelete(url)
    try {
      val response = httpClient.execute(request)
      EntityUtils.toString(response.getEntity())
    } finally {
      request.releaseConnection()
    }
  }
}
