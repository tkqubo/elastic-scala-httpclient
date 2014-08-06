package jp.co.bizreach.elasticsearch4s

import org.apache.http.client.methods.{HttpDelete, HttpPost, HttpPut}
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.entity.StringEntity
import java.nio.charset.StandardCharsets
import org.apache.http.util.EntityUtils

object HttpUtils {

  def put(url: String, json: String): String = {
    val request = new HttpPut(url)
    try {
      val builder = HttpClientBuilder.create().build()
      val entry = new StringEntity(json, StandardCharsets.UTF_8)
      entry.setContentType("application/json")
      request.setEntity(entry)
      val response = builder.execute(request)

      EntityUtils.toString(response.getEntity())
    } finally {
      request.releaseConnection()
    }
  }

  def post(url: String, json: String): String = {
    val request = new HttpPost(url)
    try {
      val builder = HttpClientBuilder.create().build()
      val entry = new StringEntity(json, StandardCharsets.UTF_8)
      entry.setContentType("application/json")
      request.setEntity(entry)
      val response = builder.execute(request)

      EntityUtils.toString(response.getEntity())
    } finally {
      request.releaseConnection()
    }
  }

  def delete(url: String): String = {
    val request = new HttpDelete(url)
    try {
      val builder = HttpClientBuilder.create().build()
      val response = builder.execute(request)

      EntityUtils.toString(response.getEntity())
    } finally {
      request.releaseConnection()
    }
  }
}
