package jp.co.bizreach.elasticsearch4s

case class ESConfig(indexName: String, typeName: Option[String] = None, preference: Option[String] = None, explain: Boolean = false, timeout: Option[Int] = None){

  /**
   * Returns the index URL.
   */
  def url(baseUrl: String) = typeName.map { typeName =>
    s"${baseUrl}/${indexName}/${typeName}"
  }.getOrElse {
    s"${baseUrl}/${indexName}"
  }

  /**
   * Returns the specified API URL with preference.
   */
  def preferenceUrl(baseUrl: String, path: String) = {
    val u = url(baseUrl) + "/" + path
    u + preference.map { x =>
      (if(u.indexOf('?') >= 0) "&" else "?") + "preference=" + x
    }.getOrElse("")
  }

  /**
   * Returns the specified API URL with received parameters.
   */
  def urlWithParameters(baseUrl: String, path: String) = {
    val u = url(baseUrl) + "/" + path

    val u2 = u + preference.map { x =>
      (if(u.indexOf('?') >= 0) "&" else "?") + "preference=" + x
    }.getOrElse("")

    val u3 = if(explain) {
      u2 + (if(u2.indexOf('?') >= 0) "&" else "?") + "explain=true"
    } else {
      u2
    }

    u3 + timeout.map { x =>
      (if(u3.indexOf('?') >= 0) "&" else "?") + "timeout=" + x + "ms"
    }.getOrElse("")
  }

  def urlWithParameters(baseUrl: String, parameters: Map[String, String]): String = {
    val path = parameters.toList.map(param => s"${param._1}=${param._2}").mkString("&")

    urlWithParameters(baseUrl, path)
  }
}

object ESConfig {

  /**
   * Creates ESConfig instance with index name and type name.
   */
  def apply(indexName: String, typeName: String): ESConfig = ESConfig(indexName, Some(typeName))

  /**
   * Creates ESConfig instance with index name, type name and preference.
   */
  def apply(indexName: String, typeName: String, preference: String): ESConfig = ESConfig(indexName, Some(typeName), Some(preference))

  /**
   * Creates ESConfig instance with index name, type name, preference and explain.
   */
  def apply(indexName: String, typeName: String, preference: String, explain: Boolean): ESConfig = ESConfig(indexName, Some(typeName), Some(preference), explain)

  /**
   * Creates ESConfig instance with index name, type name, preference, explain and timeout.
   */
  def apply(indexName: String, typeName: String, preference: String, explain: Boolean, timeout: Int): ESConfig = ESConfig(indexName, Some(typeName), Some(preference), explain, Some(timeout))

}
