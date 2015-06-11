package jp.co.bizreach.elasticsearch4s

case class ESConfig(indexName: String, typeName: Option[String] = None, preference: Option[String] = None, explain: Boolean = false){

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
    val ur = u + preference.map { x =>
      (if(u.indexOf('?') >= 0) "&" else "?") + "preference=" + x
    }.getOrElse("")
    if(explain) {
      ur + (if(ur.indexOf('?') >= 0) "&" else "?") + "explain=true"
    } else {
      ur
    }
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

}
