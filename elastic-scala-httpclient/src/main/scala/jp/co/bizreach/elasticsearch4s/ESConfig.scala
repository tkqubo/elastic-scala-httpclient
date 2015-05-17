package jp.co.bizreach.elasticsearch4s

case class ESConfig(indexName: String, typeName: Option[String] = None, preference: Option[String] = None){

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

}

object ESConfig {

  def string2config(indexName: String) = ESConfig(indexName)
  def tuple2config(tuple: (String, String)) = ESConfig(tuple._1, tuple._2)

  implicit class ESStringConfig(indexName: String){
    def / (typeName: String): ESConfig = ESConfig(indexName, typeName)
  }

  /**
   * Creates ESConfig instance with index name and type name.
   */
  def apply(indexName: String, typeName: String): ESConfig = ESConfig(indexName, Some(typeName))

  /**
   * Creates ESConfig instance with index name, type name and preference.
   */
  def apply(indexName: String, typeName: String, preference: String): ESConfig = ESConfig(indexName, Some(typeName), Some(preference))

}
