package jp.co.bizreach.elasticsearch4s

case class ESConfig(indexName: String, typeName: Option[String] = None){
  def url(baseUrl: String) = typeName.map { typeName =>
    s"${baseUrl}/${indexName}/${typeName}"
  }.getOrElse {
    s"${baseUrl}/${indexName}"
  }
}

object ESConfig {
  /**
   * Creates ESConfig instance with index name and type name.
   * This method ha been remained to keep backward compatibility.
   */
  def apply(indexName: String, typeName: String): ESConfig = ESConfig(indexName, Some(typeName))
}
