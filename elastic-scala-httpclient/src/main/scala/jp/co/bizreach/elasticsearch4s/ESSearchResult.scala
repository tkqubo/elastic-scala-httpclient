package jp.co.bizreach.elasticsearch4s

case class ESSearchResult[T](
  totalHits: Long,
  tookTime: Long,
  list: List[ESSearchResultItem[T]],
  facets: Map[String, Map[String, Any]],
  aggregations: Map[String, Any],
  source: Map[String, Any]
)

case class ESSearchResultItem[T](
  id: String,
  doc: T,
  highlightFields: Map[String, List[String]]
)
