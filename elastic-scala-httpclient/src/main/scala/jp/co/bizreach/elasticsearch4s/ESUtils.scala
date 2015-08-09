package jp.co.bizreach.elasticsearch4s

import scala.reflect.ClassTag

object ESUtils {

  def getDocumentMap(hit: Map[String, Any]): Map[String, Any] = {
    hit.get("_source").map(_.asInstanceOf[Map[String, Any]])
      .getOrElse(structuredMap(hit("fields").asInstanceOf[Map[String, Any]]))
  }

  def structuredMap(map: Map[String, Any]): Map[String, Any] = {
    def structuredMap0(group: List[(List[String], Any)]): Any = {
      group.groupBy { case (key, value) => key.head }.map { case (key, value) =>
        key -> (if(value.head._1.length == 1){
          value.head._2
        } else {
          structuredMap0(value.map { case (key, value) => key.tail -> value })
        })
      }
    }

    val list = map.map { case (key, value) => key.split("\\.").toList -> value }.toList
    structuredMap0(list).asInstanceOf[Map[String, Any]]
  }

  def createESSearchResult[T](x: Map[String, Any])(implicit c: ClassTag[T]): ESSearchResult[T] = {
    val total = x("hits").asInstanceOf[Map[String, Any]]("total").asInstanceOf[Int]
    val took  = x("took").asInstanceOf[Int]
    val hits  = x("hits").asInstanceOf[Map[String, Any]]("hits").asInstanceOf[Seq[Map[String, Any]]]

    ESSearchResult(
      total,
      took,
      hits.map { hit =>
        ESSearchResultItem(
          hit("_id").asInstanceOf[String],
          hit("_score").asInstanceOf[Double],
          JsonUtils.deserialize[T](JsonUtils.serialize(getDocumentMap(hit))),
          hit.get("highlight").asInstanceOf[Option[Map[String, List[String]]]].getOrElse(Map.empty),
          hit.get("_explanation").asInstanceOf[Option[Map[String, Any]]].getOrElse(Map.empty)
        )
      }.toList,
      x.get("facets").asInstanceOf[Option[Map[String, Map[String, Any]]]].getOrElse(Map.empty),
      x.get("aggregations").asInstanceOf[Option[Map[String, Any]]].getOrElse(Map.empty),
      x
    )
  }
  
}
